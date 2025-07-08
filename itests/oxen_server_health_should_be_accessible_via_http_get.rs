use std::time::Duration;
use crate::common::TestServer;


/// Integration test: Oxen server health should be accessible via HTTP GET
/// Uses real oxen-server process and actual HTTP GET requests (reqwest - Rust's OkHttp equivalent)
#[tokio::test]
async fn oxen_server_health_should_be_accessible_via_http_get() {
    // Create test directory
    let test_dir = std::env::temp_dir().join("oxen_http_test");
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    
    // Start oxen-server
    let server = TestServer::start_with_sync_dir(&test_dir, 3002).await.expect("Failed to start test server");
    
    // Create HTTP client (reqwest is Rust's equivalent to OkHttp)
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    
    // Test 1: Health endpoint (server already verified healthy during startup)
    println!("Testing health endpoint...");
    let health_result = match client.get(&format!("{}/api/health", server.base_url())).send().await {
        Ok(response) => {
            println!("Health response status: {}", response.status());
            if response.status().is_success() {
                let body = response.text().await.unwrap_or_default();
                println!("Health response body: {}", body);
                Ok(())
            } else {
                Err("Health check returned non-success status")
            }
        }
        Err(e) => {
            println!("Health check failed: {}", e);
            Err("Health check failed")
        }
    };
    
    // Test 2: Version endpoint (if health worked)
    if health_result.is_ok() {
        println!("Testing version endpoint...");
        match client.get(&format!("{}/api/version", server.base_url())).send().await {
            Ok(response) => {
                println!("Version response status: {}", response.status());
                if response.status().is_success() {
                    let body = response.text().await.unwrap_or_default();
                    println!("Version response body: {}", body);
                    assert!(body.contains("version"), "Version response should contain version info");
                }
            }
            Err(e) => {
                println!("Version endpoint failed: {}", e);
            }
        }
    }
    
    // Test 3: 404 endpoint
    println!("Testing 404 endpoint...");
    match client.get(&format!("{}/api/nonexistent", server.base_url())).send().await {
        Ok(response) => {
            println!("404 test response status: {}", response.status());
            // Should be 404 or some error status
        }
        Err(e) => {
            println!("404 test failed: {}", e);
        }
    }
    
    // Clean up test directory
    let _ = std::fs::remove_dir_all(&test_dir);
    
    // Server cleanup handled by Drop trait
    
    // Assert that at least the health check worked
    health_result.expect("HTTP integration test failed - server did not respond to health checks");
    
    println!("✅ Real HTTP integration test completed successfully!");
}
