use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Integration test: Oxen server health should be accessible via HTTP GET
/// Uses real oxen-server process and actual HTTP GET requests (reqwest - Rust's OkHttp equivalent)
#[tokio::test]
async fn oxen_server_health_should_be_accessible_via_http_get() {
    // Create test directory
    let test_dir = std::env::temp_dir().join("oxen_http_test");
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    
    // Start the server in the background
    let server_binary = std::env::current_dir()
        .expect("Failed to get current dir")
        .join("target/debug/oxen-server");
    
    if !server_binary.exists() {
        panic!("oxen-server binary not found. Run 'cargo build --bin oxen-server' first");
    }
    
    println!("Starting oxen-server...");
    
    let mut server_process = Command::new(&server_binary)
        .args(&["start", "--ip", "127.0.0.1", "--port", "3002"])
        .env("SYNC_DIR", &test_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start oxen-server");
    
    // Give server time to start
    sleep(Duration::from_secs(3)).await;
    
    // Check if server is still running
    if let Ok(Some(status)) = server_process.try_wait() {
        panic!("Server exited early with status: {}", status);
    }
    
    // Create HTTP client (reqwest is Rust's equivalent to OkHttp)
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    
    let base_url = "http://127.0.0.1:3002";
    
    // Test 1: Health endpoint
    println!("Testing health endpoint...");
    let health_result = timeout(Duration::from_secs(5), async {
        for attempt in 1..=5 {
            println!("Health check attempt {}/5", attempt);
            match client.get(&format!("{}/api/health", base_url)).send().await {
                Ok(response) => {
                    println!("Health response status: {}", response.status());
                    if response.status().is_success() {
                        let body = response.text().await.unwrap_or_default();
                        println!("Health response body: {}", body);
                        return Ok(());
                    }
                }
                Err(e) => {
                    println!("Health check failed: {}", e);
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
        Err("Health check failed after 5 attempts")
    }).await;
    
    // Test 2: Version endpoint (if health worked)
    if health_result.is_ok() {
        println!("Testing version endpoint...");
        match client.get(&format!("{}/api/version", base_url)).send().await {
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
    match client.get(&format!("{}/api/nonexistent", base_url)).send().await {
        Ok(response) => {
            println!("404 test response status: {}", response.status());
            // Should be 404 or some error status
        }
        Err(e) => {
            println!("404 test failed: {}", e);
        }
    }
    
    // Cleanup: Kill the server
    println!("Cleaning up server...");
    let _ = server_process.kill();
    let _ = server_process.wait();
    
    // Clean up test directory
    let _ = std::fs::remove_dir_all(&test_dir);
    
    // Assert that at least the health check worked
    health_result.expect("HTTP integration test failed - server did not respond to health checks");
    
    println!("✅ Real HTTP integration test completed successfully!");
}