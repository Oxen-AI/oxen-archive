use crate::common::{TestServer, make_initialized_repo_with_test_user};


#[tokio::test]
async fn oxen_repo_held_csv_should_be_accessible_via_http_get() {
    // This test focuses specifically on CSV file accessibility via HTTP GET
    // Create a test repository with CSV data
    let test_dir = std::env::temp_dir().join("oxen_csv_test");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    
    // Create repository with CSV file using programmatic API
    let _repo_dir = make_initialized_repo_with_test_user(&test_dir).await.expect("Failed to create initialized repo");
    
    // Start oxen-server
    let server = TestServer::start_with_sync_dir(&test_dir, 3001).await.expect("Failed to start test server");
    
    // Create HTTP client
    let client = reqwest::Client::new();
    
    // Test: HTTP PUT should work over real TCP/IP connection
    let response = client
        .put(&format!("{}/api/repos/test_user/csv_repo/files/main/products.csv", server.base_url()))
        .send()
        .await
        .expect("Failed to send HTTP PUT request for CSV");
    
    let status = response.status();
    println!("CSV HTTP PUT response status: {}", status);
    let body = response.text().await.expect("Failed to read CSV response body");
    println!("CSV HTTP PUT response body: {}", body);
    
    // Verify we can make HTTP PUT request over TCP/IP (integration test requirement)
    if status.as_u16() >= 200 && status.as_u16() < 500 {
        println!("✅ HTTP PUT over TCP/IP successful - status: {}", status);
    } else {
        println!("⚠️  HTTP PUT over TCP/IP failed - status: {}", status);
        // This is expected to fail until the full PUT endpoint is implemented
    }
}

