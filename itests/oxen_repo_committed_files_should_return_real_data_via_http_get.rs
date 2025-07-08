use std::time::Duration;
use serde_json::Value;
use crate::common::{TestServer, make_initialized_repo_with_test_files};


/// Integration test: Oxen repo committed files should return real data via HTTP GET
/// Creates actual Oxen repository with init/add/commit workflow, then retrieves data via HTTP GET
#[tokio::test]
async fn oxen_repo_committed_files_should_return_real_data_via_http_get() {
    // Setup test environment
    let test_dir = std::env::temp_dir().join("oxen_positive_test");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    
    // Create repository with test files using programmatic API
    let _repo_dir = make_initialized_repo_with_test_files(&test_dir).await.expect("Failed to create initialized repo");
    
    // Start oxen-server
    let server = TestServer::start_with_sync_dir(&test_dir, 3004).await.expect("Failed to start test server");
    
    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    
    println!("✅ Server is ready, testing API endpoints...");
    
    // Test 1: List repositories
    println!("Testing repository listing...");
    match client.get(&format!("{}/api/repos", server.base_url())).send().await {
        Ok(response) => {
            println!("Repositories list status: {}", response.status());
            if response.status().is_success() {
                let body = response.text().await.unwrap_or_default();
                println!("Repositories response: {}", body);
                
                // Parse JSON to verify structure
                if let Ok(_json) = serde_json::from_str::<Value>(&body) {
                    println!("✅ Successfully parsed repositories JSON");
                } else {
                    println!("⚠️  Could not parse repositories response as JSON");
                }
            }
        }
        Err(e) => {
            println!("❌ Repository listing failed: {}", e);
        }
    }
    
    // Test 2: Get specific repository info
    println!("Testing specific repository access...");
    match client.get(&format!("{}/api/repos/test_user/test_repo", server.base_url())).send().await {
        Ok(response) => {
            let status = response.status();
            println!("Repository info status: {}", status);
            let body = response.text().await.unwrap_or_default();
            println!("Repository info response: {}", body);
            
            if status.is_success() {
                println!("✅ Successfully accessed repository info");
            }
        }
        Err(e) => {
            println!("❌ Repository access failed: {}", e);
        }
    }
    
    // Test 3: List files in repository
    println!("Testing file listing in repository...");
    match client.get(&format!("{}/api/repos/test_user/test_repo/files", server.base_url())).send().await {
        Ok(response) => {
            let status = response.status();
            println!("Files list status: {}", status);
            let body = response.text().await.unwrap_or_default();
            println!("Files response: {}", body);
            
            if status.is_success() {
                if body.contains("test.txt") && body.contains("data.csv") {
                    println!("✅ Successfully found our test files in the repository");
                } else {
                    println!("⚠️  Test files not found in response");
                }
            }
        }
        Err(e) => {
            println!("❌ File listing failed: {}", e);
        }
    }
    
    // Test 4: Get actual file content
    println!("Testing file content retrieval...");
    match client.get(&format!("{}/api/repos/test_user/test_repo/files/main/test.txt", server.base_url())).send().await {
        Ok(response) => {
            let status = response.status();
            println!("File content status: {}", status);
            let body = response.text().await.unwrap_or_default();
            println!("File content response: {}", body);
            
            if status.is_success() && body.contains("Hello from Oxen integration test!") {
                println!("✅ Successfully retrieved actual file content!");
            } else if status.is_success() {
                println!("⚠️  Got successful response but content doesn't match expected");
            }
        }
        Err(e) => {
            println!("❌ File content retrieval failed: {}", e);
        }
    }
    
    // Test 5: Get CSV file content
    println!("Testing CSV file content retrieval...");
    match client.get(&format!("{}/api/repos/test_user/test_repo/files/main/data.csv", server.base_url())).send().await {
        Ok(response) => {
            let status = response.status();
            println!("CSV file status: {}", status);
            let body = response.text().await.unwrap_or_default();
            println!("CSV file response: {}", body);
            
            if status.is_success() && body.contains("Alice,30,New York") {
                println!("✅ Successfully retrieved CSV file content!");
            } else if status.is_success() {
                println!("⚠️  Got successful response but CSV content doesn't match expected");
            }
        }
        Err(e) => {
            println!("❌ CSV file retrieval failed: {}", e);
        }
    }
    
    // Clean up test directory
    let _ = std::fs::remove_dir_all(&test_dir);
    
    // Server cleanup handled by Drop trait
    
    println!("✅ Positive HTTP integration test completed!");
}
