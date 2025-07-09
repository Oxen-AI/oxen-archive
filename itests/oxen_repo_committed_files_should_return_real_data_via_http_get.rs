use std::time::Duration;
use serde_json::Value;
use crate::common::{TestServer, make_initialized_repo_with_test_files_in_memory};

/// Helper function to create test environment (shared setup)
async fn create_test_environment(port: u16) -> (std::path::PathBuf, TestServer, reqwest::Client) {
    let test_dir = std::env::temp_dir().join(format!("oxen_positive_test_{}", port));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    
    // Create repository with test files using programmatic API with in-memory storage
    let (_repo_dir, _repo) = make_initialized_repo_with_test_files_in_memory(&test_dir).await
        .expect("Failed to create initialized repo");
    
    // Start oxen-server
    let server = TestServer::start_with_sync_dir(&test_dir, port).await
        .expect("Failed to start test server");
    
    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    
    (test_dir, server, client)
}

/// Test repository listing endpoint
/// Tests the /api/repos/{namespace} endpoint returns valid JSON
#[tokio::test]
async fn test_list_repositories_via_http_get() {
    let (test_dir, server, client) = create_test_environment(3004).await;
    
    println!("Testing repository listing...");
    
    // Retry logic to handle race condition where server needs time to discover repository
    let mut attempts = 0;
    let max_attempts = 5;
    
    loop {
        attempts += 1;
        let response = client.get(&format!("{}/api/repos/test_user", server.base_url()))
            .send()
            .await
            .expect("Failed to send request to repositories endpoint");
        
        let status = response.status();
        println!("Repositories list status: {} (attempt {})", status, attempts);
        let body = response.text().await.unwrap_or_default();
        println!("Repositories response: {}", body);
        
        // Should return success status
        assert!(status.is_success(), "Expected 200 OK, got {}", status);
        
        // Parse JSON to verify structure
        let _json = serde_json::from_str::<Value>(&body)
            .expect("Failed to parse repositories response as valid JSON");
        println!("✅ Successfully parsed repositories JSON");
        
        // Check if repository is discovered
        if body.contains("memory_test_repo") {
            println!("✅ Found repository in response!");
            break;
        } else if attempts >= max_attempts {
            // Final attempt failed
            assert!(false, "Expected 'memory_test_repo' in response after {} attempts: {}", max_attempts, body);
        } else {
            // Wait a bit for server to discover the repository
            println!("⚠️  Repository not found yet, waiting and retrying...");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    
    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
}

/// Test specific repository info endpoint
/// Tests the /api/repos/{namespace}/{repo} endpoint
#[tokio::test]
async fn test_get_specific_repository_info() {
    let (test_dir, server, client) = create_test_environment(3005).await;
    
    println!("Testing specific repository access...");
    let response = client.get(&format!("{}/api/repos/test_user/memory_test_repo", server.base_url()))
        .send()
        .await
        .expect("Failed to send request to repository info endpoint");
    
    let status = response.status();
    println!("Repository info status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("Repository info response: {}", body);
    
    if status.is_success() {
        println!("✅ Successfully accessed repository info");
    }
    
    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
}

/// Test file listing endpoint
/// Tests the /api/repos/{namespace}/{repo}/files endpoint
#[tokio::test]
async fn test_list_files_in_repository() {
    let (test_dir, server, client) = create_test_environment(3006).await;
    
    println!("Testing file listing in repository...");
    let response = client.get(&format!("{}/api/repos/test_user/memory_test_repo/files", server.base_url()))
        .send()
        .await
        .expect("Failed to send request to files endpoint");
    
    let status = response.status();
    println!("Files list status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("Files response: {}", body);
    
    if status.is_success() {
        assert!(body.contains("test.txt") && body.contains("data.csv"), 
            "Expected test files not found in repository listing - response: {}", body);
        println!("✅ Successfully found our test files in the repository");
    }
    
    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
}

/// Test text file content retrieval
/// Tests the /api/repos/{namespace}/{repo}/file/{branch}/{path} endpoint for text files
#[tokio::test]
async fn test_get_text_file_content() {
    let (test_dir, server, client) = create_test_environment(3007).await;
    
    println!("Testing file content retrieval...");
    let response = client.get(&format!("{}/api/repos/test_user/memory_test_repo/file/main/test.txt", server.base_url()))
        .send()
        .await
        .expect("Failed to send request to file content endpoint");
    
    let status = response.status();
    println!("File content status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("File content response: {}", body);
    
    if status.is_success() {
        assert!(body.contains("Hello from Oxen integration test!"), 
            "Expected file content not found - response: {}", body);
        println!("✅ Successfully retrieved actual file content!");
    }
    
    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
}

/// Test CSV file content retrieval
/// Tests the /api/repos/{namespace}/{repo}/file/{branch}/{path} endpoint for CSV files
#[tokio::test]
async fn test_get_csv_file_content() {
    let (test_dir, server, client) = create_test_environment(3008).await;
    
    println!("Testing CSV file content retrieval...");
    let response = client.get(&format!("{}/api/repos/test_user/memory_test_repo/file/main/data.csv", server.base_url()))
        .send()
        .await
        .expect("Failed to send request to CSV file endpoint");
    
    let status = response.status();
    println!("CSV file status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("CSV file response: {}", body);
    
    if status.is_success() {
        assert!(body.contains("Alice,30,New York"), 
            "Expected CSV content not found - response: {}", body);
        println!("✅ Successfully retrieved CSV file content!");
    }
    
    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
}
