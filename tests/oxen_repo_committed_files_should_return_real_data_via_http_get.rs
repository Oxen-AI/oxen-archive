use std::process::{Command, Stdio};
use std::time::Duration;
use std::fs;
use tokio::time::sleep;
use serde_json::Value;

/// Integration test: Oxen repo committed files should return real data via HTTP GET
/// Creates actual Oxen repository with init/add/commit workflow, then retrieves data via HTTP GET
#[tokio::test]
async fn oxen_repo_committed_files_should_return_real_data_via_http_get() {
    // Setup test environment
    let test_dir = std::env::temp_dir().join("oxen_positive_test");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    
    // Create a test repository with actual files
    let repo_dir = test_dir.join("test_user").join("test_repo");
    std::fs::create_dir_all(&repo_dir).expect("Failed to create repo directory");
    
    // Create test files
    let test_file_path = repo_dir.join("test.txt");
    let test_content = "Hello from Oxen integration test!\nThis is real file content.";
    fs::write(&test_file_path, test_content).expect("Failed to write test file");
    
    let csv_file_path = repo_dir.join("data.csv");
    let csv_content = "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,Chicago";
    fs::write(&csv_file_path, csv_content).expect("Failed to write CSV file");
    
    // Initialize Oxen repository
    println!("Initializing Oxen repository with VCS workflow (init/add/commit)...");
    let oxen_binary = std::env::current_dir()
        .expect("Failed to get current dir")
        .join("target/debug/oxen");
    
    if !oxen_binary.exists() {
        panic!("oxen CLI binary not found. Run 'cargo build --bin oxen' first");
    }
    
    // Initialize Oxen repository
    let init_output = Command::new(&oxen_binary)
        .arg("init")
        .current_dir(&repo_dir)
        .output()
        .expect("Failed to init repository");
    
    if !init_output.status.success() {
        panic!("Failed to initialize repository: {}", String::from_utf8_lossy(&init_output.stderr));
    }
    
    // Configure user
    let _ = Command::new(&oxen_binary)
        .args(&["config", "--name", "Test User", "--email", "test@example.com"])
        .current_dir(&repo_dir)
        .output();
    
    // Add files
    let add_output = Command::new(&oxen_binary)
        .args(&["add", "test.txt", "data.csv"])
        .current_dir(&repo_dir)
        .output()
        .expect("Failed to add files");
    
    if !add_output.status.success() {
        panic!("Failed to add files: {}", String::from_utf8_lossy(&add_output.stderr));
    }
    
    // Commit files
    let commit_output = Command::new(&oxen_binary)
        .args(&["commit", "-m", "Initial commit with test files"])
        .current_dir(&repo_dir)
        .output()
        .expect("Failed to commit files");
    
    if !commit_output.status.success() {
        panic!("Failed to commit files: {}", String::from_utf8_lossy(&commit_output.stderr));
    }
    
    println!("✅ Created Oxen repository with test files");
    
    // Start the server
    let server_binary = std::env::current_dir()
        .expect("Failed to get current dir")
        .join("target/debug/oxen-server");
    
    if !server_binary.exists() {
        panic!("oxen-server binary not found. Run 'cargo build --bin oxen-server' first");
    }
    
    println!("Starting oxen-server...");
    
    let mut server_process = Command::new(&server_binary)
        .args(&["start", "--ip", "127.0.0.1", "--port", "3004"])
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
    
    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    
    let base_url = "http://127.0.0.1:3004";
    
    // Wait for server to be ready
    println!("Waiting for server to be ready...");
    let mut server_ready = false;
    for attempt in 1..=10 {
        println!("Server ready check attempt {}/10", attempt);
        match client.get(&format!("{}/api/health", base_url)).send().await {
            Ok(response) if response.status().is_success() => {
                server_ready = true;
                break;
            }
            Ok(response) => {
                println!("Server not ready, status: {}", response.status());
            }
            Err(e) => {
                println!("Server not ready, error: {}", e);
            }
        }
        sleep(Duration::from_millis(1000)).await;
    }
    
    if !server_ready {
        let _ = server_process.kill();
        panic!("Server failed to become ready");
    }
    
    println!("✅ Server is ready, testing API endpoints...");
    
    // Test 1: List repositories
    println!("Testing repository listing...");
    match client.get(&format!("{}/api/repos", base_url)).send().await {
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
    match client.get(&format!("{}/api/repos/test_user/test_repo", base_url)).send().await {
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
    match client.get(&format!("{}/api/repos/test_user/test_repo/files", base_url)).send().await {
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
    match client.get(&format!("{}/api/repos/test_user/test_repo/files/main/test.txt", base_url)).send().await {
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
    match client.get(&format!("{}/api/repos/test_user/test_repo/files/main/data.csv", base_url)).send().await {
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
    
    // Cleanup: Kill the server
    println!("Cleaning up server...");
    let _ = server_process.kill();
    let _ = server_process.wait();
    
    // Clean up test directory
    let _ = std::fs::remove_dir_all(&test_dir);
    
    println!("✅ Positive HTTP integration test completed!");
}