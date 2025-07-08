#[allow(dead_code)]

use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

pub struct TestServer {
    child: Child,
    base_url: String,
}

impl TestServer {
    /// Start a real oxen-server process with custom sync directory
    pub async fn start_with_sync_dir(sync_dir: &std::path::Path, port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        // Create the sync directory
        std::fs::create_dir_all(&sync_dir)?;
        
        // Find the oxen-server binary
        let server_path = std::env::current_dir()?
            .join("target")
            .join("debug")
            .join("oxen-server");
            
        if !server_path.exists() {
            return Err("oxen-server binary not found. Run 'cargo build' first".into());
        }
        
        // Start the server process
        let mut child = Command::new(server_path)
            .arg("start")
            .arg("--ip")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(&port.to_string())
            .env("SYNC_DIR", &sync_dir)
            .stdout(Stdio::null()) // Suppress output to avoid hanging
            .stderr(Stdio::null())
            .spawn()?;
            
        // Check if process is still running
        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(format!("Server process exited early with status: {}", status).into());
            }
            Ok(None) => {
                // Process is still running, good
            }
            Err(e) => {
                return Err(format!("Error checking server process: {}", e).into());
            }
        }
        
        // Try to connect to health endpoint to verify server is ready
        let client = reqwest::Client::new();
        let base_url = format!("http://127.0.0.1:{}", port);
        let start_time = std::time::Instant::now();
        
        for i in 0..1000 {
            if let Ok(response) = client.get(&format!("{}/api/health", base_url)).send().await {
                if response.status().is_success() {
                    let elapsed = start_time.elapsed();
                    println!("Server started in {:?} (attempt {})", elapsed, i + 1);
                    return Ok(TestServer {
                        child,
                        base_url,
                    });
                }
            }
            sleep(Duration::from_millis(5)).await;
        }
        
        // If we get here, server didn't start properly
        let _ = child.kill();
        Err("Server failed to start or health check failed".into())
    }
    
    /// Get the base URL for this test server
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Clean up the server process
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Create an initialized repository with test user configuration
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_user(base_dir: &std::path::Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let repo_dir = base_dir.join("test_user").join("csv_repo");
    std::fs::create_dir_all(&repo_dir)?;
    
    // Initialize repository programmatically
    let repo = liboxen::repositories::init(&repo_dir)?;
    
    // Create CSV content
    let csv_content = "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture\nBook,19.99,Education";
    std::fs::write(repo_dir.join("products.csv"), csv_content)?;
    
    // Add file and commit with user info
    liboxen::repositories::add(&repo, &repo_dir.join("products.csv"))?;
    
    let user = liboxen::model::User {
        name: "Test".to_string(),
        email: "test@test.com".to_string(),
    };
    
    liboxen::repositories::commits::commit_writer::commit_with_user(&repo, "Add CSV data", &user)?;
    
    Ok(repo_dir)
}

/// Create an initialized repository with test user and files
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_files(base_dir: &std::path::Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let repo_dir = base_dir.join("test_user").join("test_repo");
    std::fs::create_dir_all(&repo_dir)?;
    
    // Initialize repository programmatically
    let repo = liboxen::repositories::init(&repo_dir)?;
    
    // Create test files
    let test_content = "Hello from Oxen integration test!\nThis is real file content.";
    std::fs::write(repo_dir.join("test.txt"), test_content)?;
    
    let csv_content = "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,Chicago";
    std::fs::write(repo_dir.join("data.csv"), csv_content)?;
    
    // Add files and commit with user info
    liboxen::repositories::add(&repo, &repo_dir.join("test.txt"))?;
    liboxen::repositories::add(&repo, &repo_dir.join("data.csv"))?;
    
    let user = liboxen::model::User {
        name: "Test User".to_string(),
        email: "test@example.com".to_string(),
    };
    
    liboxen::repositories::commits::commit_writer::commit_with_user(&repo, "Initial commit with test files", &user)?;
    
    Ok(repo_dir)
}