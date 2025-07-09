#[allow(dead_code)]

use std::process::{Child, Command, Stdio};
use std::time::Duration;
use std::sync::Arc;
use tokio::time::sleep;
use liboxen::storage::VersionStore;

pub mod in_memory_storage;
pub use in_memory_storage::InMemoryVersionStore;

pub mod test_repository_builder;
pub use test_repository_builder::TestRepositoryBuilder;

pub mod port_leaser;
pub use port_leaser::{TestPortAllocator, PortLease};

pub struct TestServer {
    child: Child,
    base_url: String,
    _port_lease: Option<PortLease>, // Keep lease alive for server lifetime
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
                        _port_lease: None, // Manual port, no lease needed
                    });
                }
            }
            sleep(Duration::from_millis(5)).await;
        }
        
        // If we get here, server didn't start properly
        let _ = child.kill();
        Err("Server failed to start or health check failed".into())
    }
    
    /// Start a real oxen-server process with automatic port allocation
    /// This method is thread-safe and prevents port conflicts in parallel tests
    pub async fn start_with_auto_port(sync_dir: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        // Lease a port from the global allocator
        let port_lease = TestPortAllocator::instance().lease_port()
            .map_err(|e| format!("Failed to lease port: {}", e))?;
        
        let port = port_lease.port();
        
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
                    println!("Server started in {:?} (attempt {}) on auto-port {}", elapsed, i + 1, port);
                    return Ok(TestServer {
                        child,
                        base_url,
                        _port_lease: Some(port_lease), // Keep lease alive for server lifetime
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

/// Create an initialized repository with test user and CSV file using in-memory storage
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_user_in_memory(base_dir: &std::path::Path) -> Result<(std::path::PathBuf, liboxen::model::LocalRepository), Box<dyn std::error::Error>> {
    let repo_dir = base_dir.join("test_user").join("csv_repo");
    std::fs::create_dir_all(&repo_dir)?;
    
    // Initialize repository with in-memory storage using composition
    let repo = init_repo_with_in_memory_storage(&repo_dir)?;
    
    // Create CSV content
    let csv_content = "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture\nBook,19.99,Education";
    let csv_path = repo_dir.join("products.csv");
    
    // Write CSV file temporarily - the add operation will read this and store content in memory
    std::fs::write(&csv_path, csv_content)?;
    
    // Add file and commit with user info
    // The add operation will read the file and store its content in the in-memory version store
    liboxen::repositories::add(&repo, &csv_path)?;
    
    let user = liboxen::model::User {
        name: "Test".to_string(),
        email: "test@test.com".to_string(),
    };
    
    liboxen::repositories::commits::commit_writer::commit_with_user(&repo, "Add CSV data", &user)?;
    
    Ok((repo_dir, repo))
}

/// Create an initialized repository with in-memory storage for testing
#[allow(dead_code)]
pub async fn make_initialized_repo_with_in_memory_storage(base_dir: &std::path::Path) -> Result<(std::path::PathBuf, liboxen::model::LocalRepository), Box<dyn std::error::Error>> {
    let repo_dir = base_dir.join("test_user").join("memory_repo");
    std::fs::create_dir_all(&repo_dir)?;
    
    // Initialize repository with in-memory storage using composition
    let repo = init_repo_with_in_memory_storage(&repo_dir)?;
    
    Ok((repo_dir, repo))
}

/// Helper function to create test environment with auto-port allocation
/// This replaces the manual create_test_environment(port) pattern
#[allow(dead_code)]
pub async fn create_test_environment_with_auto_port() -> Result<(std::path::PathBuf, TestServer, reqwest::Client), Box<dyn std::error::Error>> {
    let unique_id = std::thread::current().id();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_dir = std::env::temp_dir().join(format!("oxen_auto_port_test_{:?}_{}", unique_id, timestamp));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    
    // Create repository with test files using programmatic API with in-memory storage
    let (_repo_dir, _repo) = make_initialized_repo_with_test_files_in_memory(&test_dir).await
        .expect("Failed to create initialized repo");
    
    // Start oxen-server with auto-port allocation
    let server = TestServer::start_with_auto_port(&test_dir).await
        .expect("Failed to start test server");
    
    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    
    Ok((test_dir, server, client))
}

/// Create an initialized repository with test files using in-memory storage
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_files_in_memory(base_dir: &std::path::Path) -> Result<(std::path::PathBuf, liboxen::model::LocalRepository), Box<dyn std::error::Error>> {
    let repo_dir = base_dir.join("test_user").join("memory_test_repo");
    std::fs::create_dir_all(&repo_dir)?;
    
    // Initialize repository with in-memory storage using composition
    let repo = init_repo_with_in_memory_storage(&repo_dir)?;
    
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
    
    Ok((repo_dir, repo))
}

/// Helper function to initialize a repository with in-memory storage using composition
/// This creates the repository structure and injects the in-memory storage
fn init_repo_with_in_memory_storage(repo_dir: &std::path::Path) -> Result<liboxen::model::LocalRepository, Box<dyn std::error::Error>> {
    // Create the basic repository structure first
    liboxen::repositories::init(repo_dir)?;
    
    // Create in-memory version store
    let in_memory_store = Arc::new(InMemoryVersionStore::new());
    in_memory_store.init()?;
    
    // Use composition to create repository with in-memory storage
    let repo = liboxen::model::LocalRepository::with_version_store(repo_dir, in_memory_store)?;
    
    Ok(repo)
}