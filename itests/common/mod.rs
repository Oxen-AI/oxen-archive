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

/// Builder for creating test repositories with various configurations
#[derive(Default)]
pub struct TestRepoBuilder {
    base_dir: Option<std::path::PathBuf>,
    repo_name: Option<String>,
    user_name: Option<String>,
    user_email: Option<String>,
    commit_message: Option<String>,
    files: Vec<(String, String)>, // (filename, content)
    use_in_memory_storage: bool,
    namespace: Option<String>,
}

impl TestRepoBuilder {
    /// Create a new TestRepoBuilder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the base directory for the repository
    pub fn base_dir<P: AsRef<std::path::Path>>(mut self, dir: P) -> Self {
        self.base_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Set the repository name (defaults to "test_repo")
    pub fn repo_name<S: Into<String>>(mut self, name: S) -> Self {
        self.repo_name = Some(name.into());
        self
    }

    /// Set the user name for commits (defaults to "Test User")
    pub fn user_name<S: Into<String>>(mut self, name: S) -> Self {
        self.user_name = Some(name.into());
        self
    }

    /// Set the user email for commits (defaults to "test@example.com")
    pub fn user_email<S: Into<String>>(mut self, email: S) -> Self {
        self.user_email = Some(email.into());
        self
    }

    /// Set the commit message (defaults to "Initial commit")
    pub fn commit_message<S: Into<String>>(mut self, message: S) -> Self {
        self.commit_message = Some(message.into());
        self
    }

    /// Add a file to be created in the repository
    pub fn add_file<S: Into<String>>(mut self, filename: S, content: S) -> Self {
        self.files.push((filename.into(), content.into()));
        self
    }

    /// Add a CSV file with sample data
    pub fn add_csv_file<S: Into<String>>(mut self, filename: S) -> Self {
        let csv_content = "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture\nBook,19.99,Education";
        self.files.push((filename.into(), csv_content.to_string()));
        self
    }

    /// Add a text file with sample data
    pub fn add_text_file<S: Into<String>>(mut self, filename: S) -> Self {
        let text_content = "Hello from Oxen integration test!\nThis is real file content.";
        self.files.push((filename.into(), text_content.to_string()));
        self
    }

    /// Add a sample data CSV file
    pub fn add_data_csv_file<S: Into<String>>(mut self, filename: S) -> Self {
        let csv_content = "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,Chicago";
        self.files.push((filename.into(), csv_content.to_string()));
        self
    }

    /// Enable in-memory storage for the repository
    pub fn use_in_memory_storage(mut self, use_memory: bool) -> Self {
        self.use_in_memory_storage = use_memory;
        self
    }

    /// Set the namespace for the repository (defaults to "test_user")
    pub fn namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Build the repository and return the path (and repo if using in-memory storage)
    pub async fn build(self) -> Result<TestRepoResult, Box<dyn std::error::Error>> {
        let base_dir = self.base_dir.ok_or("Base directory must be set")?;
        let repo_name = self.repo_name.unwrap_or_else(|| "test_repo".to_string());
        let namespace = self.namespace.unwrap_or_else(|| "test_user".to_string());
        let user_name = self.user_name.unwrap_or_else(|| "Test User".to_string());
        let user_email = self.user_email.unwrap_or_else(|| "test@example.com".to_string());
        let commit_message = self.commit_message.unwrap_or_else(|| "Initial commit".to_string());

        let repo_dir = base_dir.join(&namespace).join(&repo_name);
        std::fs::create_dir_all(&repo_dir)?;

        // Initialize repository with or without in-memory storage
        let repo = if self.use_in_memory_storage {
            init_repo_with_in_memory_storage(&repo_dir)?
        } else {
            liboxen::repositories::init(&repo_dir)?
        };

        // Create files if any were specified
        if !self.files.is_empty() {
            for (filename, content) in &self.files {
                let file_path = repo_dir.join(filename);
                std::fs::write(&file_path, content)?;
                liboxen::repositories::add(&repo, &file_path)?;
            }

            // Commit the files
            let user = liboxen::model::User {
                name: user_name,
                email: user_email,
            };
            liboxen::repositories::commits::commit_writer::commit_with_user(&repo, &commit_message, &user)?;
        }

        Ok(TestRepoResult {
            repo_dir,
            repo: if self.use_in_memory_storage { Some(repo) } else { None },
        })
    }
}

/// Result of building a test repository
pub struct TestRepoResult {
    pub repo_dir: std::path::PathBuf,
    pub repo: Option<liboxen::model::LocalRepository>,
}

impl TestRepoResult {
    /// Get the repository directory path
    pub fn path(&self) -> &std::path::Path {
        &self.repo_dir
    }

    /// Get the repository (if using in-memory storage)
    pub fn repo(&self) -> Option<&liboxen::model::LocalRepository> {
        self.repo.as_ref()
    }

    /// Convert to tuple for backward compatibility
    pub fn into_tuple(self) -> (std::path::PathBuf, Option<liboxen::model::LocalRepository>) {
        (self.repo_dir, self.repo)
    }
}

/// Create an initialized repository with test user configuration
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_user(base_dir: &std::path::Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("csv_repo")
        .user_name("Test")
        .user_email("test@test.com")
        .commit_message("Add CSV data")
        .add_csv_file("products.csv")
        .build()
        .await?;
    
    Ok(result.repo_dir)
}

/// Create an initialized repository with test user and files
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_files(base_dir: &std::path::Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("test_repo")
        .user_name("Test User")
        .user_email("test@example.com")
        .commit_message("Initial commit with test files")
        .add_text_file("test.txt")
        .add_data_csv_file("data.csv")
        .build()
        .await?;
    
    Ok(result.repo_dir)
}

/// Create an initialized repository with test user and CSV file using in-memory storage
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_user_in_memory(base_dir: &std::path::Path) -> Result<(std::path::PathBuf, liboxen::model::LocalRepository), Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("csv_repo")
        .user_name("Test")
        .user_email("test@test.com")
        .commit_message("Add CSV data")
        .add_csv_file("products.csv")
        .use_in_memory_storage(true)
        .build()
        .await?;
    
    Ok((result.repo_dir, result.repo.unwrap()))
}

/// Create an initialized repository with in-memory storage for testing
#[allow(dead_code)]
pub async fn make_initialized_repo_with_in_memory_storage(base_dir: &std::path::Path) -> Result<(std::path::PathBuf, liboxen::model::LocalRepository), Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("memory_repo")
        .use_in_memory_storage(true)
        .build()
        .await?;
    
    Ok((result.repo_dir, result.repo.unwrap()))
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
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("memory_test_repo")
        .user_name("Test User")
        .user_email("test@example.com")
        .commit_message("Initial commit with test files")
        .add_text_file("test.txt")
        .add_data_csv_file("data.csv")
        .use_in_memory_storage(true)
        .build()
        .await?;
    
    Ok((result.repo_dir, result.repo.unwrap()))
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