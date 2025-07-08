# Integration Tests

This directory contains HTTP-based integration tests for the Oxen server. These tests start real `oxen-server` processes and make actual HTTP requests to test the complete system behavior.

## Test Architecture

### Process Lifecycle
- **Each test starts its own `oxen-server` process** - ensures complete isolation
- **Server lifetime**: Created at test start, automatically killed when test completes (via `Drop` trait)
- **No shared servers** between tests - prevents state contamination

### Process Count
- **Test runner**: 1 main cargo process
- **Per test**: 1 additional `oxen-server` subprocess 
- **Parallel execution**: If running tests in parallel (`--test-threads=4`), up to 4 server processes simultaneously
- **Total**: 1 + N server processes (where N = number of concurrent tests)

## Architecture: What's Real vs Mocked

```
                    🌐 HTTP Requests (REAL)
                    ┌─────────────────────┐
                    │   reqwest::Client   │
                    │  (Integration Test) │
                    └──────────┬──────────┘
                               │ HTTP/TCP
                    ┌──────────▼──────────┐
                    │   oxen-server       │ ◄── REAL Process
                    │   (Actual Binary)   │
                    └──────────┬──────────┘
                               │ API Calls
                    ┌──────────▼──────────┐
                    │   Server Routes     │ ◄── REAL HTTP Handlers
                    │   (Actix Web)       │
                    └──────────┬──────────┘
                               │ Business Logic
                    ┌──────────▼──────────┐
                    │   Repository APIs   │ ◄── REAL Business Logic
                    │   (liboxen)         │
                    └──────────┬──────────┘
                               │ Storage Interface
                    ┌──────────▼──────────┐
                    │   VersionStore      │ ◄── REAL Interface
                    │   (Trait)           │
                    └───────────┬─────────┘
                                │ Implementation
          ┌─────────────────────┼──────────────────┐
          │                     │                  │
┌─────────▼─────────┐  ┌────────▼───────┐  ┌───────▼───────┐
│ LocalVersionStore │  │ S3VersionStore │  │ InMemoryStore │ 
│   (Production)    │  │  (Production)  │  │    (MOCK)     │ ◄── FAST!
│   Filesystem I/O  │  │   AWS S3 API   │  │   HashMap     │
│     ~50ms         │  │    ~100ms      │  │    ~5μs       │
└───────────────────┘  └────────────────┘  └───────────────┘
         │                     │                   │
    ┌────▼────┐         ┌──────▼──────┐       ┌────▼────┐
    │  Disk   │         │   AWS S3    │       │  RAM    │
    │ Storage │         │   Buckets   │       │ HashMap │
    └─────────┘         └─────────────┘       └─────────┘
```

### What This Achieves For Testing
- **🌐 Real HTTP**: Actual network requests test the full HTTP stack
- **🔧 Real Server**: Complete oxen-server process with all middleware
- **⚡ Fast Storage**: In-memory backend eliminates slow I/O (1000x speedup)
- **🎯 Real APIs**: All business logic and API endpoints are exercised
- **🔒 Isolation**: Each test gets fresh in-memory state

## Running Tests

### Run All Integration Tests
```bash
cargo test --test integration_tests -- --nocapture
```

### Run Specific Test
```bash
cargo test --test integration_tests oxen_server_health_should_be_accessible_via_http_get -- --nocapture
```

## In-Memory Storage for Speed

For faster test development and CI, use the `InMemoryVersionStore` instead of filesystem I/O:

### Available Repository Creation Functions
- `make_initialized_repo_with_test_files_in_memory()` - Basic text and CSV files with in-memory storage
- `make_initialized_repo_with_test_user_in_memory()` - CSV-focused repository with test user
- `make_initialized_repo_with_in_memory_storage()` - Empty repository with in-memory storage
- `make_initialized_repo_with_test_files()` - Basic test files with filesystem storage

### Custom Test Data with Fluent API
```rust
// For fast tests with in-memory storage
let store = TestRepositoryBuilder::new("namespace", "repo_name")
    .with_file("data.csv", "id,name\n1,Alice\n2,Bob")
    .with_file("config.json", r#"{"version": "1.0"}"#)
    .with_commit_message("Test data setup")
    .with_memory_storage()  // Fast in-memory storage
    .build()
    .unwrap();

// For realistic tests with filesystem storage (default)
let store = TestRepositoryBuilder::new("namespace", "repo_name")
    .with_file("data.csv", "id,name\n1,Alice\n2,Bob")
    .with_file("config.json", r#"{"version": "1.0"}"#)
    .with_commit_message("Test data setup")
    .build()
    .unwrap();
```

## Debugging Tips

- **Use `-- --nocapture`** to see `println!` output
- **Check server logs** in `/tmp` if server fails to start
- **Test endpoints individually** before writing complex scenarios
- **Use `reqwest::Client::builder().timeout()` for slow endpoints
- **Set unique port numbers** to avoid conflicts between parallel tests

## CI Considerations

- Tests should be **deterministic** and not rely on external services
- Use **unique temporary directories** to avoid conflicts
- **Clean up resources** properly (servers auto-cleanup via Drop trait)
- Consider **test timeouts** for CI environments