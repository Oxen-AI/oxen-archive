use crate::error::OxenError;
use crate::util::fs as oxen_fs;
use crate::view::fork::{ForkStartResponse, ForkStatus, ForkStatusResponse};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{BufWriter, BufReader, ErrorKind};
use jwalk::WalkDir;
use std::fs::File;
use std::thread;

pub const FORK_STATUS_FILE: &str = ".oxen/fork_status.toml";

fn write_status(repo_path: &Path, status: &ForkStatus) -> Result<(), OxenError> {
    let status_path = repo_path.join(FORK_STATUS_FILE);
    if let Some(parent) = status_path.parent() {
        oxen_fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&status_path)?;
    
    let mut writer = BufWriter::new(file);

    bincode::serialize_into(&mut writer, status).map_err(|e| {
        OxenError::basic_str(format!("Failed to serialize and append fork status: {}", e))
    })?;
    Ok(())
}

fn read_status(repo_path: &Path) -> Result<Option<ForkStatus>, OxenError> {
    let status_path = repo_path.join(FORK_STATUS_FILE);
    if !status_path.exists() {
        return Ok(None);
    }
    let file = File::open(&status_path)?;
    let mut reader = BufReader::new(file);

    let mut last_status: Option<ForkStatus> = None;

    loop {
        match bincode::deserialize_from(&mut reader) {
            Ok(status) => {
                last_status = Some(status);
            }
            Err(e) => {

                if let bincode::ErrorKind::Io(io_err) = e.as_ref() {
                    if io_err.kind() == ErrorKind::UnexpectedEof {
                        // We've successfully read all complete records.
                        break;
                    }
                }
                

                log::error!(
                    "Failed to deserialize from fork status log: {:?}, error: {}",
                    status_path,
                    e
                );

                return Err(OxenError::basic_str(format!(
                    "Corrupt fork status log: {}",
                    e
                )));
            }
        }
    }
    Ok(last_status)
}

pub fn start_fork(
    original_path: PathBuf,
    new_path: PathBuf,
) -> Result<ForkStartResponse, OxenError> {
    if new_path.exists() {
        return Err(OxenError::basic_str(format!(
            "A file already exists at the destination path: {}",
            new_path.to_string_lossy()
        )));
    }

    oxen_fs::create_dir_all(&new_path)?;
    write_status(&new_path, &ForkStatus::Counting(0))?;

    let mut current_count: usize = 0;
    let new_path_clone = new_path.clone();

    thread::spawn(move || {
        let total_items = match count_items(&original_path) {
            Ok(count) => {
                current_count = current_count+ count;
                write_status(&new_path, &ForkStatus::Counting(current_count)).unwrap_or_else(|e| {
                    log::error!("Failed to write counting status: {}", e);
                });
                count as f32
            },
            Err(e) => {
                log::error!("Failed to count items: {}", e);
                write_status(&new_path, &ForkStatus::Failed(e.to_string())).unwrap_or_else(|e| {
                    log::error!("Failed to write error status: {}", e);
                });
                return;
            }
        };
        let mut copied_items = 0.0;
        match copy_dir_recursive(
            &original_path,
            &new_path,
            &new_path,
            total_items,
            &mut copied_items,
        ) {
            Ok(()) => {
                write_status(&new_path, &ForkStatus::Complete).unwrap_or_else(|e| {
                    log::error!("Failed to write completion status: {}", e);
                });
            }
            Err(e) => {
                write_status(&new_path, &ForkStatus::Failed(e.to_string())).unwrap_or_else(|e| {
                    log::error!("Failed to write error status: {}", e);
                });
            }
        }
    });

    Ok(ForkStartResponse {
        repository: new_path_clone.to_string_lossy().to_string(),
        fork_status: ForkStatus::Started.to_string(),
    })
}

pub fn get_fork_status(repo_path: &Path) -> Result<ForkStatusResponse, OxenError> {
    let status = read_status(repo_path)?.ok_or_else(OxenError::fork_status_not_found)?;

    Ok(ForkStatusResponse {
        repository: repo_path.to_string_lossy().to_string(),
        status: match status {
            ForkStatus::Started => ForkStatus::Started.to_string(),
            ForkStatus::Counting(_) => ForkStatus::Counting(0).to_string(),
            ForkStatus::InProgress(_) => ForkStatus::InProgress(0.0).to_string(),
            ForkStatus::Complete => ForkStatus::Complete.to_string(),
            ForkStatus::Failed(_) => ForkStatus::Failed("".to_string()).to_string(),
        },
        progress: match status {
            ForkStatus::InProgress(p) => Some(p),
            ForkStatus::Counting(c) => Some(c as f32),
            _ => None,
        },
        error: match status {
            ForkStatus::Failed(e) => Some(e),
            _ => None,
        },
    })
}

fn copy_dir_recursive(
    src: &Path,
    dst: &Path,
    status_repo: &Path,
    total_items: f32,
    copied_items: &mut f32,
) -> Result<(), OxenError> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let dest_path = dst.join(entry.file_name());

        if path.ends_with(".oxen/workspaces") {
            continue;
        }

        if path.is_dir() {
            oxen_fs::create_dir_all(&dest_path)?;
            copy_dir_recursive(&path, &dest_path, status_repo, total_items, copied_items)?;
        } else {
            fs::copy(&path, &dest_path)?;
            *copied_items += 1.0;

            let progress = if total_items > 0.0 {
                (*copied_items / total_items) * 100.0
            } else {
                100.0 // Assume completion if there are no items to copy
            };
            write_status(status_repo, &ForkStatus::InProgress(progress))?;
        }
    }
    Ok(())
}

fn count_items(path: &Path) -> Result<usize, OxenError> {

    // Create a walker that will recursively visit all entries.
    // .into_iter() gives us an iterator over Result<DirEntry, walkdir::Error>.
    let walker = WalkDir::new(path);


    let file_count = walker
        .into_iter()
        .filter_map(Result::ok) // A more concise way to unwrap the Result
        .filter(|entry| {
            if entry.file_type().is_dir() {
                return false;
            }
            if entry.file_type().is_file() {
                if entry.path().ends_with(".oxen/workspaces") {
                    return false;
                }
                return true;
            }
            false
        })
        .count();

    Ok(file_count)
}


#[cfg(test)]
mod tests {
    use std::time::Duration;

    use uuid::Uuid;

    use super::*;
    use crate::error::OxenError;
    use crate::test;

    #[tokio::test]
    async fn test_fork_operations() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|original_repo| {
            async move {
                let original_repo_path = original_repo.path;
                let forked_repo_path = original_repo_path
                    .parent()
                    .unwrap()
                    .join("forked")
                    .join(Uuid::new_v4().to_string());

                // Fork creates new repo
                if forked_repo_path.exists() {
                    test::maybe_cleanup_repo(&forked_repo_path)?;
                }

                let dir_path = original_repo_path.join("dir");
                // Create a workspace directory and add a file to it
                oxen_fs::create_dir_all(&dir_path)?;
                let file_path = dir_path.join("test_file.txt");
                std::fs::write(file_path, "test file content")?;

                start_fork(original_repo_path.clone(), forked_repo_path.clone())?;
                let mut current_status = "in_progress".to_string();
                let mut attempts = 0;
                const MAX_ATTEMPTS: u32 = 500; // 5 seconds timeout (50 * 100ms)

                while current_status == "in_progress" && attempts < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(1000)).await; // Wait for 100 milliseconds
                    current_status = match get_fork_status(&forked_repo_path) {
                        Ok(status) => status.status,
                        Err(e) => {
                            if let OxenError::ForkStatusNotFound(_) = e {
                                "in_progress".to_string()
                            } else {
                                return Err(e);
                            }
                        }
                    };
                    attempts += 1;
                }

                if attempts >= MAX_ATTEMPTS {
                    return Err(OxenError::basic_str("Fork operation timed out"));
                }

                let file_path = original_repo_path.clone().join("dir/test_file.txt");

                assert!(forked_repo_path.exists());
                // Verify that the content of .oxen/config.toml is the same in both repos
                let new_file_path = forked_repo_path.join("dir/test_file.txt");
                let original_content = fs::read_to_string(&file_path)?;
                let mut retries = 10;
                let mut sleep_time = 100;
                let new_content = loop {
                    if new_file_path.exists() {
                        break fs::read_to_string(&new_file_path)?;
                    }

                    if retries == 0 {
                        return Err(OxenError::basic_str("File not found after retries"));
                    }

                    tokio::time::sleep(Duration::from_millis(sleep_time)).await;
                    retries -= 1;
                    sleep_time += 200;
                };

                assert_eq!(
                    original_content, new_content,
                    "The content of test_file.txt should be the same in both repositories"
                );

                // Fork fails if repo exists
                let new_repo_path_1 = original_repo_path
                    .parent()
                    .unwrap()
                    .join("forked")
                    .join(Uuid::new_v4().to_string());
                if new_repo_path_1.exists() {
                    test::maybe_cleanup_repo(&new_repo_path_1)?;
                }
                oxen_fs::create_dir_all(&new_repo_path_1)?;

                let result = start_fork(original_repo_path.clone(), new_repo_path_1.clone());
                assert!(
                    result.is_err(),
                    "Expected an error because the repo already exists."
                );

                // Fork excludes workspaces
                let new_repo_path_2 = original_repo_path
                    .parent()
                    .unwrap()
                    .join("forked")
                    .join(Uuid::new_v4().to_string());

                let workspaces_path = original_repo_path.join(".oxen/workspaces");
                // Create a workspace directory and add a file to it
                oxen_fs::create_dir_all(&workspaces_path)?;
                let workspace_file = workspaces_path.join("test_workspace.txt");
                std::fs::write(workspace_file, "test workspace content")?;

                start_fork(original_repo_path.clone(), new_repo_path_2.clone())?;
                let mut current_status = "in_progress".to_string();
                let mut attempts = 0;

                while current_status == "in_progress" && attempts < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    current_status = match get_fork_status(&new_repo_path_2) {
                        Ok(status) => status.status,
                        Err(e) => {
                            if let OxenError::ForkStatusNotFound(_) = e {
                                // Status file doesn't exist yet, continue polling
                                "in_progress".to_string()
                            } else {
                                // Propagate other errors
                                return Err(e);
                            }
                        }
                    };
                    attempts += 1;
                }

                if attempts >= MAX_ATTEMPTS {
                    return Err(OxenError::basic_str("Fork operation timed out"));
                }

                // Check that the new repository exists
                assert!(new_repo_path_2.clone().exists());

                // Verify that .oxen/workspaces was not copied
                let new_workspaces_path = new_repo_path_2.join(".oxen/workspaces");
                assert!(
                    !new_workspaces_path.exists(),
                    ".oxen/workspaces should not be copied"
                );

                test::maybe_cleanup_repo(&new_repo_path_2)?;
                // Get prefix of new repo path 2 for cleanup
                let new_repo_path_2_prefix = new_repo_path_2.parent().unwrap();
                if new_repo_path_2_prefix.exists() {
                    let mut retries = 3;
                    while retries > 0 && new_repo_path_2_prefix.exists() {
                        match std::fs::remove_dir_all(new_repo_path_2_prefix) {
                            Ok(_) => break,
                            Err(e) => {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                retries -= 1;
                                if retries == 0 {
                                    return Err(OxenError::from(e));
                                }
                            }
                        }
                    }
                }

                Ok(())
            }
        })
        .await
    }
}
