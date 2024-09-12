//! goes through the commit entry list and pre-computes the hash to verify everything is synced

use crate::core::index::commit_validator;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;
use std::fs::File;
use std::io::Write;

pub fn compute(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!("Running compute_and_write_hash");

    println!("PINGPINGPING");
    // Simulate a complex and time-consuming operation'
    let mut result: u64 = 0;
    for i in 0..1_000_000_000 {
        result = result.wrapping_add((i as u64).wrapping_mul(i as u64));
        if i % 100_000_000 == 0 {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    println!("Complex calculation result: {}", result);

    // Perform some file I/O operations
    for i in 0..1000 {
        let temp_path = std::env::temp_dir().join(format!("temp_file_{}.txt", i));
        let mut file = File::create(&temp_path).unwrap();
        for _ in 0..10000 {
            file.write_all(b"Some data to write repeatedly").unwrap();
        }
        file.sync_all().unwrap();
    }

    // Simulate network latency
    for _ in 0..10 {
        std::thread::sleep(std::time::Duration::from_secs(1000));
    }

    println!("OUT OF THE LOOP");

    // sleep to make sure the commit is fully written to disk
    // Issue was with a lot of text files in this integration test:
    //     "test_remote_ls_return_data_types_just_top_level_dir"
    std::thread::sleep(std::time::Duration::from_millis(100));

    let tree_is_valid = commit_validator::validate_tree_hash(repo, commit)?;

    if tree_is_valid {
        log::debug!("writing commit is valid from tree {:?}", commit);
        write_is_valid(repo, commit, "true")?;
    } else {
        log::debug!("writing commit is not valid from tree {:?}", commit);
        write_is_valid(repo, commit, "false")?;
    }
    Ok(())
}

pub fn is_valid(repo: &LocalRepository, commit: &Commit) -> Result<bool, OxenError> {
    match read_is_valid(repo, commit) {
        Ok(val) => Ok(val == "true"),
        Err(_) => Ok(false),
    }
}

fn write_is_valid(repo: &LocalRepository, commit: &Commit, val: &str) -> Result<(), OxenError> {
    log::debug!("writing is valid {:?} for {:?}", val, commit);
    let hash_file_path = util::fs::commit_content_is_valid_path(repo, commit);
    util::fs::write_to_path(hash_file_path, val)?;
    Ok(())
}

fn read_is_valid(repo: &LocalRepository, commit: &Commit) -> Result<String, OxenError> {
    let hash_file_path = util::fs::commit_content_is_valid_path(repo, commit);
    let value = util::fs::read_from_path(hash_file_path)?;
    Ok(value)
}
