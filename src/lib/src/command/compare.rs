use crate::api;
use crate::error::OxenError;
use crate::model::entry::commit_entry::CommitPath;
use crate::model::LocalRepository;
use std::path::PathBuf;

pub fn compare(
    repo: &LocalRepository,
    cpath_1: CommitPath,
    cpath_2: CommitPath,
    keys: Vec<String>,
    targets: Vec<String>,
    output: Option<PathBuf>,
) -> Result<(), OxenError> {
    api::local::compare::compare_files(repo, None, cpath_1, cpath_2, keys, targets, output)?;
    Ok(())
}
