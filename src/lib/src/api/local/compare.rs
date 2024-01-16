use crate::constants::{CACHE_DIR, COMPARES_DIR};
use crate::core::df::tabular::{self};
use crate::error::OxenError;
use crate::model::entry::commit_entry::CommitPath;
use crate::model::{CommitEntry, DataFrameSize, LocalRepository, Schema};
use crate::opts::DFOpts;
use crate::view::compare::{CompareDerivedDF, CompareDupes, CompareSourceDF, CompareTabular};
use crate::view::schema::SchemaWithPath;
use crate::{api, util};

use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::path::PathBuf;

const LEFT: &str = "left";
const RIGHT: &str = "right";
const KEYS_HASH_COL: &str = "_keys_hash";

pub mod hash_compare;
pub mod join_compare;
pub mod utf8_compare;

pub struct CompareItemData {
    pub commit_path: CommitPath,
    pub df: DataFrame,
    pub schema: Schema,
}

pub fn compare_files(
    repo: &LocalRepository,
    compare_id: Option<&str>,
    cpath_1: CommitPath,
    cpath_2: CommitPath,
    keys: Vec<String>,
    targets: Vec<String>,
    output: Option<PathBuf>,
) -> Result<(), OxenError> {
    let version_file_1 = get_version_file(repo, &cpath_1)?;
    let version_file_2 = get_version_file(repo, &cpath_2)?;

    if util::fs::is_tabular(&version_file_1) && util::fs::is_tabular(&version_file_2) {
        compare_tabular(repo, compare_id, cpath_1, cpath_2, keys, targets, output)?;
        Ok(())
    } else if util::fs::is_utf8(&version_file_1) && util::fs::is_utf8(&version_file_2) {
        let result = utf8_compare::compare(&version_file_1, &version_file_2)?;
        println!("{result}");
        Ok(())
    } else {
        return Err(OxenError::invalid_file_type(format!(
            "Compare not supported for files, found {:?} and {:?}",
            cpath_1.path, cpath_2.path
        )));
    }
}

pub fn get_cached_compare(
    repo: &LocalRepository,
    compare_id: &str,
    left_entry: &CommitEntry,
    right_entry: &CommitEntry,
) -> Result<Option<CompareTabular>, OxenError> {
    join_compare::get_cached_compare(repo, compare_id, left_entry, right_entry)
}

pub fn get_compare_dir(repo: &LocalRepository, compare_id: &str) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(CACHE_DIR)
        .join(COMPARES_DIR)
        .join(compare_id)
}

fn compare_tabular(
    repo: &LocalRepository,
    compare_id: Option<&str>,
    cpath_1: CommitPath,
    cpath_2: CommitPath,
    keys: Vec<String>,
    targets: Vec<String>,
    output: Option<PathBuf>,
) -> Result<CompareTabular, OxenError> {
    let version_file_1 = get_version_file(repo, &cpath_1)?;
    let version_file_2 = get_version_file(repo, &cpath_2)?;

    // Read DFs and get schemas
    let df_1 = tabular::read_df(version_file_1, DFOpts::empty())?;
    let df_2 = tabular::read_df(version_file_2, DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());

    let left_item = CompareItemData {
        commit_path: cpath_1,
        df: df_1,
        schema: schema_1,
    };

    let right_item = CompareItemData {
        commit_path: cpath_2,
        df: df_2,
        schema: schema_2,
    };

    if keys.is_empty() {
        hash_compare::compare(left_item, right_item)
    } else {
        join_compare::compare(
            repo, compare_id, left_item, right_item, keys, targets, output,
        )
    }
}

fn build_compare_tabular(
    df_1: &DataFrame,
    df_2: &DataFrame,
    left_item: &CompareItemData,
    right_item: &CompareItemData,
    derived_dfs: HashMap<String, CompareDerivedDF>,
    compare_type: String,
) -> Result<CompareTabular, OxenError> {
    let df_1_size = DataFrameSize::from_df(df_1);
    let df_2_size = DataFrameSize::from_df(df_2);

    let path_1 = left_item.commit_path.path.clone();
    let path_2 = right_item.commit_path.path.clone();

    let n_dupes_1 = tabular::n_duped_rows(df_1, &[KEYS_HASH_COL])?;
    let n_dupes_2 = tabular::n_duped_rows(df_2, &[KEYS_HASH_COL])?;

    let dupes = CompareDupes {
        left: n_dupes_1,
        right: n_dupes_2,
    };

    let og_schema_1 = SchemaWithPath {
        path: path_1.as_os_str().to_str().map(|s| s.to_owned()).unwrap(),
        schema: Schema::from_polars(&df_1.schema()),
    };

    let og_schema_2 = SchemaWithPath {
        path: path_2.as_os_str().to_str().map(|s| s.to_owned()).unwrap(),
        schema: Schema::from_polars(&df_2.schema()),
    };

    let version_1 = match &left_item.commit_path.commit {
        Some(commit) => commit.id.clone(),
        None => "".to_string(),
    };

    let version_2 = match &right_item.commit_path.commit {
        Some(commit) => commit.id.clone(),
        None => "".to_string(),
    };

    let source_df_left = CompareSourceDF {
        name: LEFT.to_string(),
        path: path_1,
        version: version_1,
        schema: og_schema_1.schema.clone(),
        size: df_1_size,
    };

    let source_df_right = CompareSourceDF {
        name: RIGHT.to_string(),
        path: path_2,
        version: version_2,
        schema: og_schema_2.schema.clone(),
        size: df_2_size,
    };

    let source_dfs: HashMap<String, CompareSourceDF> = HashMap::from([
        (LEFT.to_string(), source_df_left),
        (RIGHT.to_string(), source_df_right),
    ]);

    Ok(CompareTabular {
        compare_type,
        source: source_dfs,
        derived: derived_dfs,
        dupes,
    })
}

fn get_version_file(repo: &LocalRepository, cpath: &CommitPath) -> Result<PathBuf, OxenError> {
    if cpath.is_from_working_directory {
        Ok(cpath.path.clone())
    } else {
        let commit = &cpath.commit.clone().unwrap();

        let entry =
            api::local::entries::get_commit_entry(repo, commit, &cpath.path)?.ok_or_else(|| {
                OxenError::ResourceNotFound(
                    format!("{}@{}", cpath.path.display(), commit.id).into(),
                )
            })?;

        api::local::diff::get_version_file_from_commit_id(repo, &entry.commit_id, &entry.path)
    }
}
