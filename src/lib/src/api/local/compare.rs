use crate::constants::{CACHE_DIR, COMPARES_DIR};
use crate::core::df::tabular::{self};
use crate::error::OxenError;
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

pub fn compare_files(
    repo: &LocalRepository,
    compare_id: Option<&str>,
    entry_1: CommitEntry,
    entry_2: CommitEntry,
    keys: Vec<String>,
    targets: Vec<String>,
    output: Option<PathBuf>,
) -> Result<CompareTabular, OxenError> {
    // Assert that the files exist in their respective commits and are tabular.
    let version_file_1 =
        api::local::diff::get_version_file_from_commit_id(repo, &entry_1.commit_id, &entry_1.path)?;
    let version_file_2 =
        api::local::diff::get_version_file_from_commit_id(repo, &entry_2.commit_id, &entry_2.path)?;

    if !util::fs::is_tabular(&version_file_1) || !util::fs::is_tabular(&version_file_2) {
        return Err(OxenError::invalid_file_type(format!(
            "Compare not supported for non-tabular files, found {:?} and {:?}",
            entry_1.path, entry_2.path
        )));
    }

    // Read DFs and get schemas
    let df_1 = tabular::read_df(&version_file_1, DFOpts::empty())?;
    let df_2 = tabular::read_df(&version_file_2, DFOpts::empty())?;

    let schema_1 = Schema::from_polars(&df_1.schema());
    let schema_2 = Schema::from_polars(&df_2.schema());

    if keys.is_empty() {
        hash_compare::compare_files_by_hash(entry_1, entry_2, df_1, df_2, schema_1)
    } else {
        join_compare::compare_files_by_join(
            repo, compare_id, entry_1, entry_2, df_1, df_2, schema_1, schema_2, keys, targets,
            output,
        )
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

fn build_compare_tabular(
    df_1: &DataFrame,
    df_2: &DataFrame,
    entry_1: &CommitEntry,
    entry_2: &CommitEntry,
    derived_dfs: HashMap<String, CompareDerivedDF>,
    compare_type: String,
) -> Result<CompareTabular, OxenError> {
    let df_1_size = DataFrameSize::from_df(df_1);
    let df_2_size = DataFrameSize::from_df(df_2);

    let path_1 = entry_1.path.clone();
    let path_2 = entry_2.path.clone();

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

    let source_df_left = CompareSourceDF {
        name: LEFT.to_string(),
        path: entry_1.path.clone(),
        version: entry_1.commit_id.clone(),
        schema: og_schema_1.schema.clone(),
        size: df_1_size,
    };

    let source_df_right = CompareSourceDF {
        name: RIGHT.to_string(),
        path: entry_2.path.clone(),
        version: entry_2.commit_id.clone(),
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
