use crate::api;
use crate::constants;
use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::{CommitEntry, DataFrameDiff, Schema};
use crate::opts::DFOpts;

use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;
use std::collections::HashMap;

use crate::view::compare::{CompareDerivedDF, CompareTabular};

const ADDED_ROWS: &str = "added_rows";
const REMOVED_ROWS: &str = "removed_rows";

pub fn compare_files_by_hash(
    entry_1: CommitEntry,
    entry_2: CommitEntry,
    df_1: DataFrame,
    df_2: DataFrame,
    schema_1: Schema,
) -> Result<CompareTabular, OxenError> {
    let compare = compute_new_rows(&df_1, &df_2, &schema_1)?;
    let result = compare.to_string();
    println!("{result}");

    let derived_added_rows = CompareDerivedDF::from_compare_info(
        ADDED_ROWS,
        None,
        &entry_2.commit_id,
        &entry_1.commit_id,
        &compare.added_rows.unwrap(),
        schema_1.clone(),
    );

    let derived_removed_rows = CompareDerivedDF::from_compare_info(
        REMOVED_ROWS,
        None,
        &entry_2.commit_id,
        &entry_1.commit_id,
        &compare.removed_rows.unwrap(),
        schema_1.clone(),
    );

    let derived_dfs: HashMap<String, CompareDerivedDF> = HashMap::from([
        (ADDED_ROWS.to_string(), derived_added_rows),
        (REMOVED_ROWS.to_string(), derived_removed_rows),
    ]);

    api::local::compare::build_compare_tabular(
        &df_1,
        &df_2,
        &entry_1,
        &entry_2,
        derived_dfs,
        String::from("hash"),
    )
}

fn compute_new_rows(
    base_df: &DataFrame,
    head_df: &DataFrame,
    schema: &Schema,
) -> Result<DataFrameDiff, OxenError> {
    // Compute row indices
    let (added_indices, removed_indices) = compute_new_row_indices(base_df, head_df)?;

    // Take added from the current df
    let added_rows = if !added_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(schema);
        let head_df = tabular::transform(head_df.clone(), opts)?;
        Some(tabular::take(head_df.lazy(), added_indices)?)
    } else {
        None
    };
    log::debug!("diff_current added_rows {:?}", added_rows);

    // Take removed from versioned df
    let removed_rows = if !removed_indices.is_empty() {
        let opts = DFOpts::from_schema_columns(schema);
        let base_df = tabular::transform(base_df.clone(), opts)?;
        Some(tabular::take(base_df.lazy(), removed_indices)?)
    } else {
        None
    };
    log::debug!("diff_current removed_rows {:?}", removed_rows);

    Ok(DataFrameDiff {
        head_schema: Some(schema.to_owned()),
        base_schema: Some(schema.to_owned()),
        added_rows,
        removed_rows,
        added_cols: None,
        removed_cols: None,
    })
}

fn compute_new_row_indices(
    base_df: &DataFrame,
    head_df: &DataFrame,
) -> Result<(Vec<u32>, Vec<u32>), OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df.clone())?;
    let head_df = tabular::df_hash_rows(head_df.clone())?;

    log::debug!("diff_current got current hashes base_df {:?}", base_df);
    log::debug!("diff_current got current hashes head_df {:?}", head_df);

    let base_hash_indices: HashMap<String, u32> = base_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    let head_hash_indices: HashMap<String, u32> = head_df
        .column(constants::ROW_HASH_COL_NAME)
        .unwrap()
        .utf8()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (v.unwrap().to_string(), i as u32))
        .collect();

    // Added is all the row hashes that are in head that are not in base
    let mut added_indices: Vec<u32> = head_hash_indices
        .iter()
        .filter(|(hash, _indices)| !base_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    added_indices.sort(); // so is deterministic and returned in correct order

    // Removed is all the row hashes that are in base that are not in head
    let mut removed_indices: Vec<u32> = base_hash_indices
        .iter()
        .filter(|(hash, _indices)| !head_hash_indices.contains_key(*hash))
        .map(|(_hash, index_pair)| *index_pair)
        .collect();
    removed_indices.sort(); // so is deterministic and returned in correct order

    log::debug!("diff_current added_indices {:?}", added_indices.len());
    log::debug!("diff_current removed_indices {:?}", removed_indices.len());

    Ok((added_indices, removed_indices))
}
