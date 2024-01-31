use crate::constants;
use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::schema::Field;
use crate::model::Schema;
use crate::opts::DFOpts;
use crate::view::compare::CompareDupes;
use crate::view::compare::CompareTabularRaw;

use polars::prelude::DataFrame;
use polars::prelude::IntoLazy;
use std::collections::HashMap;

pub fn compare(
    base_df: &DataFrame,
    head_df: &DataFrame,
    schema_1: &Schema,
    schema_2: &Schema,
    keys: Vec<&str>,
) -> Result<CompareTabularRaw, OxenError> {
    let added_fields = schema_1.added_fields(schema_2);
    let removed_fields = schema_1.removed_fields(schema_2);

    if !added_fields.is_empty() || !removed_fields.is_empty() {
        let result = get_new_and_removed_cols(head_df, base_df, added_fields, removed_fields);

        match result {
            Ok((added_cols, removed_cols)) => Ok(CompareTabularRaw {
                added_cols_df: added_cols,
                removed_cols_df: removed_cols,
                diff_df: DataFrame::default(),
                match_df: DataFrame::default(),
                left_only_df: DataFrame::default(),
                right_only_df: DataFrame::default(),
                dupes: CompareDupes { left: 0, right: 0 },
                compare_strategy: super::CompareStrategy::Hash,
            }),
            Err(err) => Err(err),
        }
    } else {
        // Compute row indices
        let (added_indices, removed_indices) = compute_new_row_indices(base_df, head_df, keys)?;

        // Take added from the current df
        let added_rows = if !added_indices.is_empty() {
            let opts = DFOpts::from_schema_columns(schema_1);
            let head_df = tabular::transform(head_df.clone(), opts)?;
            tabular::take(head_df.lazy(), added_indices)?
        } else {
            DataFrame::default()
        };

        // Take removed from versioned df
        let removed_rows = if !removed_indices.is_empty() {
            let opts = DFOpts::from_schema_columns(schema_1);
            let base_df = tabular::transform(base_df.clone(), opts)?;
            tabular::take(base_df.lazy(), removed_indices.clone())?
        } else {
            DataFrame::default()
        };

        Ok(CompareTabularRaw {
            added_cols_df: DataFrame::default(),
            removed_cols_df: DataFrame::default(),
            diff_df: DataFrame::default(),
            match_df: DataFrame::default(),
            left_only_df: removed_rows,
            right_only_df: added_rows,
            dupes: CompareDupes { left: 0, right: 0 },
            compare_strategy: super::CompareStrategy::Hash,
        })
    }
}

fn get_new_and_removed_cols(
    base_df: &DataFrame,
    head_df: &DataFrame,
    added_fields: Vec<Field>,
    removed_fields: Vec<Field>,
) -> Result<(DataFrame, DataFrame), OxenError> {
    let str_fields: Vec<String> = added_fields.iter().map(|f| f.name.to_owned()).collect();
    let added_cols = head_df.select(str_fields)?;
    log::debug!("Got added col df: {}", added_cols);

    let str_fields: Vec<String> = removed_fields.iter().map(|f| f.name.to_owned()).collect();
    let removed_cols = base_df.select(str_fields)?;
    log::debug!("Got removed col df: {}", removed_cols);

    Ok((added_cols, removed_cols))
}

fn compute_new_row_indices(
    base_df: &DataFrame,
    head_df: &DataFrame,
    keys: Vec<&str>,
) -> Result<(Vec<u32>, Vec<u32>), OxenError> {
    // Hash the rows
    let base_df = tabular::df_hash_rows(base_df.clone(), Some(keys.clone()))?;
    let head_df = tabular::df_hash_rows(head_df.clone(), Some(keys.clone()))?;

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
