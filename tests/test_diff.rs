use std::path::Path;
use std::path::PathBuf;

use liboxen::api;
use liboxen::api::local::compare::CompareStrategy;
use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::entry::commit_entry::CommitPath;
use liboxen::model::ContentType;
use liboxen::opts::DFOpts;
use liboxen::test;
use liboxen::util;

// Test diff during a merge conflict should show conflicts for a dataframe
#[tokio::test]
async fn test_has_diff_merge_conflicts() -> Result<(), OxenError> {
    test::run_empty_local_repo_test_async(|repo| async move {
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();
        let data_path = repo.path.join("data.csv");
        util::fs::write_to_path(&data_path, "file,label\nimages/0.png,dog\n")?;
        command::add(&repo, &data_path)?;
        command::commit(&repo, "Add initial data.csv file with dog")?;

        // Add a fish label to the file on a branch
        let fish_branch_name = "add-fish-label";
        api::local::branches::create_checkout(&repo, fish_branch_name)?;
        let data_path = test::append_line_txt_file(data_path, "images/fish.png,fish\n")?;
        command::add(&repo, &data_path)?;
        command::commit(&repo, "Adding fish to data.csv file")?;

        // Checkout main, and branch from it to another branch to add a cat label
        command::checkout(&repo, &og_branch.name).await?;
        let cat_branch_name = "add-cat-label";
        api::local::branches::create_checkout(&repo, cat_branch_name)?;
        let data_path = test::append_line_txt_file(data_path, "images/cat.png,cat\n")?;
        command::add(&repo, &data_path)?;
        command::commit(&repo, "Adding cat to data.csv file")?;

        // Checkout main again
        command::checkout(&repo, &og_branch.name).await?;

        // Merge the fish branch in
        let result = command::merge(&repo, fish_branch_name)?;
        assert!(result.is_some());

        // And then the cat branch should have conflicts
        let result = command::merge(&repo, cat_branch_name)?;
        assert!(result.is_none());

        // Make sure we can access the conflicts in the status command
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        let head_commit = Some(api::local::commits::head_commit(&repo)?);

        let cpath_1 = CommitPath {
            commit: head_commit.clone(),
            path: PathBuf::from("data.csv"),
        };

        let cpath_2 = CommitPath {
            commit: head_commit.clone(),
            path: PathBuf::from("data.csv"),
        };

        // Get the diff dataframe
        let diff = command::compare(
            CompareStrategy::Hash,
            &repo,
            cpath_1,
            cpath_2,
            vec![],
            vec![],
            None,
        )?;
        log::debug!("{diff:?}");
        println!("{diff:?}");

        assert_eq!(
            diff,
            r"Added Rows

shape: (1, 2)
┌─────────────────┬───────┐
│ file            ┆ label │
│ ---             ┆ ---   │
│ str             ┆ str   │
╞═════════════════╪═══════╡
│ images/fish.png ┆ fish  │
└─────────────────┴───────┘


Removed Rows

shape: (1, 2)
┌────────────────┬───────┐
│ file           ┆ label │
│ ---            ┆ ---   │
│ str            ┆ str   │
╞════════════════╪═══════╡
│ images/cat.png ┆ cat   │
└────────────────┴───────┘

"
        );

        Ok(())
    })
    .await
}

#[test]
fn test_diff_tabular_add_col() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(bbox_filename.clone());

        let mut opts = DFOpts::empty();
        // Add Column
        opts.add_col = Some(String::from("is_cute:unknown:str"));
        // Save to Output
        opts.output = Some(bbox_file.clone());
        // Perform df transform
        command::df(&bbox_file, opts)?;

        let head_commit = Some(api::local::commits::head_commit(&repo)?);

        let cpath_1 = CommitPath {
            commit: None,
            path: bbox_file.clone(),
        };

        let cpath_2 = CommitPath {
            commit: head_commit.clone(),
            path: bbox_filename.clone(),
        };

        // Get the diff dataframe
        let diff = command::compare(
            CompareStrategy::Hash,
            &repo,
            cpath_1,
            cpath_2,
            vec![],
            vec![],
            None,
        )?;

        println!("{:?}", diff);

        assert_eq!(
            diff,
            r"Added Columns

shape: (6, 1)
┌─────────┐
│ is_cute │
│ ---     │
│ str     │
╞═════════╡
│ unknown │
│ unknown │
│ unknown │
│ unknown │
│ unknown │
│ unknown │
└─────────┘

"
        );

        Ok(())
    })
}

// #[test]
// fn test_diff_tabular_add_row() -> Result<(), OxenError> {
//     test::run_training_data_repo_test_fully_committed(|repo| {
//         let bbox_filename = Path::new("annotations")
//             .join("train")
//             .join("bounding_box.csv");
//         let bbox_file = repo.path.join(bbox_filename);

//         let mut opts = DFOpts::empty();
//         // Add Row
//         opts.add_row = Some(String::from("train/cat_100.jpg,cat,100.0,100.0,100,100"));
//         opts.content_type = ContentType::Csv;
//         // Save to Output
//         opts.output = Some(bbox_file.clone());
//         // Perform df transform
//         command::df(&bbox_file, opts)?;

//         match command::diff(&repo, None, &bbox_file) {
//             Ok(diff) => {
//                 println!("{diff}");

//                 assert_eq!(
//                     diff,
//                     r"Added Rows

// shape: (1, 6)
// ┌───────────────────┬───────┬───────┬───────┬───────┬────────┐
// │ file              ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
// │ ---               ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
// │ str               ┆ str   ┆ f64   ┆ f64   ┆ i64   ┆ i64    │
// ╞═══════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
// │ train/cat_100.jpg ┆ cat   ┆ 100.0 ┆ 100.0 ┆ 100   ┆ 100    │
// └───────────────────┴───────┴───────┴───────┴───────┴────────┘

// "
//                 );
//             }
//             Err(err) => {
//                 panic!("Error diffing: {}", err);
//             }
//         }

//         Ok(())
//     })
// }

// #[test]
// fn test_diff_tabular_remove_row() -> Result<(), OxenError> {
//     test::run_training_data_repo_test_fully_committed(|repo| {
//         let bbox_filename = Path::new("annotations")
//             .join("train")
//             .join("bounding_box.csv");
//         let bbox_file = repo.path.join(bbox_filename);

//         // Remove a row
//         let bbox_file = test::modify_txt_file(
//             bbox_file,
//             r"
// file,label,min_x,min_y,width,height
// train/dog_1.jpg,dog,101.5,32.0,385,330
// train/dog_2.jpg,dog,7.0,29.5,246,247
// train/cat_2.jpg,cat,30.5,44.0,333,396
// ",
//         )?;

//         match command::diff(&repo, None, bbox_file) {
//             Ok(diff) => {
//                 println!("{diff}");

//                 assert_eq!(
//                     diff,
//                     r"Removed Rows

// shape: (3, 6)
// ┌─────────────────┬───────┬───────┬───────┬───────┬────────┐
// │ file            ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
// │ ---             ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
// │ str             ┆ str   ┆ f64   ┆ f64   ┆ i64   ┆ i64    │
// ╞═════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
// │ train/dog_1.jpg ┆ dog   ┆ 102.5 ┆ 31.0  ┆ 386   ┆ 330    │
// │ train/dog_3.jpg ┆ dog   ┆ 19.0  ┆ 63.5  ┆ 376   ┆ 421    │
// │ train/cat_1.jpg ┆ cat   ┆ 57.0  ┆ 35.5  ┆ 304   ┆ 427    │
// └─────────────────┴───────┴───────┴───────┴───────┴────────┘

// "
//                 );
//             }
//             Err(err) => {
//                 panic!("Error diffing: {}", err);
//             }
//         }

//         Ok(())
//     })
// }
