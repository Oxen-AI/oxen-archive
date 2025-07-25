use async_trait::async_trait;
use clap::{arg, Command};
use std::path::PathBuf;

use liboxen::error::OxenError;
use liboxen::repositories::fork;

use crate::cmd::RunCmd;

pub const NAME: &str = "fork-status";
pub struct ForkStatusCmd;

#[async_trait]
impl RunCmd for ForkStatusCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Check the status of a fork operation")
            .arg_required_else_help(true)
            .arg(arg!(<REPOSITORY> "Path to the forked repository to check status for"))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repository = args.get_one::<String>("REPOSITORY").expect("required");
        let repo_path = PathBuf::from(repository);

        // Validate that repository path exists
        if !repo_path.exists() {
            return Err(OxenError::basic_str(format!(
                "Repository path does not exist: {}",
                repo_path.display()
            )));
        }

        let response = fork::get_fork_status(&repo_path)?;
        
        println!("Fork Status:");
        println!("Repository: {}", response.repository);
        println!("Status: {}", response.status);

        Ok(())
    }
}
