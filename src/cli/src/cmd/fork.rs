use async_trait::async_trait;
use clap::{arg, Command};
use std::path::PathBuf;

use liboxen::error::OxenError;
use liboxen::repositories::fork;

use crate::cmd::RunCmd;

pub const NAME: &str = "fork";
pub struct ForkCmd;

#[async_trait]
impl RunCmd for ForkCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Fork a repository to a new destination directory")
            .arg_required_else_help(true)
            .arg(arg!(<SOURCE> "Path to the source repository to fork"))
            .arg(arg!(<DESTINATION> "Path to the destination directory for the forked repository"))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let source = args.get_one::<String>("SOURCE").expect("required");
        let destination = args.get_one::<String>("DESTINATION").expect("required");

        let source_path = PathBuf::from(source);
        let destination_path = PathBuf::from(destination);

        // Validate that source exists
        if !source_path.exists() {
            return Err(OxenError::basic_str(format!(
                "Source repository does not exist: {}",
                source_path.display()
            )));
        }

        // Validate that source is a directory
        if !source_path.is_dir() {
            return Err(OxenError::basic_str(format!(
                "Source must be a directory: {}",
                source_path.display()
            )));
        }

        println!("Forking repository from {} to {}", source_path.display(), destination_path.display());
        
        let response = fork::start_fork(source_path, destination_path)?;
        
        println!("Fork started successfully!");
        println!("Repository: {}", response.repository);
        println!("Status: {}", response.fork_status);
        println!("Use 'oxen fork-status <destination>' to check the progress.");

        Ok(())
    }
}
