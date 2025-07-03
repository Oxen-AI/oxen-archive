use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::{command, error::OxenError};

use crate::cmd::RunCmd;
pub const NAME: &str = "set";
pub struct DbSetCmd;

#[async_trait]
impl RunCmd for DbSetCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Set a value in the database for a given key.")
            .arg(Arg::new("PATH").help("The path of the database."))
            .arg(Arg::new("KEY").help("The key to set the value for."))
            .arg(Arg::new("VALUE").help("The value to set."))
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let error = "Usage: oxen db set <PATH> <KEY> <VALUE>";
        let Some(path) = args.get_one::<String>("PATH") else {
            return Err(OxenError::basic_str(error));
        };
        let Some(key) = args.get_one::<String>("KEY") else {
            return Err(OxenError::basic_str(error));
        };
        let Some(value) = args.get_one::<String>("VALUE") else {
            return Err(OxenError::basic_str(error));
        };

        command::db::set(path, key, value)?;
        println!("Set key '{}' to value '{}'", key, value);

        Ok(())
    }
}
