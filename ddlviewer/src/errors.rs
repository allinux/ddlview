use polars::error::PolarsError;
use std::path::PathBuf;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DDLError {
    #[error("File {0} not found, please check if it exists")]
    FileNotFound(PathBuf),
    #[error("Could not open file: {0}")]
    CouldNotOpenFile(PathBuf),
    #[error("File already exists: {0}")]
    FileExists(PathBuf),
}
