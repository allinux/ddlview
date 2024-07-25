use crate::{errors::DDLError, util::get_cloud_option};
use clap::{Parser, Subcommand};

use polars::{io::cloud::CloudOptions, lazy::frame::LazyFrame, prelude::ScanArgsParquet};
use s3util::aws::{get_credential, s3::client::AwsS3, AwsConnectionParams};

#[derive(Debug, Parser)]
pub struct SchemaArgs {
    #[arg(long)]
    path: String,
}

pub fn execute(args: SchemaArgs, cloud_option: Option<CloudOptions>) -> Result<(), anyhow::Error> {
    let params = ScanArgsParquet {
        hive_options: Default::default(),
        cloud_options: cloud_option,
        ..Default::default()
    };

    let mut df = LazyFrame::scan_parquet(args.path, params)?;

    println!("{:#?}", df.schema()?);
    Ok(())
}
