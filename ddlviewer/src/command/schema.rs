use clap::Parser;

use polars::{io::cloud::CloudOptions, lazy::frame::LazyFrame, prelude::ScanArgsParquet};
use s3util::aws::s3::client::AwsS3;

#[derive(Debug, Parser)]
pub struct SchemaArgs {
    #[arg(long)]
    path: String,
}

pub fn execute<S: AsRef<str>>(args: SchemaArgs, cloud_option: Option<CloudOptions>, aws_s3: &AwsS3<S>) -> Result<(), anyhow::Error> {
    let params = ScanArgsParquet {
        hive_options: Default::default(),
        cloud_options: if args.path.starts_with("s3://") { cloud_option } else { None },
        ..Default::default()
    };

    let mut df = LazyFrame::scan_parquet(args.path, params)?;

    println!("{:#?}", df.schema()?);
    Ok(())
}
