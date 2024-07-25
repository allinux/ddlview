use clap::Parser;

use polars::{io::cloud::CloudOptions, lazy::frame::LazyFrame, prelude::ScanArgsParquet};

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
