use std::ffi::OsStr;

use clap::Parser;

use polars::{
    io::{cloud::CloudOptions, csv::write::CsvWriter, parquet::write::ParquetWriter, SerWriter},
    lazy::{dsl::col, frame::LazyFrame},
    prelude::ScanArgsParquet,
};
use s3util::aws::s3::client::AwsS3;

#[derive(Debug, Parser)]
pub struct SchemaArgs {
    #[arg(long)]
    path: String,

    #[arg(long, value_delimiter(','), default_value = "parquet")]
    format: Option<Vec<String>>,

    #[arg(long, default_value = "10")]
    count: usize,

    #[arg(long, value_delimiter(','), default_value = "*")]
    column_names: Option<Vec<String>>,

    #[arg(long)]
    save_path: Option<String>,

    #[arg(long, default_value = "100")]
    max_cols: Option<String>,

    #[arg(long, default_value = "100")]
    str_len: Option<String>,
}

pub fn execute<S: AsRef<str>>(args: SchemaArgs, cloud_option: Option<CloudOptions>, aws_s3: &AwsS3<S>) -> Result<(), anyhow::Error> {
    std::env::set_var("POLARS_FMT_MAX_COLS ", args.max_cols.unwrap());
    std::env::set_var("POLARS_FMT_STR_LEN", args.str_len.unwrap());

    let params = ScanArgsParquet {
        hive_options: Default::default(),
        cloud_options: if args.path.starts_with("s3://") { cloud_option } else { None },
        ..Default::default()
    };

    let df = LazyFrame::scan_parquet(args.path, params)?
        .limit(args.count.try_into()?)
        .select(args.column_names.unwrap_or(vec!["*".to_owned()]).iter().map(|c| col(c)).collect::<Vec<_>>());

    if let Some(save_path) = args.save_path {
        let mut file = std::fs::File::create(&save_path).unwrap();

        match std::path::Path::new(&save_path).extension().and_then(OsStr::to_str) {
            Some("parquet") => {
                let _ = ParquetWriter::new(&mut file).finish(&mut df.collect()?).unwrap();
            }
            Some("csv") => {
                CsvWriter::new(&mut file).finish(&mut df.collect()?).unwrap();
            }
            _ => panic!("Currently only .parquet or .csv is supported."),
        };
    } else {
        println!("{:#?}", df.collect()?);
    }

    Ok(())
}
