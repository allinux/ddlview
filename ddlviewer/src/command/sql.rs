use std::ffi::OsStr;

use clap::Parser;

use polars::{
    io::{cloud::CloudOptions, csv::write::CsvWriter, SerWriter},
    lazy::frame::LazyFrame,
    prelude::{ParquetWriter, ScanArgsParquet},
    sql::SQLContext,
};
use dyn_fmt::AsStrFormatExt;

#[derive(Debug, Parser)]
pub struct SchemaArgs {
    #[arg(long, value_delimiter(','))]
    path: Vec<String>,

    #[arg(long, value_delimiter(','), default_value = "parquet")]
    format: Option<Vec<String>>,

    #[arg(long)]
    query: String,

    #[arg(long)]
    save_path: Option<String>,

    #[arg(long, default_value = "false")]
    low_memory: Option<bool>,
}

pub fn execute(args: SchemaArgs, cloud_option: Option<CloudOptions>) -> Result<(), anyhow::Error> {
    let params = ScanArgsParquet {
        hive_options: Default::default(),
        cloud_options: cloud_option,
        low_memory: args.low_memory.unwrap_or(false),
        ..Default::default()
    };

    let mut context = SQLContext::new();
    let _ = args.path.iter().enumerate().try_for_each(|(index, p)| -> Result<(), anyhow::Error> {
        context.register(&"df{}".format(&[index + 1]), LazyFrame::scan_parquet(p, params.clone())?);
        Ok(())
    });

    let mut df_sql = context.execute(&args.query)?.collect()?;

    if let Some(save_path) = args.save_path {
        let mut file = std::fs::File::create(&save_path).unwrap();
        
        match std::path::Path::new(&save_path).extension().and_then(OsStr::to_str) {
            Some("parquet") => {
                let _ = ParquetWriter::new(&mut file).finish(&mut df_sql).unwrap();
            },
            Some("csv") => { 
                CsvWriter::new(&mut file).finish(&mut df_sql).unwrap();
            },
            _ => panic!("Currently only .parquet or .csv is supported."),
        };
    } else {
        println!("{:?}", df_sql);
    }
    Ok(())
}
