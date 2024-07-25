use std::ffi::OsStr;

use clap::Parser;

use dyn_fmt::AsStrFormatExt;
use polars::{
    io::{cloud::CloudOptions, csv::write::CsvWriter, SerWriter},
    lazy::frame::LazyFrame,
    prelude::{ParquetWriter, ScanArgsParquet},
    sql::SQLContext,
};
use s3util::aws::s3::client::AwsS3;

#[derive(Debug, Parser)]
pub struct SchemaArgs {
    #[arg(long, value_delimiter(','))]
    path: Vec<String>,

    // #[arg(long, value_delimiter(','), default_value = "parquet")]
    // format: Option<Vec<String>>,
    #[arg(long)]
    query: String,

    #[arg(long)]
    save_path: Option<String>,

    #[arg(long, default_value = "false")]
    low_memory: Option<bool>,
}

pub fn execute<S: AsRef<str>>(args: SchemaArgs, cloud_option: Option<CloudOptions>, aws_s3: &AwsS3<S>) -> Result<(), anyhow::Error> {
    //let path_format_pair = args.path.iter().zip(args.format.unwrap()).collect::<Vec<(_, _)>>();

    let mut context = SQLContext::new();
    // let _ = path_format_pair.iter().enumerate().try_for_each(|(index, p)| -> Result<(), anyhow::Error> {
    //     match (p.0.as_str(), p.1.as_ref()) {
    //         (src_path, "parquet") => {
    //             context.register(&"df{}".format(&[index + 1]), LazyFrame::scan_parquet(src_path, params.clone())?);
    //         }
    //         (src_path, "csv") => {
    //             // let client = BUILDER.block_on(async {
    //             //     let aws_client = aws_s3.get_client().await;

    //             //     let AwsClients::S3Client(client) = aws_client;
    //             //     client
    //             // });
    //             panic!("csv format has not yet been implemented.")
    //             //context.register(&"df{}".format(&[index + 1]), DataFrame:: (src_path, params.clone())?);
    //         }
    //         (_, _) => panic!(),
    //     }
    //     //context.register(&"df{}".format(&[index + 1]), LazyFrame::scan_parquet(p.0, params.clone())?);
    //     Ok(())
    // });
    let _ = args.path.iter().enumerate().try_for_each(|(index, p)| -> Result<(), anyhow::Error> {
        let params = ScanArgsParquet {
            hive_options: Default::default(),
            cloud_options: if p.starts_with("s3://") { cloud_option.clone() } else { None },
            low_memory: args.low_memory.unwrap_or(false),
            ..Default::default()
        };
        context.register(&"df{}".format(&[index + 1]), LazyFrame::scan_parquet(p, params)?);
        Ok(())
    });

    let mut df_sql = context.execute(&args.query)?.collect()?;

    if let Some(save_path) = args.save_path {
        let mut file = std::fs::File::create(&save_path).unwrap();

        match std::path::Path::new(&save_path).extension().and_then(OsStr::to_str) {
            Some("parquet") => {
                let _ = ParquetWriter::new(&mut file).finish(&mut df_sql).unwrap();
            }
            Some("csv") => {
                CsvWriter::new(&mut file).finish(&mut df_sql).unwrap();
            }
            _ => panic!("Currently only .parquet or .csv is supported."),
        };
    } else {
        println!("{:?}", df_sql);
    }
    Ok(())
}
