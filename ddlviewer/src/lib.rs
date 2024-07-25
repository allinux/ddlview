use polars::prelude::{LazyFrame, ScanArgsParquet};
use s3util::aws::{
    get_credential,
    s3::client::AwsS3,
    AwsConnectionParams,
};
use util::get_cloud_option;

pub mod command;
pub mod errors;
pub mod util;

pub fn run() -> Result<(), anyhow::Error> {
    // let s3 = AwsS3 {
    //     conn_params: AwsConnectionParams {
    //         profile_name: Some("ddl"),
    //         ..Default::default()
    //     },
    // };
    // let client = get_s3_client(s3);

    // let files = client.list_all_objects(
    //     "s3-an2-op-datalake-datatransfer-ha",
    //     "yhjung/atom_landing/20240601/",
    //     Some(".pgp"),
    // )?;

    // files.iter().for_each(|f| println!("{}", f));
    let conn_params = AwsConnectionParams {
        profile_name: Some("ddl".to_owned()),
        region: Some("ap-northeast-2".to_owned()),
        ..Default::default()
    };
    let s3 = AwsS3 { conn_params };

    let cred = get_credential(&s3)?;

    let region = &s3.conn_params.region.unwrap_or("ap-northeast-2".to_owned());

    let cloud_option = get_cloud_option(cred, region);
    let args = ScanArgsParquet {
        hive_options: Default::default(),
        cloud_options: cloud_option,
        ..Default::default()
    };
    let df = LazyFrame::scan_parquet(
        "s3://s3-an2-op-datalake-datatransfer-ha/yhjung/402_AIRSOLUTION_AIRPURIFIER_10_1/*/*.parquet",
        args,
    )?;
    println!("{}", df.limit(10).collect()?);
    Ok(())
}
