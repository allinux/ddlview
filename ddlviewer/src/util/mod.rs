use aws_sdk_s3::config::{Credentials, ProvideCredentials};
use polars::io::cloud::{AmazonS3ConfigKey, CloudOptions};
use s3util::aws::AwsConnectionParams;

pub fn get_cloud_option<P: AsRef<str>>(cred: Credentials, region: P) -> Option<CloudOptions> {

    let o = CloudOptions::default().with_aws([
        (AmazonS3ConfigKey::DefaultRegion, "ap-northeast-2".to_string()),
        (AmazonS3ConfigKey::Region, region.as_ref().to_owned()),
        //(AmazonS3ConfigKey::Bucket, "s3-an2-op-datalake-datatransfer-ha".to_string()),
        (AmazonS3ConfigKey::AccessKeyId, cred.access_key_id().to_owned()),
        (AmazonS3ConfigKey::SecretAccessKey, cred.secret_access_key().to_owned()),
    ]);
    Some(o)
}
