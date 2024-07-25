use std::{borrow::Cow, time::Duration};

use aws_config::{
    meta::region::RegionProviderChain,
    profile::ProfileFileRegionProvider,
    provider_config::ProviderConfig,
    retry::{RetryConfig, RetryMode},
    BehaviorVersion, Region, SdkConfig,
};
use aws_sdk_s3::{
    config::{Credentials, SharedCredentialsProvider},
    Client,
};

use s3::client::AwsS3;
use util::client::BUILDER;

pub mod s3;
pub mod util;

pub enum AwsClients {
    //GlueClient(aws_sdk_glue::client::Client),
    S3Client(aws_sdk_s3::client::Client),
    //AthenaClient(aws_sdk_athena::client::Client),
}

pub fn get_credential<S: AsRef<str>>(aws_s3: &AwsS3<S>) -> Result<aws_sdk_s3::config::Credentials, anyhow::Error> {
    BUILDER.block_on(async {
        let cred = aws_s3.get_credential().await?;
        Ok(cred)
    })
}

//impl AwsClients {
pub fn get_s3_client<S: AsRef<str>>(aws_s3: AwsS3<S>) -> Client {
    BUILDER.block_on(async {
        let aws_client = aws_s3.get_client().await;
        let AwsClients::S3Client(client) = aws_client;
        client
    })
}
//}

#[derive(Debug, Default, Clone, Copy)]
pub struct AwsConnectionParams<S: AsRef<str>> {
    pub profile_name: Option<S>,
    pub access_key_id: Option<S>,
    pub secret_access_key: Option<S>,
    pub region: Option<S>,
}

trait AwsConfig {
    async fn get_config<S: AsRef<str>>(&self, aws_connection_params: &AwsConnectionParams<S>) -> SdkConfig;
}

pub trait AwsClient {
    async fn get_client(&self) -> AwsClients;
    async fn get_credential(&self) -> Result<Credentials, anyhow::Error>;
}

impl<T: AwsClient> AwsConfig for T {
    async fn get_config<S: AsRef<str>>(&self, aws_connection_params: &AwsConnectionParams<S>) -> SdkConfig {
        let config_loader = match aws_connection_params {
            AwsConnectionParams {
                profile_name: Some(profile), ..
            } => {
                let profile_region_provider = ProfileFileRegionProvider::builder().profile_name(profile.as_ref()).build();

                let region = RegionProviderChain::first_try(profile_region_provider)
                    .or_else(Region::new("ap-northeast-2".to_owned()))
                    .region()
                    .await
                    .expect("Cannot parse region!");

                let provider = aws_config::profile::ProfileFileCredentialsProvider::builder()
                    .profile_name(profile.as_ref())
                    .configure(&ProviderConfig::default().with_region(Some(region)))
                    .build();
                aws_config::defaults(BehaviorVersion::latest()).credentials_provider(provider)
                //SharedCredentialsProvider::new(provider)
            }
            AwsConnectionParams {
                access_key_id: Some(access_key_id),
                secret_access_key: Some(secret_access_key),
                region: Some(region),
                ..
            } => {
                let cred = Credentials::new(access_key_id.as_ref(), secret_access_key.as_ref(), None, None, "");
                let provider = SharedCredentialsProvider::new(cred);
                aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(Cow::Owned(region.as_ref().to_string())))
                    .credentials_provider(provider)
            }
            AwsConnectionParams { region: Some(region), .. } => {
                aws_config::defaults(BehaviorVersion::latest()).region(Region::new(Cow::Owned(region.as_ref().to_string())))
            }
            _ => aws_config::defaults(BehaviorVersion::latest()).region(Region::new("ap-northeast-2".to_owned())),
        };
        config_loader
            .retry_config(
                RetryConfig::standard()
                    .with_max_attempts(20)
                    //.with_initial_backoff(Duration::from_millis(50))
                    .with_max_backoff(Duration::from_millis(500))
                    .with_retry_mode(RetryMode::Standard),
            )
            .load()
            .await
    }
}

#[derive(Debug)]
pub struct Parameter {
    pub runtime_date: Option<String>,
    pub max_results: Option<i32>,
    pub max_results_for_runs: Option<i32>,
}

impl Default for Parameter {
    fn default() -> Self {
        Self {
            runtime_date: Default::default(),
            max_results: Some(1000),
            max_results_for_runs: Some(1),
        }
    }
}

pub trait RuntimeInfo {
    fn run_info_per_job(&self, parameter: Parameter) -> Result<Vec<GlueOutputInfo>, anyhow::Error>;
    fn run_info_for_date(&self, parameter: Parameter) -> Result<Vec<GlueOutputInfo>, anyhow::Error>;
}

#[derive(Debug, Default, serde::Serialize)]
pub struct GlueOutputInfo {
    name: String,
    version: String,
    number_of_dpu: f64,
    last_time: String,
    is_use_in_year2024: Option<String>,
    execution_time: i32,
    cost: f64,
    description: String,
}
