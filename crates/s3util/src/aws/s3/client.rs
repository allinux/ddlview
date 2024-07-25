use aws_sdk_s3::config::ProvideCredentials;

use crate::aws::{util::client::BUILDER, AwsClient, AwsClients, AwsConfig, AwsConnectionParams};

#[derive(Debug)]
pub struct AwsS3<S: AsRef<str>> {
    //config: AwsConfig,
    pub conn_params: AwsConnectionParams<S>,
}

impl<S: AsRef<str>> AwsClient for AwsS3<S> {
    fn get_client(&self) -> AwsClients {
        BUILDER.block_on(async {
            //let profile_name = &self.conn_params.profile_name;
            let config = self.get_config(&self.conn_params).await;
            //let ro = config.credentials_provider().unwrap().provide_credentials().await;
            AwsClients::S3Client(aws_sdk_s3::client::Client::new(&config))
        })
    }

    fn get_credential(&self) -> Result<aws_sdk_s3::config::Credentials, anyhow::Error> {
        BUILDER.block_on(async {
            let config = self.get_config(&self.conn_params).await;
            config
                .credentials_provider()
                .unwrap()
                .provide_credentials()
                .await
                .map_err(|e| anyhow::anyhow!(e))
        })
    }
}
