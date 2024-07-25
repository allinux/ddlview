use clap::{Parser, Subcommand};
use ddlviewer::{command, util::get_cloud_option};
use s3util::aws::{get_credential, s3::client::AwsS3, AwsConnectionParams};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// profile name
    #[arg(short, long)]
    profile: Option<String>,

    /// aws_access_key_id
    #[arg(short = 'i', long = "id")]
    aws_access_key_id: Option<String>,

    /// aws_secret_access_key
    #[arg(short = 'k', long = "secret")]
    aws_secret_access_key: Option<String>,

    #[arg(short = 'r', long = "region", default_value = "ap-northeast-2")]
    region: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Schema(command::schema::SchemaArgs),
    Head(command::head::SchemaArgs),
    Sql(command::sql::SchemaArgs),
}

fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let args = Args::parse();
    log::info!("{:?}", args);

    let cloud_option = match args {
        Args {
            profile: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            ..
        } => None,
        _ => {
            let s3 = AwsS3 {
                conn_params: AwsConnectionParams {
                    profile_name: args.profile,
                    region: args.region,
                    access_key_id: args.aws_access_key_id,
                    secret_access_key: args.aws_secret_access_key,
                },
            };
            let cred = get_credential(&s3)?;
            get_cloud_option(cred, "ap-northeast-2")
        }
    };
    match args.command {
        Commands::Schema(args) => command::schema::execute(args, cloud_option)?,
        Commands::Head(args) => match command::head::execute(args, cloud_option) {
            Ok(_) => println!("Done."),
            Err(e) => println!("{}", e),
        },
        Commands::Sql(args) => command::sql::execute(args, cloud_option)?,
    };
    Ok(())
}
