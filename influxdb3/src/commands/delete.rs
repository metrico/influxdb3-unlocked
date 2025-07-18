use super::common::InfluxDb3Config;
use influxdb3_client::Client;
use influxdb3_types::http::HardDeletionTime;
use secrecy::ExposeSecret;
use secrecy::Secret;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use url::Url;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        match &self.cmd {
            SubCommand::Database(DatabaseConfig {
                host_url,
                auth_token,
                ca_cert,
                ..
            })
            | SubCommand::LastCache(LastCacheConfig {
                ca_cert,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::DistinctCache(DistinctCacheConfig {
                ca_cert,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::Table(TableConfig {
                ca_cert,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::Trigger(TriggerConfig {
                ca_cert,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::Token(TokenConfig {
                ca_cert,
                host_url,
                auth_token,
                ..
            }) => {
                let mut client = Client::new(host_url.clone(), ca_cert.clone())?;
                if let Some(token) = &auth_token {
                    client = client.with_auth_token(token.expose_secret());
                }
                Ok(client)
            }
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Delete a database
    Database(DatabaseConfig),
    /// Delete a last value cache
    #[clap(name = "last_cache")]
    LastCache(LastCacheConfig),
    /// Delete a distinct value cache
    #[clap(name = "distinct_cache")]
    DistinctCache(DistinctCacheConfig),
    /// Delete a table in a database
    Table(TableConfig),
    /// Delete a trigger
    Trigger(TriggerConfig),
    /// Delete a token
    Token(TokenConfig),
}

#[derive(Debug, clap::Args)]
pub struct DatabaseConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,

    /// The name of the database to be deleted
    #[clap(env = "INFLUXDB3_DATABASE_NAME", required = true)]
    pub database_name: String,

    /// When to perform hard deletion (never/now/default/timestamp).
    /// Examples: 'never', 'now', 'default', '2024-06-18T10:30:00Z'
    #[clap(long = "hard-delete", value_name = "WHEN")]
    pub hard_delete: Option<String>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct LastCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table under which the cache is being deleted
    #[clap(short = 't', long = "table")]
    table: String,

    /// The name of the cache being deleted
    #[clap(required = true)]
    cache_name: String,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct DistinctCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table under which the cache is being deleted
    #[clap(short = 't', long = "table")]
    table: String,

    /// The name of the cache being deleted
    #[clap(required = true)]
    cache_name: String,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct TableConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    #[clap(required = true)]
    /// The name of the table to be deleted
    table_name: String,

    /// When to perform hard deletion (never/now/default/timestamp).
    /// Examples: 'never', 'now', 'default', '2024-06-18T10:30:00Z'
    #[clap(long = "hard-delete", value_name = "WHEN")]
    hard_delete: Option<String>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

#[derive(Debug, clap::Parser)]
pub struct TriggerConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Force deletion even if trigger is active
    #[clap(long)]
    force: bool,

    /// Name of trigger to delete
    #[clap(required = true)]
    trigger_name: String,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct TokenConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,

    /// The name of the token to be deleted
    #[clap(long = "token-name")]
    pub token_name: String,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

fn parse_hard_delete_time(value: Option<String>) -> Option<HardDeletionTime> {
    match value {
        None => None,
        Some(s) => match s.to_lowercase().as_str() {
            "never" => Some(HardDeletionTime::Never),
            "now" => Some(HardDeletionTime::Now),
            "default" => Some(HardDeletionTime::Default),
            _ => Some(HardDeletionTime::Timestamp(s)),
        },
    }
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let client = config.get_client()?;
    match config.cmd {
        SubCommand::Database(DatabaseConfig {
            database_name,
            hard_delete,
            ..
        }) => {
            println!("Are you sure you want to delete {database_name:?}? Enter 'yes' to confirm");
            let mut confirmation = String::new();
            let _ = io::stdin().read_line(&mut confirmation);
            if confirmation.trim() != "yes" {
                println!("Cannot delete database without confirmation");
            } else {
                let hard_delete_time = parse_hard_delete_time(hard_delete);

                if hard_delete_time.is_some() {
                    client
                        .api_v3_configure_db_delete_with_hard_delete(
                            &database_name,
                            hard_delete_time,
                        )
                        .await?;
                } else {
                    client.api_v3_configure_db_delete(&database_name).await?;
                }

                println!("Database {:?} deleted successfully", &database_name);
            }
        }
        SubCommand::LastCache(LastCacheConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            table,
            cache_name,
            ..
        }) => {
            client
                .api_v3_configure_last_cache_delete(database_name, table, cache_name)
                .await?;

            println!("last cache deleted successfully");
        }
        SubCommand::DistinctCache(DistinctCacheConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            table,
            cache_name,
            ..
        }) => {
            client
                .api_v3_configure_distinct_cache_delete(database_name, table, cache_name)
                .await?;

            println!("distinct cache deleted successfully");
        }
        SubCommand::Table(TableConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            table_name,
            hard_delete,
            ..
        }) => {
            println!(
                "Are you sure you want to delete {:?}.{:?}? Enter 'yes' to confirm",
                database_name, &table_name,
            );
            let mut confirmation = String::new();
            let _ = io::stdin().read_line(&mut confirmation);
            if confirmation.trim() != "yes" {
                println!("Cannot delete table without confirmation");
            } else {
                let hard_delete_time = parse_hard_delete_time(hard_delete);

                if hard_delete_time.is_some() {
                    client
                        .api_v3_configure_table_delete_with_hard_delete(
                            &database_name,
                            &table_name,
                            hard_delete_time,
                        )
                        .await?;
                } else {
                    client
                        .api_v3_configure_table_delete(&database_name, &table_name)
                        .await?;
                }

                println!(
                    "Table {:?}.{:?} deleted successfully",
                    &database_name, &table_name
                );
            }
        }
        SubCommand::Trigger(TriggerConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            trigger_name,
            force,
            ..
        }) => {
            client
                .api_v3_configure_processing_engine_trigger_delete(
                    database_name,
                    &trigger_name,
                    force,
                )
                .await?;
            println!("Trigger {trigger_name} deleted successfully");
        }
        SubCommand::Token(TokenConfig { token_name, .. }) => {
            if token_name == "_admin" {
                println!(
                    "The operator token \"_admin\" is required and cannot be deleted. To regenerate an operator token, use: influxdb3 create token --admin --regenerate --token [TOKEN]"
                );
                return Ok(());
            }

            println!("Are you sure you want to delete {token_name:?}? Enter 'yes' to confirm");
            let mut confirmation = String::new();
            let _ = io::stdin().read_line(&mut confirmation);
            if confirmation.trim() != "yes" {
                println!("Cannot delete token without confirmation");
            } else {
                client.api_v3_configure_token_delete(&token_name).await?;
                println!("Token {token_name:?} deleted successfully");
            }
        }
    }
    Ok(())
}
