use std::{error::Error, io, path::PathBuf};

use clap::{
    Arg, Args, Command as ClapCommand, CommandFactory, Error as ClapError, FromArgMatches, Parser,
    ValueEnum, error::ErrorKind,
};
use influxdb3_client::Client;
use influxdb3_types::http::{CreateTokenWithPermissionsResponse, PermissionRequest};
use owo_colors::OwoColorize;
use secrecy::Secret;
use url::Url;

pub(crate) async fn handle_token_creation_with_config(
    client: Client,
    config: CreateTokenConfig,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    match config.admin_config {
        Some(admin_config) => {
            if admin_config.name.is_some() {
                handle_named_admin_token_creation(client, admin_config).await
            } else {
                handle_admin_token_creation(client, admin_config).await
            }
        }
        None => match config.scoped_config {
            Some(scoped_config) => {
                handle_scoped_token_creation(client, scoped_config).await
            }
            None => Err(
                "cannot create token, error with parameters run `influxdb3 create token --help`".into(),
            ),
        }
    }
}

pub(crate) async fn handle_admin_token_creation(
    client: Client,
    config: CreateAdminTokenConfig,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    let json_body = if config.regenerate {
        println!("Are you sure you want to regenerate admin token? Enter 'yes' to confirm",);
        let mut confirmation = String::new();
        let _ = io::stdin().read_line(&mut confirmation);
        if confirmation.trim() == "yes" {
            client
                .api_v3_configure_regenerate_admin_token()
                .await?
                .expect("token creation to return full token info")
        } else {
            return Err("Cannot regenerate token without confirmation".into());
        }
    } else {
        client
            .api_v3_configure_create_admin_token()
            .await?
            .expect("token creation to return full token info")
    };
    Ok(json_body)
}

pub(crate) async fn handle_named_admin_token_creation(
    client: Client,
    config: CreateAdminTokenConfig,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    let json_body = client
        .api_v3_configure_create_named_admin_token(
            config.name.expect("token name to be present"),
            config.expiry.map(|expiry| expiry.as_secs()),
        )
        .await?
        .expect("token creation to return full token info");
    Ok(json_body)
}

pub(crate) async fn handle_scoped_token_creation(
    client: Client,
    config: CreateScopedTokenConfig,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    // Parse permissions from the CLI format
    let mut permissions = Vec::new();
    for perm_str in config.permissions {
        let parts: Vec<&str> = perm_str.split(':').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid permission format: {}. Expected format: resource_type:resource_name:action", perm_str).into());
        }

        let resource_type = parts[0].to_string();
        let resource_names = if parts[1] == "*" {
            vec!["*".to_string()]
        } else {
            vec![parts[1].to_string()]
        };
        let actions: Vec<String> = parts[2].split(',').map(|s| s.to_string()).collect();

        permissions.push(PermissionRequest {
            resource_type,
            resource_names,
            actions,
        });
    }

               let token_name = config.name.ok_or("Token name is required for scoped tokens")?;
           let json_body = client
               .api_v3_configure_create_scoped_token(
                   token_name,
                   permissions,
                   config.expiry.map(|expiry| expiry.as_secs()),
               )
               .await?
               .expect("token creation to return full token info");
           Ok(json_body)
}

#[derive(Debug, ValueEnum, Clone, Copy)]
pub enum TokenOutputFormat {
    Json,
    Text,
}

#[derive(Parser, Clone, Debug)]
pub struct InfluxDb3ServerConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        name = "host",
        long = "host",
        default_value = "http://127.0.0.1:8181",
        env = "INFLUXDB3_HOST_URL"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server to create permissions.
    /// This will be the admin token to create tokens with permissions
    #[clap(name = "token", long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(name = "tls-ca", long = "tls-ca")]
    pub ca_cert: Option<PathBuf>,
}

#[derive(Parser, Debug)]
pub struct CreateAdminTokenConfig {
    /// Operator token will be regenerated when this is set
    #[clap(name = "regenerate", long = "regenerate")]
    pub regenerate: bool,

    // for named admin and permission tokens this is mandatory but not for admin tokens
    /// Name of the token
    #[clap(long)]
    pub name: Option<String>,

    /// Expires in `duration`,
    ///   e.g 10d for 10 days
    ///       1y for 1 year
    #[clap(long)]
    pub expiry: Option<humantime::Duration>,

    #[clap(flatten)]
    pub host: InfluxDb3ServerConfig,

    /// Output format for token, supports just json or text
    #[clap(long)]
    pub format: Option<TokenOutputFormat>,
}

#[derive(Parser, Debug)]
pub struct CreateScopedTokenConfig {
    /// Name of the token
    #[clap(long)]
    pub name: Option<String>,

    /// Permissions in format "resource_type:resource_name:action"
    ///   e.g. db:test_db:read,write
    ///        db:*:read
    ///        token:*:read,write
    #[clap(long, value_delimiter = ',')]
    pub permissions: Vec<String>,

    /// Expires in `duration`,
    ///   e.g 10d for 10 days
    ///       1y for 1 year
    #[clap(long)]
    pub expiry: Option<humantime::Duration>,

    #[clap(flatten)]
    pub host: InfluxDb3ServerConfig,

    /// Output format for token, supports just json or text
    #[clap(long)]
    pub format: Option<TokenOutputFormat>,
}

impl CreateAdminTokenConfig {
    pub fn as_args() -> Vec<Arg> {
        let admin_config = Self::command();
        let args = admin_config.get_arguments();
        args.into_iter().map(|arg| arg.to_owned()).collect()
    }
}

impl CreateScopedTokenConfig {
    pub fn as_args() -> Vec<Arg> {
        let scoped_config = Self::command();
        let args = scoped_config.get_arguments();
        args.into_iter().map(|arg| arg.to_owned()).collect()
    }
}

// There are few traits manually implemented for CreateTokenConfig. The reason is,
//   `influxdb3 create token --permission` was implemented as subcommands. With clap it is not
//   possible to have multiple `--permission` when it is implemented as a subcommand. In order to
//   maintain backwards compatibility `CreateTokenConfig` is implemented with roughly the shape of
//   an enum. But it is wired manually into clap's lifecycle by implementing the traits,
//     - `CommandFactory`, this allows us to dynamically switch the "expected" command.
//         For example,
//           - when triggering `--help` the command sent back is exactly the same as what was
//           before (using subcommands). The help messages are overridden so that redundant
//           switches are removed in global "usage" section.
//           - when triggered without `--help` switch then it has `--admin` as a subcommand and the
//           non admin config is included directly on the CreateTokenConfig. This is key, as this
//           enables `--permission` to be set multiple times.
//     - `FromArgMatches`, this allows us to check if it's for `--admin` and populate the right
//       variant. This is a handle for getting all the matches (based on command generated) that we
//       could use to initialise `CreateTokenConfig`
#[derive(Debug)]
pub struct CreateTokenConfig {
    pub admin_config: Option<CreateAdminTokenConfig>,
    pub scoped_config: Option<CreateScopedTokenConfig>,
}

impl CreateTokenConfig {
    pub fn get_connection_settings(&self) -> Result<&InfluxDb3ServerConfig, &'static str> {
        match (&self.admin_config, &self.scoped_config) {
            (Some(admin_config), _) => Ok(&admin_config.host),
            (_, Some(scoped_config)) => Ok(&scoped_config.host),
            _ => Err("cannot find server config"),
        }
    }

    pub fn get_output_format(&self) -> Option<&TokenOutputFormat> {
        match (&self.admin_config, &self.scoped_config) {
            (Some(admin_config), _) => admin_config.format.as_ref(),
            (_, Some(scoped_config)) => scoped_config.format.as_ref(),
            _ => None,
        }
    }
}

impl FromArgMatches for CreateTokenConfig {
    fn from_arg_matches(matches: &clap::ArgMatches) -> Result<Self, clap::Error> {
        // Check if we have admin subcommand
        if let Some(admin_subcmd_matches) = matches.subcommand_matches("--admin") {
            let name = admin_subcmd_matches.get_one::<String>("name");
            let regenerate = admin_subcmd_matches
                .get_one::<bool>("regenerate")
                .cloned()
                .unwrap_or_default();

            if name.is_some() && regenerate {
                return Err(ClapError::raw(
                    ErrorKind::ArgumentConflict,
                    "--regenerate cannot be used with --name, --regenerate only applies for operator token".yellow(),
                ));
            }

            return Ok(Self {
                admin_config: Some(CreateAdminTokenConfig::from_arg_matches(
                    admin_subcmd_matches,
                )?),
                scoped_config: None,
            });
        }

        // Check if we have scoped token parameters (both name and permissions must be present)
        if matches.contains_id("name") && matches.contains_id("permissions") {
            return Ok(Self {
                admin_config: None,
                scoped_config: Some(CreateScopedTokenConfig::from_arg_matches(matches)?),
            });
        }

        // If we have --name but no --permissions, it's a named admin token
        if matches.contains_id("name") && !matches.contains_id("permissions") {
            // This is a named admin token, not a scoped token
            return Ok(Self {
                admin_config: Some(CreateAdminTokenConfig::from_arg_matches(matches)?),
                scoped_config: None,
            });
        }

        Err(ClapError::raw(
            ErrorKind::MissingRequiredArgument,
            "Either --admin or --name must be specified".yellow(),
        ))
    }

    fn update_from_arg_matches(&mut self, matches: &clap::ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

impl Args for CreateTokenConfig {
    // we're not flattening so these can just return command()
    fn augment_args(_cmd: clap::Command) -> clap::Command {
        Self::command()
    }

    fn augment_args_for_update(_cmd: clap::Command) -> clap::Command {
        Self::command()
    }
}

impl CommandFactory for CreateTokenConfig {
    fn command() -> clap::Command {
        let admin_sub_cmd =
            ClapCommand::new("--admin").override_usage("influxdb3 create token --admin [OPTIONS]");
        let all_args = CreateAdminTokenConfig::as_args();
        let admin_sub_cmd = admin_sub_cmd.args(all_args);

        // Add scoped token arguments only to the main command, not as required
        let scoped_args = CreateScopedTokenConfig::as_args();
        
        ClapCommand::new("token")
            .subcommand(admin_sub_cmd)
            .args(scoped_args)
            .arg_required_else_help(false) // Don't require any args by default
    }

    fn command_for_update() -> clap::Command {
        Self::command()
    }
}
