use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "fc-rs", about = "Fast block-order copy with dedup", version)]
pub struct CliArgs {
    pub source: Option<String>,
    pub destination: Option<String>,

    #[arg(long, default_value = "64")]
    pub buffer: usize,

    #[arg(long, default_value_t = 4)]
    pub threads: usize,

    #[arg(long, default_value_t = 4)]
    pub workers: usize,

    #[arg(long)]
    pub dry_run: bool,

    #[arg(long, short = 'v')]
    pub verbose: bool,

    #[arg(long)]
    pub no_verify: bool,

    #[arg(long)]
    pub log_file: Option<String>,

    #[arg(long)]
    pub no_dedup: bool,

    #[arg(long, default_value = "auto")]
    pub hash: String,

    #[arg(long)]
    pub force: bool,

    #[arg(long)]
    pub overwrite: bool,

    #[arg(long = "exclude", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    #[arg(long)]
    pub no_cache: bool,

    #[arg(long, default_value_t = 22)]
    pub ssh_port: u16,

    #[arg(long)]
    pub ssh_key: Option<String>,

    #[arg(long)]
    pub ssh_password: bool,

    #[arg(long = "ssh-src-port", default_value_t = 22)]
    pub src_port: u16,

    #[arg(long = "ssh-src-key")]
    pub src_key: Option<String>,

    #[arg(long = "ssh-src-password")]
    pub src_password: bool,

    #[arg(short = 'z', long)]
    pub compress: bool,

    #[arg(long)]
    pub check_update: bool,

    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub update: Option<String>,

}
