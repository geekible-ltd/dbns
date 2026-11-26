use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;

/// NDB database server.
#[derive(Parser)]
#[command(name = "ndb-server", version, about)]
struct Args {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "ndb.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match ndb_server::run(&args.config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fatal: {e}");
            ExitCode::FAILURE
        }
    }
}
