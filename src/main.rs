// iedb/src/main.rs
use clap::Parser;

#[derive(Parser)]
#[command(name = "iedb")]
struct Cli {
    #[arg(long, default_value = "mix")]
    mode: String,

    #[arg(long, default_value = "iedb.toml")]
    config: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.mode.as_str() {
        #[cfg(feature = "agent")]
        "agent" => {
            tracing::info!("Starting in agent mode");
        }

        #[cfg(feature = "server")]
        "server" => {
            tracing::info!("Starting in server mode");
        }

        #[cfg(all(feature = "agent", feature = "server"))]
        "mix" => {
            tracing::info!("Starting in mix mode");
        }

        _ => {
            eprintln!("Unsupported mode: {}. Available modes depend on compiled features.", cli.mode);
            std::process::exit(1);
        }
    }

    Ok(())
}
