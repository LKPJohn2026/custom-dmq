use dmq_stress::config::{parse_stress_args, parse_verify_args};
use dmq_stress::{audit_topic, run_stress};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    let result = match args[1].as_str() {
        "run" => {
            let config = parse_stress_args(&args).unwrap_or_else(|e| {
                eprintln!("{e}");
                print_usage();
                std::process::exit(1);
            });
            run_stress(config).await.map(|_| ())
        }
        "verify" => {
            let config = parse_verify_args(&args).unwrap_or_else(|e| {
                eprintln!("{e}");
                print_usage();
                std::process::exit(1);
            });
            audit_topic(config).await.map(|report| {
                if !report.ok() {
                    std::process::exit(1);
                }
            })
        }
        _ => {
            eprintln!("unknown subcommand: {}", args[1]);
            print_usage();
            std::process::exit(1);
        }
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn print_usage() {
    eprintln!(
        "Usage:
  dmq-stress run [--topic ID] [--rps N] [--duration 60s] [--warmup 10s]
                 [--workers N] [--payload-bytes N] [--run-id ID] [--ledger PATH]
  dmq-stress verify --run-id ID [--topic ID] [--ledger PATH] [--group ID]"
    );
}
