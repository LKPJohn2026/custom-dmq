#[tokio::main]
async fn main() {
    let mut args: Vec<String> = std::env::args().collect();
    if args.len() == 1 {
        args.push("server".into());
    } else {
        args.insert(1, "server".into());
    }
    dmq_cli::runner::run(args).await;
}
