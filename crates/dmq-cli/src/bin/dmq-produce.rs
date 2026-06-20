#[tokio::main]
async fn main() {
    let mut args: Vec<String> = std::env::args().collect();
    args.insert(1, "produce".into());
    dmq_cli::runner::run(args).await;
}
