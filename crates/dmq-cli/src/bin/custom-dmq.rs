#[tokio::main]
async fn main() {
    dmq_cli::runner::run(std::env::args().collect()).await;
}
