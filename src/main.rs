fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if let Err(err) = minio_rust::cmd::run_cli(args) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
