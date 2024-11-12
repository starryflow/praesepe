use std::io::Write;
use tso::{Bootstrap, Config, ExitSignal};

pub fn main() {
    let mut builder = env_logger::Builder::new();
    builder
        .format(|buf, record| {
            let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            writeln!(
                buf,
                "{} [{}] - {}",
                timestamp,
                record.level(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info)
        .write_style(env_logger::WriteStyle::Always)
        .init();

    let config = Config::default();

    let (exit_sender, exit_receiver) = tokio::sync::broadcast::channel(1);
    let mut exit_signal = ExitSignal::new(exit_receiver);

    let etcd_client =
        Bootstrap::create_etcd(&config, &config.etcd_server_urls, exit_signal.clone());

    let alloc = Bootstrap::start_server(config, etcd_client, exit_signal.clone()).unwrap();

    log::info!("alloc loop will begin...");

    std::thread::spawn(move || {
        let mut count = 0;
        loop {
            let ts = alloc.handle_request(1).unwrap();
            log::info!("alloc new ts: {}", ts);

            std::thread::sleep(std::time::Duration::from_secs(3));

            count += 1;
            if count > 1000 {
                break;
            }
        }

        // exit
        let _ = exit_sender.send(());
    });

    exit_signal.wait_exit();

    log::info!("exit")
}
