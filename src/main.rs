use monitor::{BoundedBuffer, Msg};
use std::sync::Arc;

mod buffered_monitor;
mod monitor;
mod semaphores;
mod utils;

async fn collect_data(monitor: Arc<BoundedBuffer>) {
    loop {
        match monitor.try_remove() {
            Some(Msg { content }) if content == "Finish" => {
                println!("Finish");
                break;
            }
            Some(item) => println!("{item:?}"),
            None => {
                tokio::task::yield_now().await;
            }
        }
    }
}

// I used async because of the contract with tokio
#[allow(clippy::unused_async)]
async fn push_data(monitor: Arc<BoundedBuffer>) {
    (0..10).for_each(|i| {
        monitor.insert(Msg::new(format!("Data {i}")));
        println!("Inserted Data {i}");
    });
    monitor.insert(Msg::new("Finish".to_owned()));
}

#[tokio::main]
async fn main() {
    let monitor = Arc::new(BoundedBuffer::new(3));

    let other = tokio::spawn(collect_data(Arc::clone(&monitor)));

    let thread = tokio::spawn(push_data(Arc::clone(&monitor)));

    let _ = tokio::join!(other, thread);

    println!("Good bye my fren");
}
