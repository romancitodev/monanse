use monitor::*;
use std::sync::Arc;

use crate::semaphores::{Process, Semaphore};

mod monitor;
mod semaphores;
mod utils;

pub fn monitors() {
    let monitor = Arc::new(BufferMonitor::with_capacity(3));

    let other = std::thread::spawn({
        let monitor = Arc::clone(&monitor);
        move || {
            while !monitor.consume() {}
        }
    });

    let thread = std::thread::spawn({
        let monitor = Arc::clone(&monitor);
        move || {
            (0..10).for_each(|i| monitor.insert(Msg::Data(i)));
            monitor.insert(Msg::Finish);
        }
    });

    _ = thread.join();
    _ = other.join();

    println!("Good bye my fren");
}

fn main() {
    semaphores();
}

// Use case
fn semaphores() {
    let a = Semaphore::new(2);
    let b = Semaphore::new(0);
    let process_a = Process::new("a").wait_on(&a).release_on(&b);
    let process_b = Process::new("b")
        .wait_on_many_borrowed(&[&b, &b])
        .release_on_many_borrowed(&[&a, &a]);

    let sequence = seq![
        process_b, process_b, process_a, process_a, process_a, process_a
    ];
    sequence.run();
}
