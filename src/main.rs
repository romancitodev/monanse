use monitor::*;
use std::sync::Arc;

use crate::{
    examples::sequence,
    semaphores::{Process, Semaphore},
};

mod examples;
mod monitor;
mod semaphores;
mod utils;

pub fn monitors() {
    let monitor = Arc::new(BufferMonitor::with_capacity(3));

    let other = std::thread::spawn({
        let monitor = Arc::clone(&monitor);
        move || {
            while !monitor.finish() {}
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
    // monitors();
    // semaphores();
    // complex_sequence();
    sequence();
}

fn complex_sequence() {
    let a = Arc::new(Semaphore::new(2));
    let bc = Arc::new(Semaphore::new(0));
    let process_a = Arc::new(
        Process::new("a")
            .wait_on_many_borrowed(&[&a, &a])
            .release_on_many_borrowed(&[&bc, &bc]),
    );
    let process_b = Arc::new(Process::new("b").wait_on(&bc).release_on(&a));
    let process_c = Arc::new(Process::new("c").wait_on(&bc).release_on(&a));

    let sequence = seq![
        process_b, process_b, process_a, process_b, process_c, process_a
    ];

    sequence.run();
}

fn semaphores() {
    let a = Arc::new(Semaphore::new(2));
    let b = Arc::new(Semaphore::new(0));
    let process_a = Arc::new(Process::new("a").wait_on(&a).release_on(&b));
    let process_b = Arc::new(
        Process::new("b")
            .wait_on_many_borrowed(&[&b, &b])
            .release_on_many_borrowed(&[&a, &a]),
    );

    let sequence = seq![
        process_a, process_b, process_a, process_a, process_a, process_b
    ];
    sequence.run();
}
