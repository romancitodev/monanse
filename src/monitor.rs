use crate::utils::*;
use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;

pub struct BufferMonitor {
    capacity: usize,
    buffer: Mutex<VecDeque<Msg>>,
    // conditions
    empty: Condvar,
    full: Condvar,
}

pub enum Msg {
    Data(usize),
    Finish,
}

impl BufferMonitor {
    pub fn with_capacity(capacity: usize) -> BufferMonitor {
        let buffer = Mutex::new(VecDeque::with_capacity(capacity));
        Self {
            capacity,
            buffer,
            empty: Condvar::new(),
            full: Condvar::new(),
        }
    }

    pub fn insert(&self, item: Msg) {
        let mut buffer = self.buffer.lock();
        if buffer.len() >= self.capacity {
            println!("Esperando a que haya un hueco.");
            wait(&self.empty, &mut buffer);
            println!("Listo, se liberó un espacio (por lo menos)");
        }

        println!("Colocando un elemento en el buffer.");
        buffer.push_back(item);
        signal(&self.full);
    }

    pub fn finish(&self) -> bool {
        let mut buffer = self.buffer.lock();
        if buffer.len() == 0 {
            println!("Esperando a que haya un item por lo menos");
            wait(&self.full, &mut buffer);
            println!("Listo, se puede extraer un item ya.");
        }

        match buffer.pop_front() {
            Some(Msg::Finish) => {
                signal(&self.empty);
                return true;
            }
            Some(Msg::Data(d)) => {
                println!("from the deque: {d}");
            }
            None => (),
        };
        signal(&self.empty);
        return false;
    }
}
