use std::{
    any::TypeId,
    collections::{HashMap, VecDeque},
};

use parking_lot::{Condvar, Mutex, MutexGuard};
pub struct MonitorBuilder<T> {
    data: T,
    conditions: Vec<(TypeId, Condvar)>,
}

pub struct Monitor<T> {
    data: Mutex<T>,
    conditions: HashMap<TypeId, Condvar>,
}

impl<T> MonitorBuilder<T> {
    pub fn new(data: T) -> Self {
        Self {
            data,
            conditions: Vec::new(),
        }
    }

    pub fn with_condition<K: ?Sized + 'static>(mut self) -> Self {
        self.conditions.push((TypeId::of::<K>(), Condvar::new()));
        self
    }

    pub fn build(self) -> Monitor<T> {
        Monitor::new(Mutex::new(self.data), self.conditions.into_iter().collect())
    }
}

impl<T> Monitor<T> {
    pub fn new(data: Mutex<T>, conditions: HashMap<TypeId, Condvar>) -> Self {
        Self { data, conditions }
    }

    // Simple access for operations that don't need conditions
    pub fn access<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.data.lock();
        f(&mut *guard)
    }

    // Most flexible: manual control over conditions
    pub fn with_lock<F, R>(&self, f: F) -> R
    where
        F: for<'a> FnOnce(MonitorGuard<'a, T>) -> R,
    {
        let guard = self.data.lock();
        f(MonitorGuard::new(guard, &self.conditions))
    }
}

// Helper struct that provides access to data and conditions
pub struct MonitorGuard<'a, T> {
    guard: MutexGuard<'a, T>,
    conditions: &'a HashMap<TypeId, Condvar>,
}

impl<'a, T> MonitorGuard<'a, T> {
    pub fn new(guard: MutexGuard<'a, T>, conditions: &'a HashMap<TypeId, Condvar>) -> Self {
        Self { guard, conditions }
    }

    // Wait while a predicate is true
    pub fn wait_while<K: ?Sized + 'static>(&mut self, predicate: impl Fn(&T) -> bool) {
        while predicate(&*self.guard) {
            self.conditions[&TypeId::of::<K>()].wait(&mut self.guard);
        }
    }

    // Single wait (rarely needed, but available)
    pub fn wait_on<K: ?Sized + 'static>(mut self) -> Self {
        self.conditions[&TypeId::of::<K>()].wait(&mut self.guard);
        self
    }

    // Notify one waiting thread
    pub fn notify<K: ?Sized + 'static>(&self) {
        self.conditions[&TypeId::of::<K>()].notify_one();
    }

    // Notify all waiting threads
    pub fn notify_all<K: ?Sized + 'static>(&self) {
        self.conditions[&TypeId::of::<K>()].notify_all();
    }

    pub fn data(&mut self) -> &T {
        &self.guard
    }

    // Access the underlying data
    pub fn data_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NotEmpty;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NotFull;

#[derive(Debug, Clone)]
pub struct Msg {
    pub content: String,
}

impl Msg {
    pub fn new(content: String) -> Self {
        Self { content }
    }
}

pub struct BoundedBuffer {
    capacity: usize,
    monitor: Monitor<VecDeque<Msg>>,
}

impl BoundedBuffer {
    pub fn new(capacity: usize) -> Self {
        let monitor = MonitorBuilder::new(VecDeque::new())
            .with_condition::<NotEmpty>()
            .with_condition::<NotFull>()
            .build();

        Self { capacity, monitor }
    }

    pub fn insert(&self, msg: Msg) {
        self.monitor.with_lock(|mut guard| {
            // Wait while buffer is full
            guard.wait_while::<NotFull>(|buffer| buffer.len() >= self.capacity);

            // Insert the message
            guard.data_mut().push_back(msg);

            // Notify that buffer is not empty
            guard.notify::<NotEmpty>();
        });
    }

    pub fn remove(&self) -> Msg {
        self.monitor.with_lock(|mut guard| {
            // Wait while buffer is empty
            guard.wait_while::<NotEmpty>(VecDeque::is_empty);

            // Remove the message
            let msg = guard.data_mut().pop_front().unwrap();

            // Notify that buffer is not full
            guard.notify::<NotFull>();

            msg
        })
    }

    pub fn try_insert(&self, msg: Msg) -> bool {
        self.monitor.with_lock(|mut guard| {
            if guard.data_mut().len() >= self.capacity {
                false // Buffer is full
            } else {
                guard.data_mut().push_back(msg);
                guard.notify::<NotEmpty>();
                true
            }
        })
    }

    pub fn try_remove(&self) -> Option<Msg> {
        self.monitor.with_lock(|mut guard| {
            if guard.data_mut().is_empty() {
                None
            } else {
                let msg = guard.data_mut().pop_front();
                guard.notify::<NotFull>();
                msg
            }
        })
    }

    pub fn len(&self) -> usize {
        self.monitor.access(|buffer| buffer.len())
    }

    pub fn is_empty(&self) -> bool {
        self.monitor.access(|buffer| buffer.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn simple_monitor() {
        let buffer = BoundedBuffer::new(3);

        buffer.insert(Msg {
            content: "Hello".to_string(),
        });
        buffer.insert(Msg {
            content: "World".to_string(),
        });

        assert_eq!(buffer.len(), 2);

        buffer.insert(Msg {
            content: "Message".to_string(),
        });

        assert_eq!(buffer.len(), 3);

        let _ = buffer.remove();

        let msg1 = buffer.remove();
        assert_eq!(msg1.content, "World");

        let msg2 = buffer.remove();
        assert_eq!(msg2.content, "Message");

        assert!(buffer.is_empty());
    }
}
