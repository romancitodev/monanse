#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicI32, Ordering},
};

/// Agnostic implementation of a semaphore for synchronization between threads.
///
/// Consists of an Atomic Counter, a Condvar and a Mutex.
///
/// Clone isn't satisfied for [`Condvar`] and [`AtomicI32`].
/// So you must [`Arc::clone`] explicitly
pub struct Semaphore(AtomicI32, Condvar, Mutex<()>);

pub type SharedSemaphore = Arc<Semaphore>;

impl Semaphore {
    pub fn new(capacity: i32) -> Self {
        Self(AtomicI32::new(capacity), Condvar::new(), Mutex::new(()))
    }

    /// Atomically increments the semaphore by a specific count
    pub fn increment_by(&self, count: i32) {
        self.0.fetch_add(count, Ordering::Relaxed);
        self.1.notify_all();
    }

    /// Atomically decrements the semaphore by a specific count (all-or-nothing)
    /// If the semaphore doesn't have enough permits, it waits until it does
    pub fn decrement_by(&self, count: i32) {
        let mut guard = self.2.lock().unwrap();
        loop {
            let val = self.0.load(Ordering::Relaxed);
            if val >= count {
                if self
                    .0
                    .compare_exchange(val, val - count, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            } else {
                guard = self.1.wait(guard).unwrap();
            }
        }
    }

    /// Acquires multiple permits atomically and returns a RAII guard
    /// The guard will automatically release the permits when dropped
    pub fn acquire(&self, count: i32) -> SemaphoreGuard<'_> {
        self.decrement_by(count);
        SemaphoreGuard {
            semaphore: self,
            count,
        }
    }
}

/// RAII guard that automatically releases semaphore permits when dropped
pub struct SemaphoreGuard<'a> {
    semaphore: &'a Semaphore,
    count: i32,
}

impl<'a> Drop for SemaphoreGuard<'a> {
    fn drop(&mut self) {
        self.semaphore.increment_by(self.count);
    }
}

#[derive(Clone)]
pub struct Process {
    pub name: String,
    pub wait_on: Vec<SharedSemaphore>,
    pub release_on: Vec<SharedSemaphore>,
}

impl Process {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            wait_on: vec![],
            release_on: vec![],
        }
    }

    /// Adds a semaphore to wait on before executing
    pub fn wait_on(mut self, sem: &SharedSemaphore) -> Self {
        self.wait_on.push(Arc::clone(sem));
        self
    }

    /// Adds multiple semaphores to wait on before executing
    pub fn wait_on_many(mut self, sems: &[SharedSemaphore]) -> Self {
        self.wait_on.extend(sems.iter().cloned());
        self
    }

    /// Adds multiple borrowed semaphores to wait on before executing
    pub fn wait_on_many_borrowed(mut self, sems: &[&SharedSemaphore]) -> Self {
        self.wait_on.extend(sems.iter().cloned().cloned());
        self
    }

    /// Adds a semaphore to release after executing
    pub fn release_on(mut self, sem: &SharedSemaphore) -> Self {
        self.release_on.push(sem.clone());
        self
    }

    /// Adds multiple semaphores to release after executing
    pub fn release_on_many(mut self, sems: &[SharedSemaphore]) -> Self {
        self.release_on.extend(sems.iter().cloned());
        self
    }

    /// Adds multiple borrowed semaphores to release after executing
    pub fn release_on_many_borrowed(mut self, sems: &[&SharedSemaphore]) -> Self {
        self.release_on.extend(sems.iter().cloned().cloned());
        self
    }
}

pub type SharedProcess = Arc<Process>;

pub trait Seq: Send + Sync {
    fn wait(&self); // Wait until every semaphore is available
    fn release(&self); // Release every semaphore
    fn eval(&self) {
        // Default implementation
        println!("{}", std::any::type_name::<Self>());
    }
}

impl Seq for Process {
    fn wait(&self) {
        // Group semaphores by identity and count how many times each appears
        let mut sem_counts: HashMap<*const Semaphore, (i32, &SharedSemaphore)> = HashMap::new();

        for sem in &self.wait_on {
            let ptr = Arc::as_ptr(sem);
            sem_counts
                .entry(ptr)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, sem));
        }

        // Acquire all permits atomically for each unique semaphore
        for (_, (count, sem)) in sem_counts {
            sem.decrement_by(count);
        }
    }

    fn release(&self) {
        // Group semaphores by identity and count how many times each appears
        let mut sem_counts: HashMap<*const Semaphore, (i32, &SharedSemaphore)> = HashMap::new();

        for sem in &self.release_on {
            let ptr = Arc::as_ptr(sem);
            sem_counts
                .entry(ptr)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, sem));
        }

        // Release all permits atomically for each unique semaphore
        for (_, (count, sem)) in sem_counts {
            sem.increment_by(count);
        }
    }

    fn eval(&self) {
        println!("{}", self.name);
    }
}

pub struct Sequence {
    elements: Vec<SharedProcess>,
}

impl Sequence {
    /// Creates a new empty sequence
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Adds an element to the sequence
    pub fn add(mut self, element: Arc<Process>) -> Self {
        self.elements.push(element);
        self
    }

    /// Adds multiple elements to the sequence
    pub fn add_many(mut self, elements: impl IntoIterator<Item = Arc<Process>>) -> Self {
        self.elements.extend(elements);
        self
    }

    /// Runs all elements in the sequence concurrently
    pub fn run(&self) {
        let mut handles: Vec<std::thread::JoinHandle<()>> = vec![];
        for element in &self.elements {
            let element = Arc::clone(&element);
            let handle = std::thread::spawn(move || {
                element.wait();
                element.eval();
                element.release();
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl Default for Sequence {
    fn default() -> Self {
        Self::new()
    }
}

#[macro_export]
macro_rules! seq {
    ($($elem:expr),* $(,)?) => {
        $crate::semaphores::Sequence::new()$(.add($elem.clone()))*
    };
}
