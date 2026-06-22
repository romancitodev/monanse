#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicI32, Ordering},
};

use parking_lot::{Condvar, Mutex};

pub(crate) struct InnerSemaphore(AtomicI32, Condvar, Mutex<()>);

#[derive(Clone)]
pub struct Semaphore(Arc<InnerSemaphore>);

impl Semaphore {
    pub fn new(capacity: i32) -> Self {
        Self(Arc::new(InnerSemaphore(
            AtomicI32::new(capacity),
            Condvar::new(),
            Mutex::new(()),
        )))
    }

    pub fn increment(&self) {
        self.increment_many(1);
    }

    /// Atomically increments the semaphore by a specific count
    pub fn increment_many(&self, count: i32) {
        self.0.0.fetch_add(count, Ordering::Relaxed);
        self.0.1.notify_all();
    }

    pub fn decrement(&self) {
        self.decrement_many(1);
    }

    /// Atomically decrements the semaphore by a specific count (all-or-nothing)
    /// If the semaphore doesn't have enough permits, it waits until it does
    pub fn decrement_many(&self, count: i32) {
        let mut guard = self.0.2.lock();
        loop {
            let val = self.0.0.load(Ordering::Relaxed);
            if val >= count {
                if self
                    .0
                    .0
                    .compare_exchange(val, val - count, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            } else {
                self.0.1.wait(&mut guard);
            }
        }
    }

    /// Acquires multiple permits atomically and returns a RAII guard
    /// The guard will automatically release the permits when dropped
    pub fn acquire(&self) -> SemaphoreGuard<'_> {
        self.decrement();
        SemaphoreGuard {
            semaphore: self,
            count: 1,
        }
    }

    /// Acquires multiple permits atomically and returns a RAII guard
    /// The guard will automatically release the permits when dropped
    pub fn acquire_many(&self, count: i32) -> SemaphoreGuard<'_> {
        self.decrement_many(count);
        SemaphoreGuard {
            semaphore: self,
            count,
        }
    }

    pub(crate) fn inner_ptr(&self) -> *const InnerSemaphore {
        Arc::as_ptr(&self.0)
    }
}

/// RAII guard that automatically releases semaphore permits when dropped
pub struct SemaphoreGuard<'a> {
    semaphore: &'a Semaphore,
    count: i32,
}

impl Drop for SemaphoreGuard<'_> {
    fn drop(&mut self) {
        self.semaphore.increment_many(self.count);
    }
}

#[derive(Clone)]
struct InnerProcess {
    pub name: String,
    pub wait_on: Vec<Semaphore>,
    pub release_on: Vec<Semaphore>,
}

#[derive(Clone)]
pub struct Process(Arc<InnerProcess>);

impl Process {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Arc::new(InnerProcess {
            name: name.into(),
            wait_on: vec![],
            release_on: vec![],
        }))
    }

    #[must_use]
    /// Adds a semaphore to wait on before executing
    pub fn wait_on(mut self, sem: &Semaphore) -> Self {
        // We need to mutate the inner process, but we only have an Arc.
        // During the building phase, it's safe to assume we can unwrap or clone if needed,
        // but typically builders work on owned data.
        // Since we are changing the design to Arc internally, the builder pattern
        // that takes `mut self` is slightly tricky if we already shared it.
        // However, usually builders are used before sharing.
        // We can use Arc::make_mut to get mutable access if we are the only owner.

        let inner = Arc::make_mut(&mut self.0);
        inner.wait_on.push(sem.clone());
        self
    }

    /// Adds multiple semaphores to wait on before executing
    pub fn wait_on_many(mut self, sems: &[Semaphore]) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.wait_on.extend(sems.iter().cloned());
        self
    }

    /// Adds multiple borrowed semaphores to wait on before executing
    pub fn wait_on_many_borrowed(mut self, sems: &[&Semaphore]) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.wait_on.extend(sems.iter().copied().cloned());
        self
    }

    /// Adds a semaphore to release after executing
    pub fn release_on(mut self, sem: &Semaphore) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.release_on.push(sem.clone());
        self
    }

    /// Adds multiple semaphores to release after executing
    pub fn release_on_many(mut self, sems: &[Semaphore]) -> Self {
        let inner = Arc::make_mut(&mut self.0);

        // Just add them; we can optimize the storage in the InnerProcess independently
        // Or we can optimize during insertion.
        // The previous complex logic was inside release_on_many which is a builder method.
        // It seems it was trying to deduplicate semaphores?
        // The user complained about complexity: "el metodo release_on_many antes era re sencillo y pasó a ser complicado?"
        // The complication was added in the previous turn to handle unique semaphores based on pointer identity
        // because we were using Arc pointers.

        // Let's go back to simple implementation.
        inner.release_on.extend(sems.iter().cloned());
        self
    }

    /// Adds multiple borrowed semaphores to release after executing
    pub fn release_on_many_borrowed(mut self, sems: &[&Semaphore]) -> Self {
        let inner = Arc::make_mut(&mut self.0);
        inner.release_on.extend(sems.iter().copied().cloned());
        self
    }
}

pub type SharedProcess = Process;

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
        let mut sem_counts: HashMap<*const InnerSemaphore, (i32, &Semaphore)> = HashMap::new();

        for sem in &self.0.wait_on {
            // Access through .0
            // We can provide a helper method to get the pointer to InnerSemaphore to avoid .0.0 ugliness
            let ptr = sem.inner_ptr();
            sem_counts
                .entry(ptr)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, sem));
        }

        // Acquire all permits atomically for each unique semaphore
        for (_, (count, sem)) in sem_counts {
            sem.decrement_many(count);
        }
    }

    fn release(&self) {
        // Group semaphores by identity and count how many times each appears
        let mut sem_counts: HashMap<*const InnerSemaphore, (i32, &Semaphore)> = HashMap::new();

        for sem in &self.0.release_on {
            // Access through .0
            let ptr = sem.inner_ptr();
            sem_counts
                .entry(ptr)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, sem));
        }

        // Release all permits atomically for each unique semaphore
        for (_, (count, sem)) in sem_counts {
            sem.increment_many(count);
        }
    }

    fn eval(&self) {
        println!("{}", self.0.name);
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
    pub fn add(mut self, element: Process) -> Self {
        self.elements.push(element);
        self
    }

    /// Adds multiple elements to the sequence
    pub fn add_many(mut self, elements: impl IntoIterator<Item = Process>) -> Self {
        self.elements.extend(elements);
        self
    }

    /// Runs all elements in the sequence concurrently
    pub fn run(&self) {
        let mut handles: Vec<std::thread::JoinHandle<()>> = vec![];
        for element in &self.elements {
            let element = element.clone(); // Just clone the Process (which is Arc internal)
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
