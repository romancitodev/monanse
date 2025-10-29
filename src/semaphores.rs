#![allow(dead_code, unused_imports)]
//! # Semaphore API
//!
//! A clean, ergonomic API for semaphore-based synchronization with minimal boilerplate.
//!
//! ## Key Features
//! - No manual `Arc` wrapping required - handled internally
//! - No `.clone()` calls needed in `seq!` macro
//! - Builder pattern for intuitive element construction
//! - Cheap cloning via internal `Arc` references
//!
//! ## Example
//! ```rust
//! let a = Semaphore::new(2);
//! let b = Semaphore::new(0);
//!
//! let process_a = Element::new("a")
//!     .wait_on(&a)
//!     .release_on(&b);
//!
//! let process_b = Element::new("b")
//!     .wait_on(&b)
//!     .wait_on(&b)
//!     .release_on(&a)
//!     .release_on(&a);
//!
//! // Reuse elements without explicit cloning
//! let sequence = seq![
//!     process_a, process_b, process_a, process_a, process_b, process_a
//! ];
//! sequence.run();
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

// Internal semaphore implementation
struct SemaphoreInner(AtomicUsize);

impl SemaphoreInner {
    fn new(initial: usize) -> Self {
        Self(AtomicUsize::new(initial))
    }

    fn acquire(&self) {
        loop {
            let val = self.0.load(Ordering::Acquire);
            if val > 0
                && self
                    .0
                    .compare_exchange(val, val - 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                break;
            }
            // busy wait
            std::hint::spin_loop();
        }
    }

    fn release(&self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

/// A semaphore for synchronization between threads.
/// Internally uses Arc, so cloning is cheap and doesn't require manual Arc wrapping.
#[derive(Clone)]
pub struct Semaphore(Arc<SemaphoreInner>);

impl Semaphore {
    /// Creates a new semaphore with the given initial count
    pub fn new(initial: usize) -> Self {
        Self(Arc::new(SemaphoreInner::new(initial)))
    }

    /// Acquires the semaphore, blocking until available
    pub fn acquire(&self) {
        self.0.acquire();
    }

    /// Releases the semaphore, incrementing the count
    pub fn release(&self) {
        self.0.release();
    }
}

/// A sequence of elements that can be executed concurrently
pub struct Sequence {
    elements: Vec<Process>,
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
            let element = element.clone();
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

pub trait Seq: Send + Sync {
    fn wait(&self); // Wait until every semaphore is available
    fn release(&self); // Release every semaphore
    fn eval(&self) {
        // Default implementation
        println!("{}", std::any::type_name::<Self>());
    }
}

/// An element in a sequence that waits on and releases semaphores.
/// Internally uses Arc, so cloning is cheap and allows reuse in sequences.
#[derive(Clone)]
pub struct Process {
    inner: Arc<ProcessInner>,
}

struct ProcessInner {
    name: String,
    wait_on: Vec<Semaphore>,
    release_on: Vec<Semaphore>,
}

impl Process {
    /// Creates a new element with the given name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(ProcessInner {
                name: name.into(),
                wait_on: Vec::new(),
                release_on: Vec::new(),
            }),
        }
    }

    /// Adds a semaphore to wait on before executing
    pub fn wait_on(mut self, sem: &Semaphore) -> Self {
        // We need to reconstruct the Arc to modify it
        let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        inner.wait_on.push(sem.clone());
        self.inner = Arc::new(inner);
        self
    }

    /// Adds multiple semaphores to wait on before executing
    pub fn wait_on_many(mut self, sems: &[Semaphore]) -> Self {
        let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        inner.wait_on.extend(sems.iter().cloned());
        self.inner = Arc::new(inner);
        self
    }

    /// Adds multiple borrowed semaphores to wait on before executing
    pub fn wait_on_many_borrowed(mut self, sems: &[&Semaphore]) -> Self {
        let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        inner.wait_on.extend(sems.iter().cloned().cloned());
        self.inner = Arc::new(inner);
        self
    }

    /// Adds a semaphore to release after executing
    pub fn release_on(mut self, sem: &Semaphore) -> Self {
        let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        inner.release_on.push(sem.clone());
        self.inner = Arc::new(inner);
        self
    }

    /// Adds multiple semaphores to release after executing
    pub fn release_on_many(mut self, sems: &[Semaphore]) -> Self {
        let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        inner.release_on.extend(sems.iter().cloned());
        self.inner = Arc::new(inner);
        self
    }

    /// Adds multiple borrowed semaphores to release after executing
    pub fn release_on_many_borrowed(mut self, sems: &[&Semaphore]) -> Self {
        let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        inner.release_on.extend(sems.iter().cloned().cloned());
        self.inner = Arc::new(inner);
        self
    }
}

impl ProcessInner {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            wait_on: self.wait_on.clone(),
            release_on: self.release_on.clone(),
        }
    }
}

impl Seq for Process {
    fn wait(&self) {
        for s in &self.inner.wait_on {
            s.acquire();
        }
    }

    fn release(&self) {
        for s in &self.inner.release_on {
            s.release();
        }
    }

    fn eval(&self) {
        println!("{}", self.inner.name);
    }
}

#[macro_export]
macro_rules! seq {
    ($($elem:expr),* $(,)?) => {
        $crate::semaphores::Sequence::new()$(.add($elem.clone()))*
    };
}
