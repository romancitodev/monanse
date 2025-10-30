use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicI32, Ordering},
};

/// Agnostic implementation of a semaphore for synchronization between threads.
///
/// Consists of an Atomic Counter, a Condvar and a Mutex.
///
/// Clone isn't satisfied for [`Condvar`] and [`AtomicI32`]
/// So you must [`Arc::clone`] explicitly
pub struct Semaphore(AtomicI32, Condvar, Mutex<()>);

pub type SharedSemaphore = Arc<Semaphore>;

impl Semaphore {
    pub fn new(capacity: usize) -> Self {
        Self(
            AtomicI32::new(capacity as i32),
            Condvar::new(),
            Mutex::new(()),
        )
    }

    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::Release);
        self.1.notify_all();
    }

    pub fn decrement(&self) {
        let mut guard = self.2.lock().unwrap();
        while self.0.load(Ordering::Acquire) <= 0 {
            guard = self.1.wait(guard).unwrap();
        }
        self.0.fetch_sub(1, Ordering::Acquire);
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
        // let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        self.wait_on.extend(sems.iter().cloned());
        // self.inner = Arc::new(inner);
        self
    }

    /// Adds multiple borrowed semaphores to wait on before executing
    pub fn wait_on_many_borrowed(mut self, sems: &[&SharedSemaphore]) -> Self {
        // let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        self.wait_on.extend(sems.iter().cloned().cloned());
        // self.inner = Arc::new(inner);
        self
    }

    /// Adds a semaphore to release after executing
    pub fn release_on(mut self, sem: &SharedSemaphore) -> Self {
        // let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        self.release_on.push(sem.clone());
        // self.inner = Arc::new(inner);
        self
    }

    /// Adds multiple semaphores to release after executing
    pub fn release_on_many(mut self, sems: &[SharedSemaphore]) -> Self {
        // let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        self.release_on.extend(sems.iter().cloned());
        // self.inner = Arc::new(inner);
        self
    }

    /// Adds multiple borrowed semaphores to release after executing
    pub fn release_on_many_borrowed(mut self, sems: &[&SharedSemaphore]) -> Self {
        // let mut inner = Arc::try_unwrap(self.inner).unwrap_or_else(|arc| (*arc).clone());
        self.release_on.extend(sems.iter().cloned().cloned());
        // self.inner = Arc::new(inner);
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
        for s in &self.wait_on {
            s.decrement();
        }
    }

    fn release(&self) {
        for s in &self.release_on {
            s.increment();
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
