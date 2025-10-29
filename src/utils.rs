use parking_lot::{Condvar, MutexGuard};

pub fn wait<T>(lock: &Condvar, guard: &mut MutexGuard<T>) {
    lock.wait(guard);
}

pub fn signal(lock: &Condvar) -> bool {
    lock.notify_one()
}
