/// A small blocking semaphore for limiting access to a shared resource.
///
/// A semaphore starts with a fixed number of permits. Calling [`acquire`] waits
/// until a permit is available and returns a [`SemaphorePermit`]. The permit is
/// returned to the semaphore when the guard is dropped.
///
/// This semaphore blocks the current OS thread while waiting, so it is suitable
/// for synchronous code or for work running on a blocking thread pool. Do not
/// call [`acquire`] directly from async tasks running on an async executor's
/// worker threads.
///
/// The implementation does not guarantee fairness between waiting threads.
///
/// [`acquire`]: Self::acquire
///
/// # Examples
///
/// Limit a section of work to one concurrent caller:
///
/// ```
/// use std::sync::Arc;
///
/// use util::sync::semaphore::Semaphore;
///
/// let semaphore = Arc::new(Semaphore::new(1));
///
/// let first = semaphore.acquire();
/// assert_eq!(semaphore.available_permits(), 0);
///
/// drop(first);
/// assert_eq!(semaphore.available_permits(), 1);
/// ```
///
/// Try to acquire a permit without blocking:
///
/// ```
/// use util::sync::semaphore::Semaphore;
///
/// let semaphore = Semaphore::new(1);
/// let permit = semaphore.try_acquire().expect("permit should be available");
///
/// assert!(semaphore.try_acquire().is_none());
///
/// drop(permit);
/// assert!(semaphore.try_acquire().is_some());
/// ```
pub struct Semaphore {
    permits: std::sync::Mutex<usize>,
    condvar: std::sync::Condvar,
}

impl Semaphore {
    /// Creates a semaphore with `permits` initially available permits.
    ///
    /// # Panics
    ///
    /// Since there is no `add_permits` or similar API, a semaphore created with
    /// zero permits would effectively be permanently closed. To prevent this,
    /// the function panics when attempting to create a semaphore with zero
    /// available permits.
    pub fn new(permits: usize) -> Self {
        assert!(permits > 0);
        Self {
            permits: std::sync::Mutex::new(permits),
            condvar: std::sync::Condvar::new(),
        }
    }

    /// Acquires one [SemaphorePermit], blocking the current thread until one is
    /// available.
    pub fn acquire(&self) -> SemaphorePermit<'_> {
        let mut permits = self.permits.lock().unwrap();
        while *permits == 0 {
            permits = self.condvar.wait(permits).unwrap();
        }
        *permits -= 1;
        SemaphorePermit {
            permits: &self.permits,
            condvar: &self.condvar,
        }
    }

    /// Attempts to acquire one [SemaphorePermit] without blocking.
    ///
    /// Returns [`Some`] guard when a permit was available, or [`None`] if the
    /// semaphore currently has no available permits.
    pub fn try_acquire(&self) -> Option<SemaphorePermit<'_>> {
        let mut permits = self.permits.lock().unwrap();
        if *permits == 0 {
            return None;
        }
        *permits -= 1;
        Some(SemaphorePermit {
            permits: &self.permits,
            condvar: &self.condvar,
        })
    }

    /// Returns the number of permits currently available.
    ///
    /// This is a point-in-time snapshot. In concurrent code, another thread may
    /// acquire or release a permit immediately after this method returns.
    pub fn available_permits(&self) -> usize {
        *self.permits.lock().unwrap()
    }
}

/// A semaphore permit acquired through [`Semaphore::acquire`] or
/// [`Semaphore::try_acquire`].
pub struct SemaphorePermit<'a> {
    permits: &'a std::sync::Mutex<usize>,
    condvar: &'a std::sync::Condvar,
}

impl<'a> Drop for SemaphorePermit<'a> {
    fn drop(&mut self) {
        {
            let mut permits = self.permits.lock().unwrap();
            *permits += 1;
        }
        self.condvar.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    use super::Semaphore;

    #[test]
    fn acquire_consumes_permit_until_dropped() {
        let semaphore = Semaphore::new(1);

        let permit = semaphore.acquire();

        assert_eq!(semaphore.available_permits(), 0);
        drop(permit);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn multiple_initial_permits() {
        let semaphore = Semaphore::new(2);

        let first = semaphore.acquire();
        let second = semaphore.acquire();

        assert_eq!(semaphore.available_permits(), 0);
        drop(first);
        assert_eq!(semaphore.available_permits(), 1);
        drop(second);
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[test]
    fn repeated_acquire_and_drop_does_not_grow_capacity() {
        let semaphore = Semaphore::new(1);

        for _ in 0..3 {
            drop(semaphore.acquire());
        }

        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn acquire_blocks_until_permit_is_released() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.acquire();
        let (sender, receiver) = mpsc::channel();

        let thread = {
            let semaphore = semaphore.clone();
            std::thread::spawn(move || {
                let _permit = semaphore.acquire();
                sender.send(()).unwrap();
            })
        };

        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        drop(permit);

        receiver
            .recv()
            .expect("permit should be acquired after release");
        thread.join().unwrap();
    }
}
