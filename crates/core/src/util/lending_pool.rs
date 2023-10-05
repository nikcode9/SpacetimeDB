//! like, a semaphore but with values. or something

// use std::collections::VecDeque;
// use std::future::Future;
// use std::mem::ManuallyDrop;
// use std::ops::{Deref, DerefMut};
// use std::pin::Pin;
// use std::sync::Arc;
// use std::task::{Context, Poll};

// use parking_lot::Mutex;
// use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::notify_once::{NotifiedOnce, NotifyOnce};

use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct LendingPool<T> {
    mu: Arc<tokio::sync::Mutex<Option<T>>>,
    closed_notify: Arc<NotifyOnce>,
}

impl<T> Clone for LendingPool<T> {
    fn clone(&self) -> Self {
        Self {
            mu: self.mu.clone(),
            closed_notify: self.closed_notify.clone(),
        }
    }
}

impl<T> Default for LendingPool<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> LendingPool<T> {
    pub fn new() -> Self {
        Self::from_iter(std::iter::empty())
    }

    pub fn request(&self) -> impl Future<Output = Result<LentResource<T>, PoolClosed>> {
        let mu = self.mu.clone();
        async move {
            let x = mu.lock_owned().await;
            Ok(LentResource {
                resource: tokio::sync::OwnedMutexGuard::try_map(x, Option::as_mut).map_err(|_| PoolClosed)?,
            })
        }
    }

    pub fn add(&self, resource: T) -> Result<(), PoolClosed> {
        self.add_multiple(std::iter::once(resource))
    }

    pub fn add_multiple<I: IntoIterator<Item = T>>(&self, resources: I) -> Result<(), PoolClosed> {
        *self.mu.try_lock().unwrap() = resources.into_iter().next();
        Ok(())
    }

    pub fn close(&self) -> Closed<'_> {
        self.closed_notify.notify();
        self.closed()
    }

    pub fn closed(&self) -> Closed<'_> {
        Closed {
            notified: self.closed_notify.notified(),
        }
    }
}

pub struct LentResource<T> {
    resource: tokio::sync::OwnedMappedMutexGuard<Option<T>, T>,
}

impl<T> FromIterator<T> for LendingPool<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self {
            mu: tokio::sync::Mutex::new(iter.into_iter().next()).into(),
            closed_notify: NotifyOnce::new().into(),
        }
    }
}

// pub struct LendingPool<T> {
//     sem: Arc<Semaphore>,
//     inner: Arc<LendingPoolInner<T>>,
// }

// impl<T> Clone for LendingPool<T> {
//     fn clone(&self) -> Self {
//         Self {
//             sem: self.sem.clone(),
//             inner: self.inner.clone(),
//         }
//     }
// }

// impl<T> Default for LendingPool<T> {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// struct LendingPoolInner<T> {
//     closed_notify: NotifyOnce,
//     vec: Mutex<PoolVec<T>>,
// }

// struct PoolVec<T> {
//     total_count: usize,
//     deque: Option<VecDeque<T>>,
// }

#[derive(Debug)]
pub struct PoolClosed;

// impl<T> LendingPool<T> {
//     pub fn new() -> Self {
//         Self::from_iter(std::iter::empty())
//     }

//     pub fn request(&self) -> impl Future<Output = Result<LentResource<T>, PoolClosed>> {
//         let acq = self.sem.clone().acquire_owned();
//         let pool_inner = self.inner.clone();
//         async move {
//             let permit = acq.await.map_err(|_| PoolClosed)?;
//             let resource = pool_inner
//                 .vec
//                 .lock()
//                 .deque
//                 .as_mut()
//                 .ok_or(PoolClosed)?
//                 .pop_front()
//                 .ok_or(PoolClosed)?;
//             Ok(LentResource {
//                 resource: ManuallyDrop::new(resource),
//                 permit: ManuallyDrop::new(permit),
//                 pool_inner,
//             })
//         }
//     }

//     pub fn add(&self, resource: T) -> Result<(), PoolClosed> {
//         self.add_multiple(std::iter::once(resource))
//     }

//     pub fn add_multiple<I: IntoIterator<Item = T>>(&self, resources: I) -> Result<(), PoolClosed> {
//         let resources = resources.into_iter();
//         let mut inner = self.inner.vec.lock();
//         let deque = inner.deque.as_mut().ok_or(PoolClosed)?;
//         let mut num_new = 0;
//         deque.extend(resources.inspect(|_| num_new += 1));
//         inner.total_count += num_new;
//         self.sem.add_permits(num_new);
//         Ok(())
//     }

//     pub fn num_total(&self) -> usize {
//         self.inner.vec.lock().total_count
//     }

//     pub fn num_available(&self) -> usize {
//         self.sem.available_permits()
//     }

//     pub fn close(&self) -> Closed<'_> {
//         let mut vec = self.inner.vec.lock();
//         self.sem.close();
//         if let Some(deque) = vec.deque.take() {
//             vec.total_count -= deque.len();
//         }
//         if vec.total_count == 0 {
//             self.inner.closed_notify.notify();
//         }
//         self.closed()
//     }

//     pub fn closed(&self) -> Closed<'_> {
//         Closed {
//             notified: self.inner.closed_notify.notified(),
//         }
//     }
// }

// impl<T> FromIterator<T> for LendingPool<T> {
//     fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
//         let deque = VecDeque::from_iter(iter);
//         Self {
//             sem: Arc::new(Semaphore::new(deque.len())),
//             inner: Arc::new(LendingPoolInner {
//                 closed_notify: NotifyOnce::new(),
//                 vec: Mutex::new(PoolVec {
//                     total_count: deque.len(),
//                     deque: Some(deque),
//                 }),
//             }),
//         }
//     }
// }

pin_project_lite::pin_project! {
    pub struct Closed<'a> {
        #[pin]
        notified: NotifiedOnce<'a>,
    }
}

impl Future for Closed<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().notified.poll(cx)
    }
}

// pub struct LentResource<T> {
//     resource: ManuallyDrop<T>,
//     permit: ManuallyDrop<OwnedSemaphorePermit>,
//     pool_inner: Arc<LendingPoolInner<T>>,
// }

impl<T> Deref for LentResource<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.resource
    }
}

impl<T> DerefMut for LentResource<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.resource
    }
}

// // impl<T> LentResource<T> {
// //     fn keep(this: Self) -> T {
// //         let mut this = ManuallyDrop::new(this);
// //         let resource = unsafe { ManuallyDrop::take(&mut this.resource) };
// //         let permit = unsafe { ManuallyDrop::take(&mut this.permit) };
// //         permit.forget();
// //         let prev_count = this.pool.total_count.fetch_sub(1, SeqCst);
// //         resource
// //     }
// // }

// impl<T> Drop for LentResource<T> {
//     fn drop(&mut self) {
//         let resource = unsafe { ManuallyDrop::take(&mut self.resource) };
//         let permit = unsafe { ManuallyDrop::take(&mut self.permit) };
//         {
//             let mut vec = self.pool_inner.vec.lock();
//             if let Some(deque) = &mut vec.deque {
//                 deque.push_back(resource);
//                 drop(permit);
//             } else {
//                 drop(resource);
//                 permit.forget();
//                 vec.total_count -= 1;
//                 if vec.total_count == 0 {
//                     self.pool_inner.closed_notify.notify();
//                 }
//             }
//         }
//     }
// }
