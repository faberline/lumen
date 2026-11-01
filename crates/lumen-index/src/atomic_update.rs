//! Atomic updates shared by the internal counters. Keep the existing Docker
//! builder's Rust 1.92 support without using the API deprecated in Rust 1.99.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

macro_rules! update {
    ($name:ident, $atomic:ty, $value:ty) => {
        pub(crate) fn $name(
            atomic: &$atomic,
            set_order: Ordering,
            fetch_order: Ordering,
            mut change: impl FnMut($value) -> Option<$value>,
        ) -> Result<$value, $value> {
            let mut previous = atomic.load(fetch_order);
            loop {
                let Some(next) = change(previous) else {
                    return Err(previous);
                };
                match atomic.compare_exchange_weak(previous, next, set_order, fetch_order) {
                    Ok(replaced) => return Ok(replaced),
                    Err(observed) => previous = observed,
                }
            }
        }
    };
}

update!(update_u64, AtomicU64, u64);
update!(update_usize, AtomicUsize, usize);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_identity_exhaustion_preserves_both_counter_types() {
        let wide = AtomicU64::new(u64::MAX);
        assert_eq!(
            update_u64(&wide, Ordering::Relaxed, Ordering::Relaxed, |n| n
                .checked_add(1)),
            Err(u64::MAX)
        );
        assert_eq!(wide.load(Ordering::Relaxed), u64::MAX);
        let native = AtomicUsize::new(usize::MAX);
        assert_eq!(
            update_usize(&native, Ordering::AcqRel, Ordering::Acquire, |n| n
                .checked_add(1)),
            Err(usize::MAX)
        );
        assert_eq!(native.load(Ordering::Acquire), usize::MAX);
    }

    #[test]
    fn intervening_write_retries_from_the_observed_value() {
        let value = AtomicU64::new(1);
        let mut calls = 0;
        let replaced = update_u64(&value, Ordering::AcqRel, Ordering::Acquire, |n| {
            calls += 1;
            if calls == 1 {
                value.store(5, Ordering::Release);
            }
            n.checked_add(1)
        });
        assert_eq!(replaced, Ok(5));
        assert_eq!(value.load(Ordering::Acquire), 6);
        assert!(calls >= 2);
    }

    #[test]
    fn concurrent_increments_return_each_replaced_value_once() {
        let value = std::sync::Arc::new(AtomicUsize::new(0));
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let value = value.clone();
                std::thread::spawn(move || {
                    (0..64)
                        .map(|_| {
                            update_usize(&value, Ordering::AcqRel, Ordering::Acquire, |n| {
                                n.checked_add(1)
                            })
                            .unwrap()
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut replaced: Vec<_> = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect();
        replaced.sort_unstable();
        assert_eq!(replaced, (0..256).collect::<Vec<_>>());
        assert_eq!(value.load(Ordering::Acquire), 256);
    }
}
