//! The set of resources that need reconciling, and the rules of taking
//! them out of it.
//!
//! Resources are put in with [`Scheduler::mark`]. A worker takes one out
//! with [`Scheduler::next`], reconciles it, and reports how that went with
//! [`Scheduler::finish`]. The scheduler knows nothing of the source or of
//! Morph. It deals in resource ids, in turns, and in the numbers of the
//! transactions that a turn settles.
//!
//! # The rules
//!
//! * **Once.** A resource is in the set once, however often it is marked.
//! * **One turn at a time.** A resource that has been handed out is not
//!   handed out again until its turn has been finished. A mark that arrives
//!   meanwhile is kept beside the turn, and makes the resource wait again
//!   when the turn ends.
//! * **Bounded.** A resource that waits holds one of a fixed number of
//!   places, and keeps it through its turn. A mark that needs a place waits
//!   for one, which is how a full set slows down whoever is marking.
//! * **Oldest first.** Of the resources that are due, the one that began to
//!   wait first is handed out.
//! * **Retry, then give up.** A turn that fails makes the resource wait
//!   again, for a time that grows with each failure. After
//!   `max_attempts` failures in a row the resource is listed as unhealthy,
//!   and is left alone until it is marked again.
//! * **Wait for what is needed.** A turn that cannot go on until other
//!   resources have been reconciled parks the resource. It is handed out
//!   again as soon as any of them succeeds, or when it is marked. The
//!   resources it waits for are marked if nothing else has them in hand.
//!   A wait that would go round in a circle is a failure instead.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use morph::ResourceId;
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::config::RetryConfig;

/// Why a resource is to be reconciled.
///
/// The origin is reported with the turn and has no effect on scheduling.
/// When marks are merged, the origin of the first is the one that is kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// A committed transaction changed its row.
    Change,
    /// A full reconciliation marked it, whether or not anything changed.
    Periodic,
    /// It was in the dirty table when the controller started.
    Recovered,
    /// A resource that refers to it is waiting for it to be reconciled.
    Needed,
    /// A resource it refers to changed what it exports.
    Propagated,
}

/// What became of a mark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Marked {
    /// The resource was neither waiting nor in a turn, and is waiting now.
    New,
    /// The mark was merged with what was there: the resource was already
    /// waiting, or is in a turn and will wait again when the turn ends.
    Coalesced,
}

/// A resource waiting its turn.
#[derive(Debug)]
struct Waiting {
    /// When it began to wait. The earliest goes first.
    since: Instant,
    /// The earliest moment its turn may begin: at once for a mark, after
    /// the wait for a retry.
    not_before: Instant,
    /// How many turns in a row have failed.
    attempts: u32,
    /// The transactions to settle once the resource is dealt with.
    covers: Vec<u64>,
    origin: Origin,
    /// Holds a place in the set, so that the set stays bounded. None for a
    /// resource that the scheduler marked itself because a parked resource
    /// waits for it: such a mark may not wait for room, since the parked
    /// resources may hold every place.
    _place: Option<OwnedSemaphorePermit>,
}

/// A resource in a turn, and what to do with it afterwards.
#[derive(Debug)]
struct Running {
    /// A mark that arrived during the turn. It becomes the resource's next
    /// wait when the turn ends.
    again: Option<Waiting>,
}

/// A resource that waits for others to be reconciled before its next turn.
#[derive(Debug)]
struct Parked {
    /// The wait to resume with, place and all.
    waiting: Waiting,
    /// What it waits for. Any one of them succeeding ends the wait.
    on: BTreeSet<ResourceId>,
}

/// One turn at reconciling a resource.
///
/// A lease comes from [`Scheduler::next`] and is given back with
/// [`Scheduler::finish`]. While it is out, the resource is handed to nobody
/// else. It holds the resource's place in the set and one worker permit.
///
/// A lease that is dropped instead gives back the place and the permit,
/// but the scheduler goes on counting the resource as in a turn. It is
/// never handed out again, and the transactions the turn covered are never
/// settled.
#[derive(Debug)]
pub struct Lease {
    /// The resource to reconcile.
    pub resource: ResourceId,
    /// Which attempt this is, starting at one: one more than the number of
    /// turns in a row that have failed.
    pub attempt: u32,
    /// Why the resource was marked.
    pub origin: Origin,
    /// How long the resource waited for this turn: since it was marked, or
    /// since its last turn failed.
    pub waited: Duration,
    covers: Vec<u64>,
    _place: Option<OwnedSemaphorePermit>,
    _worker: OwnedSemaphorePermit,
}

/// How a turn ended, as the worker tells [`Scheduler::finish`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The resource is reconciled.
    Succeeded,
    /// It is not, for this reason. The scheduler does not read the reason.
    Failed(String),
    /// It cannot be until these resources have been reconciled, and is to
    /// be tried again as soon as any of them has been.
    Waiting(Vec<ResourceId>),
}

/// What became of a resource once its turn ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Reconciled, and no longer in the set.
    Done,
    /// Reconciled, and waiting again because it was marked during the turn.
    DoneAndMarkedAgain,
    /// Failed, and waiting to be tried again.
    ///
    /// The duration is the wait worked out for this failure. A resource
    /// that was marked during the turn is due at once instead, and its next
    /// turn still counts as the next attempt.
    Retrying(Duration),
    /// Failed as often as allowed, and listed as unhealthy.
    ///
    /// The resource is not tried again until it is marked again. If it was
    /// marked during the turn it is waiting already, with its attempts
    /// counted from one.
    Unhealthy,
    /// Parked until one of the resources it waits for is reconciled, or
    /// until it is marked. A resource that was marked during the turn is
    /// not parked: it waits for its next turn at once.
    ///
    /// Of the resources waited for, those that were neither waiting, nor
    /// in a turn, nor parked themselves have been marked with
    /// [`Origin::Needed`], so that the wait ends.
    Waiting,
}

/// Everything the lock protects.
#[derive(Debug, Default)]
struct Inner {
    /// Resources waiting for a turn. None of them is also in `running`.
    waiting: BTreeMap<ResourceId, Waiting>,
    /// Resources in a turn.
    running: BTreeMap<ResourceId, Running>,
    /// Resources parked until something they wait for is reconciled. None
    /// of them is in `waiting` or `running`.
    parked: BTreeMap<ResourceId, Parked>,
    /// The parked resources that wait for each resource: the index of
    /// `parked` by what is waited for.
    waiters: BTreeMap<ResourceId, BTreeSet<ResourceId>>,
    /// Resources that were given up on and have not been reconciled since.
    unhealthy: BTreeSet<ResourceId>,
    /// Set by `close`. No turn is handed out once it is set.
    closed: bool,
}

/// Decides which resource is reconciled next.
///
/// A resource is in the set once however many times it was marked. Two
/// turns at the same resource never overlap: a mark that arrives while the
/// resource is in a turn is kept, and makes it wait again when the turn
/// ends. The set is bounded, and a mark waits for room. Failures are tried
/// again after a growing wait, and given up on after enough of them.
///
/// # Waiting for other resources
///
/// A turn may end with [`Outcome::Waiting`]: the resource cannot be
/// reconciled until others have been, as a row that refers to a row whose
/// resource has no record yet. The resource is then parked, keeping its
/// place and the transactions it covers, and leaves the set of waiting
/// resources. It is handed out again when any of the resources it waits
/// for finishes a turn successfully, or when it is marked. The resources
/// it waits for are marked by the scheduler itself if nothing else has
/// them in hand, under [`Origin::Needed`] and without taking a place, so
/// that a set full of parked resources cannot stop the one they all need
/// from being marked. A resource that would wait, directly or through
/// others, for itself has its turn counted as failed instead.
///
/// # Places and permits
///
/// Two semaphores bound the work, and neither is ever closed.
///
/// * A *place* is held by every resource that waits, and stays with it
///   through its turn and while it is parked. A mark that is kept during a
///   turn holds a place of its own. There are `capacity` places, so no
///   more resources than that can be waiting, parked or in a turn, besides
///   those the scheduler marked itself, and a [`mark`](Self::mark) that
///   needs a place waits until one is given back. That holds whoever marks
///   to the pace of the workers. In the controller it is the intake that
///   marks, and behind it the reader of the replication stream.
/// * A *worker permit* is held by every [`Lease`], from
///   [`next`](Self::next) to [`finish`](Self::finish). There are `workers`
///   of them, so no more turns than that are in progress at once.
///
/// # Locking
///
/// The waiting resources, the turns in progress, the unhealthy list and the
/// closed flag are behind one asynchronous mutex. It is held only to look
/// at them and to change them. Waiting for a place, for a worker permit, or
/// for a resource to become due is done with the mutex released, so a full
/// set never stops a turn from being finished.
///
/// # Examples
///
/// ```
/// use morph::ResourceId;
/// use morph_controller_core::config::RetryConfig;
/// use morph_controller_core::scheduler::{Disposition, Marked, Origin, Outcome, Scheduler};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let scheduler = Scheduler::new(100, 4, RetryConfig::default(), |_transaction| {});
/// let queue: ResourceId = "aws_sqs_queue.orders".parse().unwrap();
///
/// // However often a resource is marked, it waits once.
/// assert_eq!(scheduler.mark(queue.clone(), None, Origin::Change).await, Marked::New);
/// assert_eq!(scheduler.mark(queue.clone(), None, Origin::Periodic).await, Marked::Coalesced);
/// assert_eq!(scheduler.waiting().await, 1);
///
/// // A worker takes the turn, reconciles the resource, and says how it went.
/// let lease = scheduler.next().await.expect("the scheduler is open");
/// assert_eq!(lease.resource, queue);
/// assert_eq!((lease.attempt, lease.origin), (1, Origin::Change));
/// assert_eq!(scheduler.finish(lease, Outcome::Succeeded).await, Disposition::Done);
///
/// // Once closed, the scheduler hands out nothing more.
/// scheduler.close().await;
/// assert!(scheduler.next().await.is_none());
/// # }
/// ```
pub struct Scheduler {
    inner: Mutex<Inner>,
    /// Wakes the calls to `next` that found nothing due.
    wake: Notify,
    places: Arc<Semaphore>,
    workers: Arc<Semaphore>,
    retry: RetryConfig,
    settle: Box<dyn Fn(u64) + Send + Sync>,
}

impl Inner {
    /// Takes `resource` out of the parked resources, and out of the index
    /// of what they wait for, and gives back the wait it was parked with.
    ///
    /// The caller has checked that it is parked.
    fn unpark(&mut self, resource: &ResourceId) -> Waiting {
        let parked = self.parked.remove(resource).expect("checked to be parked");
        for needed in &parked.on {
            if let Some(waiters) = self.waiters.get_mut(needed) {
                waiters.remove(resource);
                if waiters.is_empty() {
                    self.waiters.remove(needed);
                }
            }
        }
        parked.waiting
    }

    /// Makes every resource parked waiting for `resource` wait again at
    /// once, and says how many there were.
    fn wake_waiters(&mut self, resource: &ResourceId) -> usize {
        let Some(waiters) = self.waiters.remove(resource) else {
            return 0;
        };
        let count = waiters.len();
        for waiter in waiters {
            if !self.parked.contains_key(&waiter) {
                continue;
            }
            let mut waiting = self.unpark(&waiter);
            waiting.not_before = Instant::now();
            self.waiting.insert(waiter, waiting);
        }
        count
    }

    /// Whether `from`, if it is parked, waits for `target` directly or
    /// through other parked resources.
    fn reaches(&self, from: &ResourceId, target: &ResourceId) -> bool {
        let mut pending = vec![from.clone()];
        let mut seen = BTreeSet::new();
        while let Some(current) = pending.pop() {
            if current == *target {
                return true;
            }
            if !seen.insert(current.clone()) {
                continue;
            }
            if let Some(parked) = self.parked.get(&current) {
                pending.extend(parked.on.iter().cloned());
            }
        }
        false
    }
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("places", &self.places.available_permits())
            .field("workers", &self.workers.available_permits())
            .finish_non_exhaustive()
    }
}

impl Scheduler {
    /// A scheduler with room for `capacity` resources, `workers` turns at
    /// once, and `retry` for failures.
    ///
    /// A `capacity` or a `workers` of nought is treated as one.
    ///
    /// `settle` is told the number of a transaction once for every mark
    /// that named it, when the turn that covers the mark succeeds or the
    /// resource is given up on. It is called from within
    /// [`finish`](Self::finish) while the scheduler's lock is held, so it
    /// should return quickly.
    pub fn new(
        capacity: usize,
        workers: usize,
        retry: RetryConfig,
        settle: impl Fn(u64) + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            wake: Notify::new(),
            places: Arc::new(Semaphore::new(capacity.max(1))),
            workers: Arc::new(Semaphore::new(workers.max(1))),
            retry,
            settle: Box::new(settle),
        }
    }

    /// Marks `resource` as needing reconciliation, and waits for room if
    /// the set is full.
    ///
    /// `covers` is the number of the transaction that said so, if one did.
    /// It is settled once the resource has been dealt with. `origin` is
    /// kept if this mark begins a new wait, and dropped if the mark is
    /// merged into one that has an origin already.
    ///
    /// A resource that is already waiting, or that is in a turn with a mark
    /// already kept, takes the mark in at once and needs no room. Any other
    /// mark takes a place first, waiting for one with the lock released,
    /// and then looks again, because things may have moved on meanwhile:
    ///
    /// * a resource that has begun to wait takes the mark in, and the place
    ///   is given back;
    /// * a resource in a turn has the mark kept for when the turn ends;
    /// * any other resource begins to wait, and one waiting call to
    ///   [`next`](Self::next) is woken.
    ///
    /// Only the last of these is [`Marked::New`].
    ///
    /// A resource that is parked is woken by a mark: it waits again at
    /// once, with what it covered and the new mark's transaction, and
    /// needs no room, because it kept its place.
    ///
    /// A mark does not take a resource off the unhealthy list. Only a turn
    /// that succeeds does that. Marks are still taken after
    /// [`close`](Self::close), though nothing hands them out.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. If the future is dropped before it
    /// completes, the resource has not been marked, and a place that had
    /// been taken is given back.
    pub async fn mark(&self, resource: ResourceId, covers: Option<u64>, origin: Origin) -> Marked {
        {
            let mut inner = self.inner.lock().await;
            if let Some(waiting) = inner.waiting.get_mut(&resource) {
                waiting.covers.extend(covers);
                return Marked::Coalesced;
            }
            if let Some(running) = inner.running.get_mut(&resource)
                && let Some(again) = &mut running.again
            {
                again.covers.extend(covers);
                return Marked::Coalesced;
            }
            if inner.parked.contains_key(&resource) {
                let mut waiting = inner.unpark(&resource);
                waiting.covers.extend(covers);
                waiting.not_before = Instant::now();
                inner.waiting.insert(resource, waiting);
                self.wake.notify_one();
                return Marked::Coalesced;
            }
        }
        // Room is waited for with the lock released. A place is given back
        // by `finish`, which needs the lock, so waiting with the lock held
        // would wait for ever.
        let place = Arc::clone(&self.places)
            .acquire_owned()
            .await
            .expect("the set is never closed to new places");
        let mut inner = self.inner.lock().await;
        // Things may have moved on while room was being waited for: another
        // mark may have made the resource wait, or its turn may have begun
        // or ended.
        if let Some(waiting) = inner.waiting.get_mut(&resource) {
            waiting.covers.extend(covers);
            return Marked::Coalesced;
        }
        let waiting = Waiting {
            since: Instant::now(),
            not_before: Instant::now(),
            attempts: 0,
            covers: covers.into_iter().collect(),
            origin,
            _place: Some(place),
        };
        if inner.parked.contains_key(&resource) {
            // Parked meanwhile: woken as above, and the place just taken is
            // given back with `waiting`.
            let mut parked = inner.unpark(&resource);
            parked.covers.extend(waiting.covers);
            parked.not_before = Instant::now();
            inner.waiting.insert(resource, parked);
            self.wake.notify_one();
            return Marked::Coalesced;
        }
        if let Some(running) = inner.running.get_mut(&resource) {
            match &mut running.again {
                Some(again) => again.covers.extend(waiting.covers),
                None => running.again = Some(waiting),
            }
            return Marked::Coalesced;
        }
        inner.waiting.insert(resource, waiting);
        self.wake.notify_one();
        Marked::New
    }

    /// Hands out the next turn, waiting for a free worker and for a
    /// resource that is due.
    ///
    /// A worker permit is taken first. Then, of the waiting resources that
    /// are due, the one that began to wait first is moved into a turn and
    /// returned as a [`Lease`]. If none is due, the call waits until the
    /// earliest of them is, or until a mark or a finished turn wakes it,
    /// and looks again.
    ///
    /// Returns `None` once the scheduler is closed, whatever is still
    /// waiting. The closed flag is looked at only when a worker permit has
    /// been had, so a call made while every permit is out returns when one
    /// is given back.
    ///
    /// # Cancel safety
    ///
    /// Dropping the future loses no resource: nothing leaves the set until
    /// the call returns, and the worker permit is given back. A wake-up
    /// that the dropped call had already taken is not passed on, though. A
    /// resource that has become due may then stay in the set until another
    /// call looks: a new one, or one that a later mark or retry wakes.
    pub async fn next(&self) -> Option<Lease> {
        // The permit comes first, so that no resource leaves the set before
        // there is a worker free to take it.
        let worker = Arc::clone(&self.workers)
            .acquire_owned()
            .await
            .expect("workers are never closed");
        loop {
            let soonest = {
                let mut inner = self.inner.lock().await;
                if inner.closed {
                    return None;
                }
                let now = Instant::now();
                let ready = inner
                    .waiting
                    .iter()
                    .filter(|(_, waiting)| waiting.not_before <= now)
                    .min_by_key(|(_, waiting)| waiting.since)
                    .map(|(resource, _)| resource.clone());
                if let Some(resource) = ready {
                    let waiting = inner.waiting.remove(&resource).expect("just found");
                    inner
                        .running
                        .insert(resource.clone(), Running { again: None });
                    return Some(Lease {
                        resource,
                        attempt: waiting.attempts + 1,
                        origin: waiting.origin,
                        waited: now.saturating_duration_since(waiting.since),
                        covers: waiting.covers,
                        _place: waiting._place,
                        _worker: worker,
                    });
                }
                inner
                    .waiting
                    .values()
                    .map(|waiting| waiting.not_before)
                    .min()
            };
            match soonest {
                Some(when) => {
                    tokio::select! {
                        () = tokio::time::sleep_until(when) => {}
                        () = self.wake.notified() => {}
                    }
                }
                None => self.wake.notified().await,
            }
        }
    }

    /// Ends a turn, and says what becomes of the resource.
    ///
    /// * [`Outcome::Succeeded`]: the resource leaves the unhealthy list, and
    ///   the transactions the turn covered are settled. If it was marked
    ///   during the turn, it waits again as that mark left it.
    /// * [`Outcome::Failed`] before attempt `max_attempts`: the resource
    ///   waits again in the same place, covering the same transactions, and
    ///   is due after [`Backoff::delay`](crate::config::Backoff::delay) for
    ///   this attempt. A mark kept during the turn is merged into that wait
    ///   and makes it due at once.
    /// * [`Outcome::Failed`] on attempt `max_attempts` or later: the
    ///   resource joins the unhealthy list, and the transactions the turn
    ///   covered are settled, because this mark will do no more for them. A
    ///   mark kept during the turn makes the resource wait again, with its
    ///   attempts counted afresh.
    /// * [`Outcome::Waiting`]: the resource is parked with what it covers
    ///   until one of the resources it waits for succeeds, and those of
    ///   them that nothing has in hand are marked under
    ///   [`Origin::Needed`]. The attempt is not counted as a failure. A
    ///   mark kept during the turn makes the resource wait again at once
    ///   instead of parking. Waiting for a resource that waits for this
    ///   one, directly or through others, or for itself, is counted as a
    ///   failure, as [`Outcome::Failed`] would be.
    ///
    /// A success also wakes every resource parked waiting for this one:
    /// each waits again at once, as it was parked.
    ///
    /// The worker permit is given back in every case, and the place unless
    /// the resource waits again in it or is parked.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. A future dropped before it completes
    /// leaves the resource as a dropped [`Lease`] does.
    pub async fn finish(&self, lease: Lease, outcome: Outcome) -> Disposition {
        let Lease {
            resource,
            attempt,
            origin,
            covers,
            _place: place,
            ..
        } = lease;
        let mut inner = self.inner.lock().await;
        let running = inner.running.remove(&resource);
        let again = running.and_then(|running| running.again);
        // A wait that would go round in a circle, or for nothing at all,
        // is a failure: nothing would ever end it.
        let outcome = match outcome {
            Outcome::Waiting(on) => {
                let circular = on.is_empty()
                    || on
                        .iter()
                        .any(|needed| *needed == resource || inner.reaches(needed, &resource));
                if circular {
                    Outcome::Failed("waits for itself".to_owned())
                } else {
                    Outcome::Waiting(on)
                }
            }
            other => other,
        };
        match outcome {
            Outcome::Succeeded => {
                inner.unhealthy.remove(&resource);
                for number in covers {
                    (self.settle)(number);
                }
                let woken = inner.wake_waiters(&resource);
                if woken > 0 {
                    self.wake.notify_waiters();
                }
                match again {
                    Some(again) => {
                        inner.waiting.insert(resource, again);
                        self.wake.notify_one();
                        Disposition::DoneAndMarkedAgain
                    }
                    None => Disposition::Done,
                }
            }
            Outcome::Waiting(on) => {
                let mut waiting = Waiting {
                    since: Instant::now(),
                    not_before: Instant::now(),
                    attempts: attempt.saturating_sub(1),
                    covers,
                    origin,
                    _place: place,
                };
                if let Some(again) = again {
                    // Marked meanwhile: there is something new to look at,
                    // so the resource is due at once rather than parked.
                    waiting.covers.extend(again.covers);
                    inner.waiting.insert(resource, waiting);
                    self.wake.notify_one();
                    return Disposition::Waiting;
                }
                let on: BTreeSet<ResourceId> = on.into_iter().collect();
                for needed in &on {
                    inner
                        .waiters
                        .entry(needed.clone())
                        .or_default()
                        .insert(resource.clone());
                    let in_hand = inner.waiting.contains_key(needed)
                        || inner.running.contains_key(needed)
                        || inner.parked.contains_key(needed);
                    if !in_hand {
                        inner.waiting.insert(
                            needed.clone(),
                            Waiting {
                                since: Instant::now(),
                                not_before: Instant::now(),
                                attempts: 0,
                                covers: Vec::new(),
                                origin: Origin::Needed,
                                _place: None,
                            },
                        );
                        self.wake.notify_one();
                    }
                }
                inner.parked.insert(resource, Parked { waiting, on });
                Disposition::Waiting
            }
            Outcome::Failed(_) if attempt >= self.retry.max_attempts => {
                inner.unhealthy.insert(resource.clone());
                // The transactions are dealt with as far as they ever will
                // be by this mark, and settling them lets their position be
                // confirmed. The next full reconciliation tries again.
                for number in covers {
                    (self.settle)(number);
                }
                if let Some(again) = again {
                    inner.waiting.insert(resource, again);
                    self.wake.notify_one();
                }
                Disposition::Unhealthy
            }
            Outcome::Failed(_) => {
                let delay = self.retry.backoff.delay(attempt);
                let mut waiting = Waiting {
                    since: Instant::now(),
                    not_before: Instant::now() + delay,
                    attempts: attempt,
                    covers,
                    origin,
                    _place: place,
                };
                if let Some(again) = again {
                    // A mark that arrived during the turn has been due since
                    // it was made, so the retry is due at once, and settles
                    // that mark's transactions with its own.
                    waiting.not_before = waiting.not_before.min(again.not_before);
                    waiting.covers.extend(again.covers);
                }
                inner.waiting.insert(resource, waiting);
                self.wake.notify_one();
                Disposition::Retrying(delay)
            }
        }
    }

    /// Stops turns from being handed out.
    ///
    /// The closed flag is set, and the calls to [`next`](Self::next) that
    /// are waiting for a resource at that moment are woken, to find the
    /// flag and return `None`. Resources that are waiting stay in the set
    /// and are handed to nobody. Turns already handed out are not
    /// interrupted, and [`finish`](Self::finish) works as before. A closed
    /// scheduler cannot be opened again.
    pub async fn close(&self) {
        self.inner.lock().await.closed = true;
        self.wake.notify_waiters();
    }

    /// How many resources are waiting for a turn, due or not.
    pub async fn waiting(&self) -> usize {
        self.inner.lock().await.waiting.len()
    }

    /// How many resources are in a turn.
    pub async fn running(&self) -> usize {
        self.inner.lock().await.running.len()
    }

    /// How many resources are parked, waiting for others.
    pub async fn parked(&self) -> usize {
        self.inner.lock().await.parked.len()
    }

    /// The parked resources, each with what it waits for, in the order of
    /// their ids.
    pub async fn parked_on(&self) -> Vec<(ResourceId, Vec<ResourceId>)> {
        self.inner
            .lock()
            .await
            .parked
            .iter()
            .map(|(resource, parked)| (resource.clone(), parked.on.iter().cloned().collect()))
            .collect()
    }

    /// The resources that were given up on and have not been reconciled
    /// since, in the order of their ids.
    pub async fn unhealthy(&self) -> Vec<ResourceId> {
        self.inner.lock().await.unhealthy.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;

    fn id(text: &str) -> ResourceId {
        text.parse().unwrap()
    }

    fn retry(max_attempts: u32) -> RetryConfig {
        RetryConfig {
            max_attempts,
            backoff: crate::config::Backoff {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(8),
                multiplier: 2.0,
            },
        }
    }

    fn scheduler(
        capacity: usize,
        workers: usize,
        max_attempts: u32,
    ) -> (Arc<Scheduler>, Arc<StdMutex<Vec<u64>>>) {
        let settled = Arc::new(StdMutex::new(Vec::new()));
        let recorder = Arc::clone(&settled);
        let scheduler = Scheduler::new(capacity, workers, retry(max_attempts), move |number| {
            recorder.lock().unwrap().push(number);
        });
        (Arc::new(scheduler), settled)
    }

    #[tokio::test]
    async fn a_resource_marked_three_times_gets_one_turn() {
        let (scheduler, _) = scheduler(10, 2, 3);
        assert_eq!(
            scheduler
                .mark(id("server.a"), Some(1), Origin::Change)
                .await,
            Marked::New
        );
        assert_eq!(
            scheduler
                .mark(id("server.a"), Some(2), Origin::Change)
                .await,
            Marked::Coalesced
        );
        assert_eq!(
            scheduler.mark(id("server.a"), None, Origin::Periodic).await,
            Marked::Coalesced
        );
        assert_eq!(
            scheduler
                .mark(id("server.b"), Some(2), Origin::Change)
                .await,
            Marked::New
        );
        assert_eq!(scheduler.waiting().await, 2);

        let first = scheduler.next().await.unwrap();
        let second = scheduler.next().await.unwrap();
        assert_eq!(first.resource, id("server.a"), "the oldest first");
        assert_eq!(second.resource, id("server.b"));
        assert_eq!(first.covers, [1, 2]);
        assert_eq!(scheduler.waiting().await, 0);
        assert_eq!(scheduler.running().await, 2);
    }

    #[tokio::test]
    async fn the_same_resource_never_runs_twice_at_once() {
        let (scheduler, settled) = scheduler(10, 4, 3);
        scheduler
            .mark(id("server.a"), Some(1), Origin::Change)
            .await;
        let lease = scheduler.next().await.unwrap();

        // Marked while running: no second turn until the first ends.
        assert_eq!(
            scheduler
                .mark(id("server.a"), Some(2), Origin::Change)
                .await,
            Marked::Coalesced
        );
        let nothing = tokio::time::timeout(Duration::from_millis(50), scheduler.next()).await;
        assert!(nothing.is_err(), "no turn while the resource runs");

        let disposition = scheduler.finish(lease, Outcome::Succeeded).await;
        assert_eq!(disposition, Disposition::DoneAndMarkedAgain);
        assert_eq!(settled.lock().unwrap().as_slice(), [1]);
        let again = scheduler.next().await.unwrap();
        assert_eq!(again.resource, id("server.a"));
        assert_eq!(again.covers, [2]);
        assert_eq!(
            scheduler.finish(again, Outcome::Succeeded).await,
            Disposition::Done
        );
        assert_eq!(settled.lock().unwrap().as_slice(), [1, 2]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_is_tried_again_after_a_growing_wait_and_then_given_up_on() {
        let (scheduler, settled) = scheduler(10, 1, 3);
        scheduler
            .mark(id("server.a"), Some(7), Origin::Change)
            .await;

        let lease = scheduler.next().await.unwrap();
        assert_eq!(lease.attempt, 1);
        let Disposition::Retrying(delay) =
            scheduler.finish(lease, Outcome::Failed("no".into())).await
        else {
            panic!("not retrying");
        };
        assert!(
            delay >= Duration::from_millis(800) && delay <= Duration::from_millis(1200),
            "{delay:?}"
        );
        let too_soon = tokio::time::timeout(Duration::from_millis(500), scheduler.next()).await;
        assert!(too_soon.is_err(), "not before the wait is over");
        let lease = scheduler.next().await.unwrap();
        assert_eq!(lease.attempt, 2);
        assert!(matches!(
            scheduler.finish(lease, Outcome::Failed("no".into())).await,
            Disposition::Retrying(_)
        ));
        assert!(
            settled.lock().unwrap().is_empty(),
            "nothing is dealt with yet"
        );

        let lease = scheduler.next().await.unwrap();
        assert_eq!(lease.attempt, 3);
        assert_eq!(
            scheduler.finish(lease, Outcome::Failed("no".into())).await,
            Disposition::Unhealthy
        );
        assert_eq!(scheduler.unhealthy().await, [id("server.a")]);
        assert_eq!(
            settled.lock().unwrap().as_slice(),
            [7],
            "given up on counts as dealt with"
        );
        assert_eq!(scheduler.waiting().await, 0);

        // A new mark gives it another go, with the attempts counted afresh,
        // and success takes it off the unhealthy list.
        scheduler.mark(id("server.a"), None, Origin::Periodic).await;
        let lease = scheduler.next().await.unwrap();
        assert_eq!(lease.attempt, 1);
        scheduler.finish(lease, Outcome::Succeeded).await;
        assert!(scheduler.unhealthy().await.is_empty());
    }

    #[tokio::test]
    async fn the_set_is_bounded_and_a_mark_waits_for_room() {
        let (scheduler, _) = scheduler(1, 1, 3);
        scheduler.mark(id("server.a"), None, Origin::Change).await;
        let blocked = tokio::time::timeout(
            Duration::from_millis(50),
            scheduler.mark(id("server.b"), None, Origin::Change),
        )
        .await;
        assert!(blocked.is_err(), "no room for a second");
        // Marking the same one again needs no room.
        assert_eq!(
            scheduler.mark(id("server.a"), None, Origin::Change).await,
            Marked::Coalesced
        );

        let lease = scheduler.next().await.unwrap();
        let marking = tokio::spawn({
            let scheduler = Arc::clone(&scheduler);
            async move { scheduler.mark(id("server.b"), None, Origin::Change).await }
        });
        scheduler.finish(lease, Outcome::Succeeded).await;
        assert_eq!(marking.await.unwrap(), Marked::New, "room was made");
    }

    #[tokio::test]
    async fn closing_stops_turns_from_being_handed_out() {
        let (scheduler, _) = scheduler(10, 1, 3);
        scheduler.mark(id("server.a"), None, Origin::Change).await;
        scheduler.close().await;
        assert!(scheduler.next().await.is_none());
        assert_eq!(
            scheduler.waiting().await,
            1,
            "still on record for whoever restarts"
        );
    }

    #[tokio::test]
    async fn only_so_many_turns_run_at_once() {
        let (scheduler, _) = scheduler(10, 1, 3);
        scheduler.mark(id("server.a"), None, Origin::Change).await;
        scheduler.mark(id("server.b"), None, Origin::Change).await;
        let first = scheduler.next().await.unwrap();
        let waiting = tokio::time::timeout(Duration::from_millis(50), scheduler.next()).await;
        assert!(waiting.is_err(), "one worker, one turn");
        scheduler.finish(first, Outcome::Succeeded).await;
        assert_eq!(scheduler.next().await.unwrap().resource, id("server.b"));
    }

    #[tokio::test]
    async fn a_lease_carries_its_origin_and_how_long_it_waited_and_the_counts_follow() {
        let (scheduler, settled) = scheduler(10, 2, 3);
        scheduler
            .mark(id("server.a"), None, Origin::Recovered)
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(scheduler.waiting().await, 1);
        assert_eq!(scheduler.running().await, 0);
        let lease = scheduler.next().await.unwrap();
        assert_eq!(lease.origin, Origin::Recovered);
        assert_eq!(lease.attempt, 1);
        assert!(
            lease.waited >= Duration::from_millis(20),
            "{:?}",
            lease.waited
        );
        assert_eq!(scheduler.waiting().await, 0);
        assert_eq!(scheduler.running().await, 1);
        assert!(format!("{lease:?}").starts_with("Lease"));
        assert!(format!("{scheduler:?}").contains("Scheduler"));
        assert_eq!(
            scheduler.finish(lease, Outcome::Succeeded).await,
            Disposition::Done
        );
        assert_eq!(scheduler.running().await, 0);
        assert!(
            settled.lock().unwrap().is_empty(),
            "a mark that covers nothing settles nothing"
        );
    }

    #[tokio::test]
    async fn a_resource_that_waits_is_parked_and_woken_when_what_it_needs_succeeds() {
        let (scheduler, settled) = scheduler(10, 4, 3);
        scheduler
            .mark(id("subscription.billing"), Some(1), Origin::Change)
            .await;
        let lease = scheduler.next().await.unwrap();
        // The subscription needs the topic, which nothing has in hand: the
        // scheduler marks it itself, without a place.
        let disposition = scheduler
            .finish(lease, Outcome::Waiting(vec![id("topic.orders")]))
            .await;
        assert_eq!(disposition, Disposition::Waiting);
        assert_eq!(scheduler.parked().await, 1);
        assert_eq!(scheduler.waiting().await, 1, "the topic waits");
        assert_eq!(
            scheduler.parked_on().await,
            [(id("subscription.billing"), vec![id("topic.orders")])]
        );
        assert!(settled.lock().unwrap().is_empty(), "nothing is settled yet");
        assert_eq!(
            scheduler.places.available_permits(),
            9,
            "the parked resource keeps its place, the needed one takes none"
        );

        let topic = scheduler.next().await.unwrap();
        assert_eq!(topic.resource, id("topic.orders"));
        assert_eq!((topic.origin, topic.attempt), (Origin::Needed, 1));
        // Nothing else is due while the topic runs.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), scheduler.next())
                .await
                .is_err()
        );
        assert_eq!(
            scheduler.finish(topic, Outcome::Succeeded).await,
            Disposition::Done
        );
        assert_eq!(scheduler.parked().await, 0);
        let again = scheduler.next().await.unwrap();
        assert_eq!(again.resource, id("subscription.billing"));
        assert_eq!(again.attempt, 1, "waiting is not a failed attempt");
        assert_eq!(again.covers, [1], "it still covers its transaction");
        assert_eq!(
            scheduler.finish(again, Outcome::Succeeded).await,
            Disposition::Done
        );
        assert_eq!(settled.lock().unwrap().as_slice(), [1]);
        assert_eq!(scheduler.places.available_permits(), 10);
    }

    #[tokio::test]
    async fn a_needed_resource_already_in_hand_is_not_marked_again_and_wakes_all_its_waiters() {
        let (scheduler, _) = scheduler(10, 4, 3);
        scheduler
            .mark(id("topic.orders"), None, Origin::Change)
            .await;
        scheduler.mark(id("sub.a"), Some(1), Origin::Change).await;
        scheduler.mark(id("sub.b"), Some(2), Origin::Change).await;
        let topic = scheduler.next().await.unwrap();
        let a = scheduler.next().await.unwrap();
        let b = scheduler.next().await.unwrap();
        assert_eq!(
            scheduler
                .finish(a, Outcome::Waiting(vec![id("topic.orders")]))
                .await,
            Disposition::Waiting
        );
        assert_eq!(
            scheduler
                .finish(b, Outcome::Waiting(vec![id("topic.orders")]))
                .await,
            Disposition::Waiting
        );
        assert_eq!(scheduler.waiting().await, 0, "the topic is running already");
        assert_eq!(scheduler.parked().await, 2);
        scheduler.finish(topic, Outcome::Succeeded).await;
        assert_eq!(scheduler.parked().await, 0);
        assert_eq!(scheduler.waiting().await, 2);
        let first = scheduler.next().await.unwrap();
        let second = scheduler.next().await.unwrap();
        let mut woken = vec![first.resource.to_string(), second.resource.to_string()];
        woken.sort();
        assert_eq!(woken, ["sub.a", "sub.b"]);
    }

    #[tokio::test]
    async fn a_mark_wakes_a_parked_resource_and_a_failure_of_what_it_needs_leaves_it_parked() {
        let (scheduler, settled) = scheduler(10, 4, 3);
        scheduler.mark(id("sub.a"), Some(1), Origin::Change).await;
        let a = scheduler.next().await.unwrap();
        scheduler
            .finish(a, Outcome::Waiting(vec![id("topic.orders")]))
            .await;
        let topic = scheduler.next().await.unwrap();
        assert!(matches!(
            scheduler.finish(topic, Outcome::Failed("no".into())).await,
            Disposition::Retrying(_)
        ));
        assert_eq!(scheduler.parked().await, 1, "a failure wakes nothing");

        // A mark of the parked resource wakes it, and merges its covers.
        assert_eq!(
            scheduler.mark(id("sub.a"), Some(2), Origin::Periodic).await,
            Marked::Coalesced
        );
        assert_eq!(scheduler.parked().await, 0);
        let a = scheduler.next().await.unwrap();
        assert_eq!(a.resource, id("sub.a"));
        assert_eq!(a.covers, [1, 2]);
        assert_eq!(a.origin, Origin::Change, "the first origin is kept");
        scheduler.finish(a, Outcome::Succeeded).await;
        assert_eq!(settled.lock().unwrap().as_slice(), [1, 2]);
        // The topic's retry is still there, and the waiters index is clean.
        assert_eq!(scheduler.waiting().await, 1);
        assert!(scheduler.inner.lock().await.waiters.is_empty());
    }

    #[tokio::test]
    async fn a_wait_that_goes_round_in_a_circle_is_a_failure() {
        let (scheduler, _) = scheduler(10, 4, 2);
        scheduler.mark(id("a.x"), None, Origin::Change).await;
        scheduler.mark(id("b.y"), None, Origin::Change).await;
        scheduler.mark(id("c.z"), None, Origin::Change).await;
        let a = scheduler.next().await.unwrap();
        let b = scheduler.next().await.unwrap();
        let c = scheduler.next().await.unwrap();
        // a waits for b, b waits for c: fine.
        assert_eq!(
            scheduler.finish(a, Outcome::Waiting(vec![id("b.y")])).await,
            Disposition::Waiting
        );
        assert_eq!(
            scheduler.finish(b, Outcome::Waiting(vec![id("c.z")])).await,
            Disposition::Waiting
        );
        // c waiting for a would close the circle: a failure, retried.
        assert!(matches!(
            scheduler.finish(c, Outcome::Waiting(vec![id("a.x")])).await,
            Disposition::Retrying(_)
        ));
        assert_eq!(scheduler.parked().await, 2);
        // Waiting for itself, or for nothing, is the same.
        let c = scheduler.next().await.unwrap();
        assert_eq!(c.attempt, 2);
        assert_eq!(
            scheduler.finish(c, Outcome::Waiting(vec![id("c.z")])).await,
            Disposition::Unhealthy
        );
        scheduler.mark(id("d.w"), None, Origin::Change).await;
        let d = scheduler.next().await.unwrap();
        assert!(matches!(
            scheduler.finish(d, Outcome::Waiting(Vec::new())).await,
            Disposition::Retrying(_)
        ));
    }

    #[tokio::test]
    async fn a_resource_marked_during_a_waiting_turn_is_not_parked() {
        let (scheduler, _) = scheduler(10, 4, 3);
        scheduler.mark(id("sub.a"), Some(1), Origin::Change).await;
        let a = scheduler.next().await.unwrap();
        scheduler.mark(id("sub.a"), Some(2), Origin::Change).await;
        assert_eq!(
            scheduler
                .finish(a, Outcome::Waiting(vec![id("topic.orders")]))
                .await,
            Disposition::Waiting
        );
        assert_eq!(scheduler.parked().await, 0);
        assert_eq!(
            scheduler.waiting().await,
            1,
            "the resource itself, due at once; the topic is not marked"
        );
        let a = scheduler.next().await.unwrap();
        assert_eq!(a.resource, id("sub.a"));
        assert_eq!(a.covers, [1, 2]);
    }

    #[tokio::test]
    async fn a_full_set_of_parked_resources_cannot_stop_what_they_need() {
        // Two places, both taken by resources that then park on a third:
        // the third is still marked and handed out.
        let (scheduler, _) = scheduler(2, 4, 3);
        scheduler.mark(id("sub.a"), None, Origin::Change).await;
        scheduler.mark(id("sub.b"), None, Origin::Change).await;
        let a = scheduler.next().await.unwrap();
        let b = scheduler.next().await.unwrap();
        scheduler
            .finish(a, Outcome::Waiting(vec![id("topic.t")]))
            .await;
        scheduler
            .finish(b, Outcome::Waiting(vec![id("topic.t")]))
            .await;
        // An ordinary mark of the needed resource merges into its wait, and
        // needs no place either.
        assert_eq!(
            tokio::time::timeout(
                Duration::from_millis(200),
                scheduler.mark(id("topic.t"), None, Origin::Change)
            )
            .await
            .unwrap(),
            Marked::Coalesced
        );
        let topic = tokio::time::timeout(Duration::from_millis(500), scheduler.next())
            .await
            .expect("the needed resource is due without a place")
            .unwrap();
        assert_eq!(topic.resource, id("topic.t"));
        assert_eq!(topic.origin, Origin::Needed, "the first origin is kept");
        scheduler.finish(topic, Outcome::Succeeded).await;
        assert_eq!(scheduler.waiting().await, 2, "both waiters are due");
        assert_eq!(scheduler.places.available_permits(), 0);
    }

    #[tokio::test]
    async fn a_mark_that_waited_for_room_finds_the_resource_parked() {
        let (scheduler, _) = scheduler(1, 4, 3);
        scheduler.mark(id("sub.a"), Some(1), Origin::Change).await;
        let a = scheduler.next().await.unwrap();
        // The mark cannot get a place while the turn holds the only one...
        let marking = tokio::spawn({
            let scheduler = Arc::clone(&scheduler);
            async move { scheduler.mark(id("sub.a"), Some(2), Origin::Change).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // ...and the turn ends by parking, which keeps the place. Only the
        // placeless mark of the topic frees nothing, so the mark is still
        // waiting; a success of the topic releases nothing either. Finish
        // the topic quickly and let the parked resource run to free the
        // place, which lets the mark in to find... the resource waiting.
        scheduler
            .finish(a, Outcome::Waiting(vec![id("topic.t")]))
            .await;
        let topic = scheduler.next().await.unwrap();
        scheduler.finish(topic, Outcome::Succeeded).await;
        let a = scheduler.next().await.unwrap();
        assert_eq!(a.covers, [1]);
        scheduler.finish(a, Outcome::Succeeded).await;
        assert_eq!(marking.await.unwrap(), Marked::New);
        assert_eq!(scheduler.waiting().await, 1);
    }

    #[tokio::test]
    async fn marks_after_closing_are_kept_but_never_handed_out() {
        let (scheduler, _) = scheduler(10, 1, 3);
        scheduler.close().await;
        assert_eq!(
            scheduler.mark(id("server.a"), None, Origin::Change).await,
            Marked::New
        );
        assert!(scheduler.next().await.is_none());
        assert_eq!(scheduler.waiting().await, 1);
    }
}
