//! The scheduler's races: a mark that waited for room and finds that
//! things moved on meanwhile, a resource given up on while a mark was
//! kept for it, and a wait that reaches the same resource twice.

use std::sync::{Arc, Mutex};

use morph::ResourceId;
use morph_controller_core::config::RetryConfig;
use morph_controller_core::scheduler::{Disposition, Marked, Origin, Outcome, Scheduler};

fn id(text: &str) -> ResourceId {
    text.parse().unwrap()
}

fn scheduler(
    capacity: usize,
    workers: usize,
    max_attempts: u32,
) -> (Arc<Scheduler>, Arc<Mutex<Vec<u64>>>) {
    let settled = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&settled);
    let retry: RetryConfig = toml::from_str(&format!(
        "max_attempts = {max_attempts}\ninitial = \"1ms\"\nmax = \"5ms\"\nmultiplier = 2.0"
    ))
    .unwrap();
    let scheduler = Scheduler::new(capacity, workers, retry, move |number| {
        recorder.lock().unwrap().push(number);
    });
    (Arc::new(scheduler), settled)
}

fn settled(numbers: &Arc<Mutex<Vec<u64>>>) -> Vec<u64> {
    let mut numbers = numbers.lock().unwrap().clone();
    numbers.sort_unstable();
    numbers
}

/// Lets the tasks spawned so far run until they wait.
async fn settle_tasks() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_mark_that_waited_for_room_finds_its_resource_waiting_and_merges_into_it() {
    let (scheduler, numbers) = scheduler(2, 5, 3);
    let (a, b, c) = (id("vm.a"), id("vm.b"), id("vm.c"));
    assert_eq!(
        scheduler.mark(a.clone(), Some(1), Origin::Change).await,
        Marked::New
    );
    assert_eq!(
        scheduler.mark(c.clone(), Some(2), Origin::Change).await,
        Marked::New
    );
    // The set is full: a mark of B waits for a place.
    let pending = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        let b = b.clone();
        async move { scheduler.mark(b, Some(7), Origin::Change).await }
    });
    settle_tasks().await;
    assert!(!pending.is_finished(), "no place is free");
    // A waits for B, which makes B wait without a place of its own. A
    // keeps its place while parked, so the mark still waits.
    let lease = scheduler.next().await.unwrap();
    assert_eq!(lease.resource, a);
    assert_eq!(
        scheduler
            .finish(lease, Outcome::Waiting(vec![b.clone()]))
            .await,
        Disposition::Waiting
    );
    settle_tasks().await;
    assert!(!pending.is_finished(), "a parked resource keeps its place");
    assert_eq!(scheduler.waiting().await, 2, "C and B");
    // C ends and gives its place back. The mark takes it, looks again,
    // finds B waiting, and merges into it.
    let lease = scheduler.next().await.unwrap();
    assert_eq!(lease.resource, c);
    assert_eq!(
        scheduler.finish(lease, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(pending.await.unwrap(), Marked::Coalesced);
    assert_eq!(scheduler.waiting().await, 1, "B, with the merged mark");
    // B's turn settles the merged mark's transaction, and wakes A.
    let lease = scheduler.next().await.unwrap();
    assert_eq!(lease.resource, b);
    assert_eq!(
        lease.origin,
        Origin::Needed,
        "the first mark's origin is kept"
    );
    assert_eq!(
        scheduler.finish(lease, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(settled(&numbers), vec![2, 7]);
    let lease = scheduler.next().await.unwrap();
    assert_eq!(lease.resource, a);
    assert_eq!(
        scheduler.finish(lease, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(settled(&numbers), vec![1, 2, 7]);
    assert_eq!(scheduler.waiting().await, 0);
    assert_eq!(scheduler.parked().await, 0);
}

#[tokio::test]
async fn a_mark_that_waited_for_room_finds_its_resource_parked_and_wakes_it() {
    let (scheduler, numbers) = scheduler(2, 5, 3);
    let (a, c, x, y) = (id("vm.a"), id("vm.c"), id("vm.x"), id("vm.y"));
    assert_eq!(
        scheduler.mark(a.clone(), Some(1), Origin::Change).await,
        Marked::New
    );
    assert_eq!(
        scheduler.mark(c.clone(), Some(2), Origin::Change).await,
        Marked::New
    );
    let pending = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        let x = x.clone();
        async move { scheduler.mark(x, Some(9), Origin::Change).await }
    });
    settle_tasks().await;
    assert!(!pending.is_finished());
    // Both turns begin. A waits for X, so X is marked as needed, has its
    // turn, and parks waiting for Y.
    let lease_a = scheduler.next().await.unwrap();
    assert_eq!(lease_a.resource, a);
    let lease_c = scheduler.next().await.unwrap();
    assert_eq!(lease_c.resource, c);
    assert_eq!(
        scheduler
            .finish(lease_a, Outcome::Waiting(vec![x.clone()]))
            .await,
        Disposition::Waiting
    );
    let lease_x = scheduler.next().await.unwrap();
    assert_eq!(lease_x.resource, x);
    assert_eq!(
        scheduler
            .finish(lease_x, Outcome::Waiting(vec![y.clone()]))
            .await,
        Disposition::Waiting
    );
    settle_tasks().await;
    assert!(!pending.is_finished(), "A's place is still held");
    assert_eq!(
        scheduler.parked_on().await,
        vec![(a.clone(), vec![x.clone()]), (x.clone(), vec![y.clone()])]
    );
    // C ends and gives its place back. The mark takes it, finds X parked,
    // and wakes it: X waits again at once, covering the mark's transaction
    // too, and the place just taken is given back.
    assert_eq!(
        scheduler.finish(lease_c, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(pending.await.unwrap(), Marked::Coalesced);
    assert_eq!(
        scheduler.parked_on().await,
        vec![(a.clone(), vec![x.clone()])]
    );
    assert_eq!(scheduler.waiting().await, 2, "X and Y");
    let lease = scheduler.next().await.unwrap();
    assert_eq!(lease.resource, x, "X began to wait before Y");
    assert_eq!(
        scheduler.finish(lease, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(settled(&numbers), vec![2, 9]);
    assert_eq!(scheduler.parked().await, 0, "A is woken by X");
    // The place given back can be taken: a third resource fits beside A
    // and Y.
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            scheduler.mark(id("vm.z"), None, Origin::Change)
        )
        .await
        .expect("a place is free"),
        Marked::New
    );
}

#[tokio::test]
async fn a_resource_given_up_on_that_was_marked_during_its_last_turn_waits_again() {
    let (scheduler, numbers) = scheduler(5, 2, 1);
    let a = id("vm.a");
    assert_eq!(
        scheduler.mark(a.clone(), Some(1), Origin::Change).await,
        Marked::New
    );
    let lease = scheduler.next().await.unwrap();
    assert_eq!(
        scheduler.mark(a.clone(), Some(2), Origin::Change).await,
        Marked::Coalesced
    );
    // The one attempt allowed fails: the resource is unhealthy, the
    // transaction that marked it is settled, and the mark kept during
    // the turn makes it wait again, counted afresh.
    assert_eq!(
        scheduler.finish(lease, Outcome::Failed("no".into())).await,
        Disposition::Unhealthy
    );
    assert_eq!(settled(&numbers), vec![1]);
    assert_eq!(scheduler.unhealthy().await, vec![a.clone()]);
    assert_eq!(scheduler.waiting().await, 1);
    let lease = scheduler.next().await.unwrap();
    assert_eq!((lease.resource.clone(), lease.attempt), (a.clone(), 1));
    assert_eq!(
        scheduler.finish(lease, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(settled(&numbers), vec![1, 2]);
    assert!(scheduler.unhealthy().await.is_empty());
}

#[tokio::test]
async fn a_wait_that_reaches_a_resource_by_two_paths_looks_at_it_once_and_still_finds_a_circle() {
    let (scheduler, _) = scheduler(10, 6, 3);
    let names = ["vm.c", "vm.a", "vm.b", "vm.p", "vm.d"];
    for name in names {
        assert_eq!(
            scheduler.mark(id(name), None, Origin::Change).await,
            Marked::New
        );
    }
    let mut leases = Vec::new();
    for name in names {
        let lease = scheduler.next().await.unwrap();
        assert_eq!(lease.resource, id(name));
        leases.push(lease);
    }
    let [c, a, b, p, d] = <[_; 5]>::try_from(leases).unwrap();
    // A diamond: P waits for A and B, which both wait for C, which waits
    // for Z. D then waits for P, and the check for a circle walks the
    // diamond, reaching C twice.
    assert_eq!(
        scheduler
            .finish(c, Outcome::Waiting(vec![id("vm.z")]))
            .await,
        Disposition::Waiting
    );
    assert_eq!(
        scheduler
            .finish(a, Outcome::Waiting(vec![id("vm.c")]))
            .await,
        Disposition::Waiting
    );
    assert_eq!(
        scheduler
            .finish(b, Outcome::Waiting(vec![id("vm.c")]))
            .await,
        Disposition::Waiting
    );
    assert_eq!(
        scheduler
            .finish(p, Outcome::Waiting(vec![id("vm.a"), id("vm.b")]))
            .await,
        Disposition::Waiting
    );
    assert_eq!(
        scheduler
            .finish(d, Outcome::Waiting(vec![id("vm.p")]))
            .await,
        Disposition::Waiting
    );
    assert_eq!(scheduler.parked().await, 5);
    // Z, which everything waits for through the diamond, may not wait for
    // D: that would close the circle, and is counted as a failure.
    let z = scheduler.next().await.unwrap();
    assert_eq!(z.resource, id("vm.z"));
    assert_eq!(z.origin, Origin::Needed);
    assert!(matches!(
        scheduler
            .finish(z, Outcome::Waiting(vec![id("vm.d")]))
            .await,
        Disposition::Retrying(_)
    ));
    assert_eq!(scheduler.parked().await, 5);
}

#[tokio::test]
async fn two_marks_that_waited_for_room_find_their_resource_in_a_turn_and_are_kept_for_after_it() {
    let (scheduler, numbers) = scheduler(3, 5, 3);
    let (a, c, d, x) = (id("vm.a"), id("vm.c"), id("vm.d"), id("vm.x"));
    for (resource, number) in [(&a, 1), (&c, 2), (&d, 3)] {
        assert_eq!(
            scheduler
                .mark(resource.clone(), Some(number), Origin::Change)
                .await,
            Marked::New
        );
    }
    // The set is full: two marks of X wait for places.
    let first = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        let x = x.clone();
        async move { scheduler.mark(x, Some(7), Origin::Change).await }
    });
    settle_tasks().await;
    let second = tokio::spawn({
        let scheduler = Arc::clone(&scheduler);
        let x = x.clone();
        async move { scheduler.mark(x, Some(8), Origin::Propagated).await }
    });
    settle_tasks().await;
    assert!(!first.is_finished() && !second.is_finished());
    // A waits for X, which is marked as needed and begins its turn.
    let lease_a = scheduler.next().await.unwrap();
    let lease_c = scheduler.next().await.unwrap();
    let lease_d = scheduler.next().await.unwrap();
    assert_eq!(
        scheduler
            .finish(lease_a, Outcome::Waiting(vec![x.clone()]))
            .await,
        Disposition::Waiting
    );
    let lease_x = scheduler.next().await.unwrap();
    assert_eq!(lease_x.resource, x);
    // One place frees: the first mark finds X in a turn and is kept for
    // after it. Another frees: the second finds the first kept, and joins
    // it.
    assert_eq!(
        scheduler.finish(lease_c, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(first.await.unwrap(), Marked::Coalesced);
    settle_tasks().await;
    assert!(!second.is_finished(), "the second still waits for a place");
    assert_eq!(
        scheduler.finish(lease_d, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(second.await.unwrap(), Marked::Coalesced);
    // X's turn ends: A is woken, and X waits again at once with both
    // marks. A has waited longer, so it goes first. X's next turn settles
    // both marks' transactions.
    assert_eq!(
        scheduler.finish(lease_x, Outcome::Succeeded).await,
        Disposition::DoneAndMarkedAgain
    );
    assert_eq!(scheduler.parked().await, 0, "A is woken");
    let lease_a = scheduler.next().await.unwrap();
    assert_eq!(lease_a.resource, a);
    let lease_x = scheduler.next().await.unwrap();
    assert_eq!(lease_x.resource, x);
    assert_eq!(
        lease_x.origin,
        Origin::Change,
        "the first kept mark's origin"
    );
    assert_eq!(
        scheduler.finish(lease_a, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(settled(&numbers), vec![1, 2, 3]);
    assert_eq!(
        scheduler.finish(lease_x, Outcome::Succeeded).await,
        Disposition::Done
    );
    assert_eq!(settled(&numbers), vec![1, 2, 3, 7, 8]);
    assert_eq!(scheduler.waiting().await, 0);
}
