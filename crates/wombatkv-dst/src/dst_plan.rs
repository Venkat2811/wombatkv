//! Plan-aware fault dispatch (Stage 3.5).
//!
//! Sibling primitive to [`crate::dst_buggify!`]. Where `dst_buggify!` does
//! deterministic per-callsite rolls based on seed+file+line, this
//! module consults a LOADED `FaultPlan` and fires the specific
//! event scheduled for the current op. This is what makes the DST
//! runner's fault classes (`S3PutFailure`, `WireBadEnvelope`, etc.)
//! actually fire in production code paths at the right ops, rather
//! than rolling dice.
//!
//! # Usage from production code
//!
//! ```ignore
//! use wombatkv_dst::dst_plan;
//! use wombatkv_dst::WombatKvFaultEvent;
//!
//! // Inside a put-kv code path:
//! #[cfg(feature = "dst")]
//! if let Some(ev) = dst_plan::fire_for_current_op() {
//!     if matches!(ev, WombatKvFaultEvent::S3PutFailure { .. }
//!                  | WombatKvFaultEvent::S3PutLatency { .. }) {
//!         return Err(/* simulate the fault */);
//!     }
//! }
//! ```
//!
//! # Usage from the DST runner
//!
//! ```ignore
//! dst_plan::set_plan(loaded_plan);
//! for op_n in 0..N {
//!     // production code under test calls fire_for_current_op() internally
//!     run_one_op();
//!     dst_plan::advance_op();
//! }
//! ```
//!
//! # Why a separate module
//!
//! `dst_buggify` answers "should chaos fire here at all?" via probabilistic
//! per-callsite roll. `dst_plan` answers "what specific scheduled event
//! should fire at this op?" via plan lookup. Both can coexist, buggify
//! is for general chaos exploration, plan is for reproducing a specific
//! fault scenario.

use std::sync::{Mutex, OnceLock};

use crate::{FaultPlan, FaultTrigger, WombatKvFaultEvent};

struct PlanState {
    plan: Option<FaultPlan>,
    op_counter: u32,
}

fn state() -> &'static Mutex<PlanState> {
    static STATE: OnceLock<Mutex<PlanState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(PlanState { plan: None, op_counter: 0 }))
}

/// Load a `FaultPlan` for the current run. Replaces any prior plan
/// and resets the op counter to 0. Call once at the start of a DST
/// run before the production code under test starts driving ops.
pub fn set_plan(plan: FaultPlan) {
    let mut s = state().lock().expect("dst_plan state poisoned");
    s.plan = Some(plan);
    s.op_counter = 0;
}

/// Clear the loaded plan and reset the op counter. Use between runs
/// in the same process (e.g., between seeds in a sweep test) to
/// avoid cross-run state leak.
pub fn clear() {
    let mut s = state().lock().expect("dst_plan state poisoned");
    s.plan = None;
    s.op_counter = 0;
}

/// Increment the global op counter. The runner calls this AFTER
/// each op completes (success or fault), so the NEXT op consults the
/// new counter value.
pub fn advance_op() {
    let mut s = state().lock().expect("dst_plan state poisoned");
    s.op_counter = s.op_counter.saturating_add(1);
}

/// Current op counter value. Useful for tests + reporting.
#[must_use]
pub fn current_op() -> u32 {
    let s = state().lock().expect("dst_plan state poisoned");
    s.op_counter
}

/// If a fault event in the loaded plan triggers at the current op,
/// return a clone of it. Otherwise return `None`.
///
/// "Triggers at the current op" means: the event's `FaultTrigger` is
/// `AfterKvOp { n: current_op + 1 }` (one-based count matching the
/// schedule generator's convention).
///
/// Returns a CLONE so callers can drop the global lock before acting
/// on the event.
#[must_use]
pub fn fire_for_current_op() -> Option<WombatKvFaultEvent> {
    let s = state().lock().expect("dst_plan state poisoned");
    let plan = s.plan.as_ref()?;
    let op_n = s.op_counter;
    for sched in &plan.events {
        if let FaultTrigger::AfterKvOp { n } = sched.trigger {
            if n == op_n + 1 {
                return Some(sched.event.clone());
            }
        }
    }
    None
}

/// Is there a put-suppressing fault scheduled for the current op?
/// Convenience wrapper used by production put paths.
#[must_use]
pub fn is_put_suppressed() -> bool {
    matches!(
        fire_for_current_op(),
        Some(
            WombatKvFaultEvent::S3PutFailure { .. }
                | WombatKvFaultEvent::S3PutLatency { .. }
                | WombatKvFaultEvent::KillBeforeChainHead
                | WombatKvFaultEvent::KillBeforeSidecar
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule_for_class;
    use crate::WombatKvFailureClass;

    #[test]
    fn no_plan_means_no_fire() {
        clear();
        assert!(fire_for_current_op().is_none());
        assert!(!is_put_suppressed());
    }

    #[test]
    fn plan_with_op_trigger_fires_at_right_op() {
        // Use a class whose schedule emits AfterKvOp triggers.
        let plan = schedule_for_class(42, WombatKvFailureClass::ConcurrentSameKeySave);
        // Capture the op-numbers where events fire (1-based per schedule
        // generator convention).
        let trigger_ops: Vec<u32> =
            plan.events
                .iter()
                .filter_map(|s| {
                    if let FaultTrigger::AfterKvOp { n } = s.trigger {
                        Some(n)
                    } else {
                        None
                    }
                })
                .collect();
        assert!(!trigger_ops.is_empty(), "expected at least one AfterKvOp trigger");

        set_plan(plan);

        // Walk through ops; at each op N, check if an event fires.
        // We expect fires at op (n-1) for each trigger n (0-based op
        // counter, 1-based schedule n).
        let mut fires: Vec<u32> = Vec::new();
        for _op_n in 0..20u32 {
            if fire_for_current_op().is_some() {
                fires.push(current_op());
            }
            advance_op();
        }
        // Expected fires = trigger_ops shifted by -1 (1-based → 0-based)
        // AND sorted ascending (we collect `fires` in op-counter order).
        let mut expected: Vec<u32> = trigger_ops.iter().map(|n| n - 1).collect();
        expected.sort_unstable();
        assert_eq!(fires, expected, "plan fires at wrong ops");

        clear();
    }

    #[test]
    fn is_put_suppressed_matches_put_class_events() {
        // ConcurrentSameKeySave uses S3PutLatency, put-suppressing.
        let plan = schedule_for_class(7, WombatKvFailureClass::ConcurrentSameKeySave);
        set_plan(plan);
        let mut suppress_count = 0;
        for _ in 0..20u32 {
            if is_put_suppressed() {
                suppress_count += 1;
            }
            advance_op();
        }
        assert!(suppress_count >= 1, "expected put-suppressing fires");
        clear();
    }
}
