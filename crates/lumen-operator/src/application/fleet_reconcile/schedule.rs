//! When the fleet loop converges, and which fleets it converges: a pure
//! scheduler over watch events and a clock the caller passes in. No cluster,
//! no async types.

use std::collections::BTreeSet;
use std::time::Duration;

/// How often a full repair pass runs while leader, to catch a missed event.
pub const FLEET_REPAIR_INTERVAL: Duration = Duration::from_secs(600);

/// One change seen by a watch. The `Option` is a child's `lumen.dev/fleet`
/// label value; `None` means the child is not a fleet child.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetEvent {
    FleetChanged(String),
    FleetDeleted(String),
    ChildChanged(Option<String>),
    ChildDeleted(Option<String>),
}

/// Work for one pass: the fleets to converge, and whether to converge all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Batch {
    pub fleets: BTreeSet<String>,
    pub full_repair: bool,
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        self.fleets.is_empty() && !self.full_repair
    }
}

/// Decides what to converge. `now` is the time since process start.
#[derive(Debug)]
pub struct Scheduler {
    repair_every: Option<Duration>,
    leader: bool,
    pending: BTreeSet<String>,
    repair_due: Option<Duration>,
}

impl Scheduler {
    pub fn new(repair_every: Option<Duration>) -> Self {
        Self {
            repair_every,
            leader: false,
            pending: BTreeSet::new(),
            repair_due: None,
        }
    }

    /// Gaining leadership schedules one full repair at once. Losing it drops
    /// queued work: the new leader repairs everything anyway.
    pub fn set_leader(&mut self, leader: bool, now: Duration) {
        if leader == self.leader {
            return;
        }
        self.leader = leader;
        if leader {
            self.repair_due = Some(now);
        } else {
            self.pending.clear();
            self.repair_due = None;
        }
    }

    /// Events seen while not leader are dropped, not queued.
    pub fn observe(&mut self, event: FleetEvent) {
        if !self.leader {
            return;
        }
        match event {
            FleetEvent::FleetChanged(name)
            | FleetEvent::FleetDeleted(name)
            | FleetEvent::ChildChanged(Some(name))
            | FleetEvent::ChildDeleted(Some(name)) => {
                self.pending.insert(name);
            }
            FleetEvent::ChildChanged(None) | FleetEvent::ChildDeleted(None) => {}
        }
    }

    /// Take the queued fleets. A full repair is due when its time has come;
    /// the next one is scheduled `repair_every` later, or never when it is off.
    pub fn poll(&mut self, now: Duration) -> Batch {
        if !self.leader {
            return Batch::default();
        }
        let fleets = std::mem::take(&mut self.pending);
        let full_repair = matches!(self.repair_due, Some(due) if now >= due);
        if full_repair {
            self.repair_due = self.repair_every.map(|every| now + every);
        }
        Batch {
            fleets,
            full_repair,
        }
    }
}
