//! Consumer group coordinator: join, heartbeat, and range partition assignment.

use std::collections::HashMap;
use std::io;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub member_id: u64,
    pub generation: u32,
    pub assigned_partitions: Vec<u16>,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupState {
    pub group_id: u16,
    pub topic_id: u16,
    pub generation: u32,
    pub members: HashMap<u64, GroupMember>,
    pub rebalance_pending: bool,
}

pub fn session_timeout_ms() -> u64 {
    std::env::var("DMQ_GROUP_SESSION_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15_000)
}

impl GroupState {
    pub fn new(group_id: u16, topic_id: u16) -> Self {
        Self {
            group_id,
            topic_id,
            generation: 1,
            members: HashMap::new(),
            rebalance_pending: false,
        }
    }

    pub fn join(
        &mut self,
        member_id: u64,
        topic_partition_count: u16,
        now_ms: u64,
    ) -> io::Result<(u64, u32, Vec<u16>)> {
        let assigned_id = if member_id == 0 {
            next_member_id(&self.members)
        } else {
            member_id
        };
        self.generation = self.generation.saturating_add(1);
        self.rebalance_pending = true;
        self.members.insert(
            assigned_id,
            GroupMember {
                member_id: assigned_id,
                generation: self.generation,
                assigned_partitions: Vec::new(),
                last_seen_ms: now_ms,
            },
        );
        let assignments = range_assign(topic_partition_count, &self.members, self.generation);
        for (member_id, parts) in &assignments {
            if let Some(member) = self.members.get_mut(member_id) {
                member.assigned_partitions = parts.clone();
                member.generation = self.generation;
                member.last_seen_ms = now_ms;
            }
        }
        self.rebalance_pending = false;
        let parts = assignments
            .get(&assigned_id)
            .cloned()
            .unwrap_or_default();
        Ok((assigned_id, self.generation, parts))
    }

    pub fn heartbeat(
        &mut self,
        member_id: u64,
        generation: u32,
        now_ms: u64,
    ) -> io::Result<(u8, bool)> {
        let Some(member) = self.members.get_mut(&member_id) else {
            return Ok((1, true));
        };
        if member.generation != generation {
            return Ok((2, true));
        }
        member.last_seen_ms = now_ms;
        Ok((0, self.rebalance_pending))
    }

    pub fn expire_stale_members(&mut self, now_ms: u64, timeout_ms: u64) -> bool {
        let stale: Vec<u64> = self
            .members
            .iter()
            .filter(|(_, m)| now_ms.saturating_sub(m.last_seen_ms) > timeout_ms)
            .map(|(id, _)| *id)
            .collect();
        if stale.is_empty() {
            return false;
        }
        for id in stale {
            self.members.remove(&id);
        }
        self.generation = self.generation.saturating_add(1);
        self.rebalance_pending = true;
        true
    }

    pub fn rebalance(
        &mut self,
        topic_partition_count: u16,
        now_ms: u64,
    ) -> HashMap<u64, Vec<u16>> {
        let assignments = range_assign(topic_partition_count, &self.members, self.generation);
        for (member_id, parts) in &assignments {
            if let Some(member) = self.members.get_mut(member_id) {
                member.assigned_partitions = parts.clone();
                member.last_seen_ms = now_ms;
            }
        }
        self.rebalance_pending = false;
        assignments
    }
}

/// Range assigner: divide contiguous partition ranges across sorted member ids.
pub fn range_assign(
    partition_count: u16,
    members: &HashMap<u64, GroupMember>,
    generation: u32,
) -> HashMap<u64, Vec<u16>> {
    let mut ids: Vec<u64> = members
        .values()
        .filter(|m| m.generation == generation || m.assigned_partitions.is_empty())
        .map(|m| m.member_id)
        .collect();
    if ids.is_empty() {
        ids = members.keys().copied().collect();
    }
    ids.sort_unstable();
    ids.dedup();
    let n = ids.len();
    if n == 0 || partition_count == 0 {
        return HashMap::new();
    }
    let base = partition_count as usize / n;
    let extra = partition_count as usize % n;
    let mut out = HashMap::new();
    let mut partition = 0u16;
    for (idx, member_id) in ids.iter().enumerate() {
        let count = base + if idx < extra { 1 } else { 0 };
        let mut parts = Vec::with_capacity(count);
        for _ in 0..count {
            parts.push(partition);
            partition = partition.saturating_add(1);
        }
        out.insert(*member_id, parts);
    }
    out
}

fn next_member_id(members: &HashMap<u64, GroupMember>) -> u64 {
    let mut id = 1u64;
    while members.contains_key(&id) {
        id += 1;
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_assign_splits_evenly() {
        let mut members = HashMap::new();
        members.insert(
            1,
            GroupMember {
                member_id: 1,
                generation: 1,
                assigned_partitions: vec![],
                last_seen_ms: 0,
            },
        );
        members.insert(
            2,
            GroupMember {
                member_id: 2,
                generation: 1,
                assigned_partitions: vec![],
                last_seen_ms: 0,
            },
        );
        let assigned = range_assign(4, &members, 1);
        assert_eq!(assigned.get(&1).map(|v| v.len()), Some(2));
        assert_eq!(assigned.get(&2).map(|v| v.len()), Some(2));
        let all: std::collections::HashSet<u16> = assigned.values().flatten().copied().collect();
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn join_assigns_member() {
        let mut group = GroupState::new(1, 1);
        let (id, gen, parts) = group.join(0, 2, 100).unwrap();
        assert!(id > 0);
        assert_eq!(gen, 2);
        assert!(!parts.is_empty());
    }

    #[test]
    fn stale_member_triggers_rebalance_flag() {
        let mut group = GroupState::new(1, 1);
        group.join(1, 2, 100).unwrap();
        assert!(group.expire_stale_members(120_000, 10_000));
        assert!(group.members.is_empty());
    }
}
