//! Simple topic-level ACL checks for produce, fetch, and admin operations.

use std::collections::HashSet;
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    Produce,
    Fetch,
    Admin,
}

#[derive(Debug, Clone, Default)]
pub struct Acl {
    allowed: HashSet<(String, Operation, u16)>,
    deny_by_default: bool,
}

impl Acl {
    pub fn from_env() -> Self {
        let mut acl = Self {
            deny_by_default: std::env::var("DMQ_ACL_DENY_BY_DEFAULT")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            allowed: HashSet::new(),
        };
        if let Ok(raw) = std::env::var("DMQ_ACL") {
            for rule in raw.split(';').filter(|s| !s.is_empty()) {
                acl.parse_rule(rule);
            }
        }
        acl
    }

    fn parse_rule(&mut self, rule: &str) {
        // format: principal:operation:topic_id  e.g. admin:produce:1
        let parts: Vec<&str> = rule.split(':').collect();
        if parts.len() != 3 {
            return;
        }
        let principal = parts[0].to_string();
        let op = match parts[1].to_ascii_lowercase().as_str() {
            "produce" => Operation::Produce,
            "fetch" => Operation::Fetch,
            "admin" => Operation::Admin,
            _ => return,
        };
        let topic_id = parts[2].parse().unwrap_or(0);
        self.allowed.insert((principal, op, topic_id));
    }

    pub fn check(&self, principal: &str, op: Operation, topic_id: u16) -> io::Result<()> {
        if !self.is_configured() {
            return Ok(());
        }
        if self.allowed.contains(&(principal.to_string(), op, topic_id))
            || self.allowed.contains(&(principal.to_string(), op, 0))
            || self.allowed.contains(&("*".to_string(), op, topic_id))
            || self.allowed.contains(&("*".to_string(), op, 0))
        {
            return Ok(());
        }
        if self.deny_by_default {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "acl denied",
            ));
        }
        Ok(())
    }

    pub fn is_configured(&self) -> bool {
        self.deny_by_default || !self.allowed.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_allow_permits_operation() {
        let mut acl = Acl::default();
        acl.allowed
            .insert(("alice".to_string(), Operation::Produce, 1));
        assert!(acl.check("alice", Operation::Produce, 1).is_ok());
        assert!(acl.check("bob", Operation::Produce, 1).is_err() == false);
    }

    #[test]
    fn deny_by_default_blocks_unknown_principals() {
        let acl = Acl {
            deny_by_default: true,
            allowed: HashSet::new(),
        };
        assert!(acl.check("alice", Operation::Fetch, 1).is_err());
    }
}
