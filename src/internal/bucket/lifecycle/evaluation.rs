use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};

use super::constants::{AMZ_EXPIRATION, DISABLED, HTTP_TIME_FORMAT, MINIO_TRANSITION};
use super::error::Error;
use super::model::{Action, Evaluator, Event, Lifecycle, ObjectOpts, Retention};

impl Lifecycle {
    pub fn validate(&self, retention: Retention) -> Result<(), Error> {
        if self.rules.len() > 1000 {
            return Err(Error::LifecycleTooManyRules);
        }
        if self.rules.is_empty() {
            return Err(Error::LifecycleNoRule);
        }
        for rule in &self.rules {
            rule.validate()?;
            if retention.lock_enabled
                && (rule.expiration.delete_all.val || !rule.del_marker_expiration.empty())
            {
                return Err(Error::LifecycleBucketLocked);
            }
        }
        for idx in 0..self.rules.len() {
            for other in &self.rules[idx + 1..] {
                if self.rules[idx].id == other.id && !self.rules[idx].id.is_empty() {
                    return Err(Error::LifecycleDuplicateId);
                }
            }
        }
        Ok(())
    }

    pub fn has_active_rules(&self, prefix: &str) -> bool {
        if self.rules.is_empty() {
            return false;
        }
        let now = Utc::now();
        self.rules.iter().any(|rule| {
            if rule.status == DISABLED {
                return false;
            }
            let rule_prefix = rule.get_prefix();
            if !prefix.is_empty()
                && !rule_prefix.is_empty()
                && !prefix.starts_with(rule_prefix)
                && !rule_prefix.starts_with(prefix)
            {
                return false;
            }
            if rule
                .noncurrent_version_expiration
                .noncurrent_days
                .unwrap_or(0)
                > 0
            {
                return true;
            }
            if rule.noncurrent_version_expiration.newer_noncurrent_versions > 0 {
                return true;
            }
            if !rule.noncurrent_version_transition.is_null() {
                return true;
            }
            if rule.expiration.date.is_some_and(|date| date < now) {
                return true;
            }
            if rule.expiration.days.is_some() {
                return true;
            }
            if rule.expiration.delete_marker.val {
                return true;
            }
            if rule.transition.date.is_some_and(|date| date < now) {
                return true;
            }
            !rule.transition.is_null()
        })
    }

    pub fn filter_rules(&self, obj: &ObjectOpts) -> Vec<super::model::Rule> {
        if obj.name.is_empty() {
            return Vec::new();
        }
        self.rules
            .iter()
            .filter(|rule| rule.status != DISABLED)
            .filter(|rule| obj.name.starts_with(rule.get_prefix()))
            .filter(|rule| rule.filter.test_tags(&obj.user_tags))
            .filter(|rule| obj.delete_marker || rule.filter.by_size(obj.size))
            .cloned()
            .collect()
    }

    pub fn eval(&self, obj: ObjectOpts) -> Event {
        self.eval_at(&obj, Some(Utc::now()), 0)
    }

    pub fn eval_upcoming(&self, obj: &ObjectOpts) -> Event {
        self.eval_at(obj, None, 0)
    }

    fn eval_at(
        &self,
        obj: &ObjectOpts,
        now: Option<DateTime<Utc>>,
        remaining_versions: usize,
    ) -> Event {
        let mod_time = match obj.mod_time {
            Some(mod_time) => mod_time,
            None => return Event::default(),
        };
        let mut events = Vec::new();
        if let Some(restore_expires) = obj.restore_expires {
            if now.is_some_and(|current| current > restore_expires) {
                let action = if obj.is_latest {
                    Action::DeleteRestored
                } else {
                    Action::DeleteRestoredVersion
                };
                events.push(Event {
                    action,
                    due: Some(now.expect("checked above")),
                    ..Event::default()
                });
            }
        }
        for rule in self.filter_rules(obj) {
            if obj.expired_object_delete_marker() {
                if rule.expiration.delete_marker.val {
                    events.push(Event {
                        action: Action::DeleteVersion,
                        rule_id: rule.id.clone(),
                        due: now,
                        ..Event::default()
                    });
                    break;
                }
                if let Some(days) = rule.expiration.days {
                    let due = expected_expiry_time(mod_time, days);
                    if now.is_none_or(|current| current > due) {
                        events.push(Event {
                            action: Action::DeleteVersion,
                            rule_id: rule.id.clone(),
                            due: Some(due),
                            ..Event::default()
                        });
                        break;
                    }
                }
            }

            if obj.is_latest && obj.delete_marker && !rule.del_marker_expiration.empty() {
                if let Some(due) = rule.del_marker_expiration.next_due(obj) {
                    if now.is_none_or(|current| current > due) {
                        events.push(Event {
                            action: Action::DelMarkerDeleteAllVersions,
                            rule_id: rule.id.clone(),
                            due: Some(due),
                            ..Event::default()
                        });
                    }
                }
                continue;
            }

            if !obj.is_latest && rule.noncurrent_version_expiration.set {
                let retained_enough = rule.noncurrent_version_expiration.newer_noncurrent_versions
                    == 0
                    || remaining_versions
                        >= rule.noncurrent_version_expiration.newer_noncurrent_versions as usize;
                let successor = obj.successor_mod_time.unwrap_or(mod_time);
                let due = expected_expiry_time(
                    successor,
                    rule.noncurrent_version_expiration
                        .noncurrent_days
                        .unwrap_or(0),
                );
                let old_enough = now.is_none_or(|current| current > due);
                if retained_enough && old_enough {
                    events.push(Event {
                        action: Action::DeleteVersion,
                        rule_id: rule.id.clone(),
                        due: Some(due),
                        ..Event::default()
                    });
                }
            }

            if !obj.is_latest && !rule.noncurrent_version_transition.is_null() && !obj.delete_marker
            {
                if let Some(due) = rule.noncurrent_version_transition.next_due(obj) {
                    if now.is_none_or(|current| current > due) {
                        events.push(Event {
                            action: Action::TransitionVersion,
                            rule_id: rule.id.clone(),
                            due: Some(due),
                            storage_class: rule.noncurrent_version_transition.storage_class,
                            ..Event::default()
                        });
                    }
                }
            }

            if obj.is_latest && !obj.delete_marker {
                if let Some(date) = rule.expiration.date {
                    if now.is_none_or(|current| current > date) {
                        events.push(Event {
                            action: Action::Delete,
                            rule_id: rule.id.clone(),
                            due: Some(date),
                            ..Event::default()
                        });
                    }
                } else if let Some(days) = rule.expiration.days {
                    let due = expected_expiry_time(mod_time, days);
                    if now.is_none_or(|current| current > due) {
                        let action = if rule.expiration.delete_all.val {
                            Action::DeleteAllVersions
                        } else {
                            Action::Delete
                        };
                        events.push(Event {
                            action,
                            rule_id: rule.id.clone(),
                            due: Some(due),
                            ..Event::default()
                        });
                    }
                }
                if let Some(due) = rule.transition.next_due(obj) {
                    if now.is_none_or(|current| current > due) {
                        events.push(Event {
                            action: Action::Transition,
                            rule_id: rule.id.clone(),
                            due: Some(due),
                            storage_class: rule.transition.storage_class,
                            ..Event::default()
                        });
                    }
                }
            }
        }

        if events.is_empty() {
            return Event::default();
        }
        events.sort_by(|a, b| compare_events(a, b, now));
        events[0].clone()
    }

    pub fn prediction_headers(&self, obj: &ObjectOpts) -> HashMap<String, String> {
        let event = self.eval_upcoming(obj);
        let mut headers = HashMap::new();
        match event.action {
            Action::Delete
            | Action::DeleteVersion
            | Action::DeleteAllVersions
            | Action::DelMarkerDeleteAllVersions => {
                if let Some(due) = event.due {
                    headers.insert(
                        AMZ_EXPIRATION.to_owned(),
                        format!(
                            "expiry-date=\"{}\", rule-id=\"{}\"",
                            due.format(HTTP_TIME_FORMAT),
                            event.rule_id
                        ),
                    );
                }
            }
            Action::Transition | Action::TransitionVersion => {
                if let Some(due) = event.due {
                    headers.insert(
                        MINIO_TRANSITION.to_owned(),
                        format!(
                            "transition-date=\"{}\", rule-id=\"{}\"",
                            due.format(HTTP_TIME_FORMAT),
                            event.rule_id
                        ),
                    );
                }
            }
            _ => {}
        }
        headers
    }

    pub fn noncurrent_versions_expiration_limit(&self, obj: &ObjectOpts) -> Event {
        for rule in self.filter_rules(obj) {
            if rule.noncurrent_version_expiration.newer_noncurrent_versions == 0 {
                continue;
            }
            return Event {
                action: Action::DeleteVersion,
                rule_id: rule.id,
                noncurrent_days: rule
                    .noncurrent_version_expiration
                    .noncurrent_days
                    .unwrap_or(0),
                newer_noncurrent_versions: rule
                    .noncurrent_version_expiration
                    .newer_noncurrent_versions,
                ..Event::default()
            };
        }
        Event::default()
    }
}

impl Evaluator {
    pub fn eval(&self, objs: &[ObjectOpts], now: DateTime<Utc>) -> Vec<Event> {
        let mut events = vec![Event::default(); objs.len()];
        let mut newer_noncurrent_versions = 0usize;
        let mut index = 0usize;
        while index < objs.len() {
            let obj = &objs[index];
            let event = self
                .policy
                .eval_at(obj, Some(now), newer_noncurrent_versions);
            let keep_event = event.clone();
            match event.action {
                Action::DeleteAllVersions | Action::DelMarkerDeleteAllVersions => {
                    events[index] = keep_event;
                    break;
                }
                _ => {
                    if !obj.is_latest && keep_event.action != Action::DeleteVersion {
                        newer_noncurrent_versions += 1;
                    }
                    events[index] = keep_event;
                }
            }
            index += 1;
        }
        events
    }

    pub fn evaluate(&self, objs: &[ObjectOpts]) -> Result<Vec<Event>, Error> {
        if objs.is_empty() {
            return Ok(Vec::new());
        }
        if objs.len() != objs[0].num_versions {
            return Err(Error::Parse(format!(
                "number of versions mismatch, expected {}, got {}",
                objs[0].num_versions,
                objs.len()
            )));
        }
        Ok(self.eval(objs, Utc::now()))
    }
}

pub fn expected_expiry_time(mod_time: DateTime<Utc>, days: i32) -> DateTime<Utc> {
    if days == 0 {
        return mod_time;
    }
    let next = mod_time + Duration::days(i64::from(days + 1));
    Utc.with_ymd_and_hms(next.year(), next.month(), next.day(), 0, 0, 0)
        .single()
        .expect("valid UTC midnight")
}

fn compare_events(a: &Event, b: &Event, now: Option<DateTime<Utc>>) -> Ordering {
    let a_due = a.due.unwrap_or(DateTime::<Utc>::MIN_UTC);
    let b_due = b.due.unwrap_or(DateTime::<Utc>::MIN_UTC);
    let both_due = now.is_some_and(|current| current > a_due && current > b_due) || a_due == b_due;
    if both_due {
        let a_delete = is_delete_action(a.action);
        let b_delete = is_delete_action(b.action);
        return match (a_delete, b_delete) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => Ordering::Less,
        };
    }
    if a_due < b_due {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

fn is_delete_action(action: Action) -> bool {
    matches!(
        action,
        Action::Delete
            | Action::DeleteVersion
            | Action::DeleteAllVersions
            | Action::DelMarkerDeleteAllVersions
    )
}
