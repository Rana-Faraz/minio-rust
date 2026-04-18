use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use roxmltree::{Document, Node};

use super::error::Error;
use super::model::{
    And, Boolean, DelMarkerExpiration, Expiration, Filter, Lifecycle, NoncurrentVersionExpiration,
    NoncurrentVersionTransition, Prefix, Rule, Tag, Transition,
};

impl Lifecycle {
    pub fn to_xml(&self) -> String {
        let mut xml = String::from("<LifecycleConfiguration>");
        for rule in &self.rules {
            rule.write_xml(&mut xml);
        }
        if let Some(expiry_updated_at) = self.expiry_updated_at {
            xml.push_str(&format!(
                "<ExpiryUpdatedAt>{}</ExpiryUpdatedAt>",
                expiry_updated_at.to_rfc3339_opts(SecondsFormat::Secs, true)
            ));
        }
        xml.push_str("</LifecycleConfiguration>");
        xml
    }
}

pub fn parse_lifecycle_config(xml: &str) -> Result<Lifecycle, Error> {
    let doc = parse_document(xml)?;
    let root = doc.root_element();
    match root.tag_name().name() {
        "LifecycleConfiguration" | "BucketLifecycleConfiguration" => {}
        _ => {
            return Err(Error::Parse(format!(
                "expected element type <LifecycleConfiguration>/<BucketLifecycleConfiguration> but have <{}>",
                root.tag_name().name()
            )));
        }
    }
    let mut lifecycle = Lifecycle::default();
    for child in element_children(root) {
        match child.tag_name().name() {
            "Rule" => lifecycle.rules.push(parse_rule(child)?),
            "ExpiryUpdatedAt" => {
                lifecycle.expiry_updated_at = Some(parse_rfc3339_datetime(text(child)?)?);
            }
            _ => {
                return Err(Error::Parse(format!(
                    "expected element type <Rule> but have <{}>",
                    child.tag_name().name()
                )));
            }
        }
    }
    Ok(lifecycle)
}

pub fn parse_lifecycle_config_with_id(xml: &str) -> Result<Lifecycle, Error> {
    let mut lifecycle = parse_lifecycle_config(xml)?;
    let mut generated = 0usize;
    for rule in &mut lifecycle.rules {
        if rule.id.is_empty() {
            generated += 1;
            rule.id = format!("generated-rule-{generated}");
        }
    }
    Ok(lifecycle)
}

pub fn parse_noncurrent_version_expiration(
    xml: &str,
) -> Result<NoncurrentVersionExpiration, Error> {
    let doc = parse_document(xml)?;
    parse_noncurrent_version_expiration_node(doc.root_element())
}

fn parse_document(xml: &str) -> Result<Document<'_>, Error> {
    Document::parse(xml).map_err(|error: roxmltree::Error| Error::Parse(error.to_string()))
}

fn parse_rule(node: Node<'_, '_>) -> Result<Rule, Error> {
    let mut rule = Rule::default();
    for child in element_children(node) {
        match child.tag_name().name() {
            "ID" => rule.id = text(child)?.to_owned(),
            "Status" => rule.status = text(child)?.to_owned(),
            "Filter" => rule.filter = parse_filter(child)?,
            "Prefix" => rule.prefix = Prefix::new(text(child)?),
            "Expiration" => rule.expiration = parse_expiration(child)?,
            "Transition" => rule.transition = parse_transition(child)?,
            "DelMarkerExpiration" => {
                rule.del_marker_expiration = parse_del_marker_expiration(child)?
            }
            "NoncurrentVersionExpiration" => {
                rule.noncurrent_version_expiration =
                    parse_noncurrent_version_expiration_node(child)?
            }
            "NoncurrentVersionTransition" => {
                rule.noncurrent_version_transition = parse_noncurrent_version_transition(child)?
            }
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    Ok(rule)
}

fn parse_filter(node: Node<'_, '_>) -> Result<Filter, Error> {
    let mut filter = Filter {
        set: true,
        ..Filter::default()
    };
    for child in element_children(node) {
        match child.tag_name().name() {
            "Prefix" => filter.prefix = Prefix::new(text(child)?),
            "And" => filter.and = parse_and(child)?,
            "Tag" => {
                filter.tag = parse_tag(child)?;
                filter.direct_tag_count += 1;
            }
            "ObjectSizeLessThan" => filter.object_size_less_than = parse_i64(text(child)?)?,
            "ObjectSizeGreaterThan" => filter.object_size_greater_than = parse_i64(text(child)?)?,
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    Ok(filter)
}

fn parse_and(node: Node<'_, '_>) -> Result<And, Error> {
    let mut and = And::default();
    for child in element_children(node) {
        match child.tag_name().name() {
            "Prefix" => and.prefix = Prefix::new(text(child)?),
            "Tag" => and.tags.push(parse_tag(child)?),
            "ObjectSizeLessThan" => and.object_size_less_than = parse_i64(text(child)?)?,
            "ObjectSizeGreaterThan" => and.object_size_greater_than = parse_i64(text(child)?)?,
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    Ok(and)
}

fn parse_tag(node: Node<'_, '_>) -> Result<Tag, Error> {
    let mut key = None;
    let mut value = None;
    for child in element_children(node) {
        match child.tag_name().name() {
            "Key" => {
                if key.is_some() {
                    return Err(Error::DuplicatedXmlTag);
                }
                key = Some(text(child)?.to_owned());
            }
            "Value" => {
                if value.is_some() {
                    return Err(Error::DuplicatedXmlTag);
                }
                value = Some(text(child)?.to_owned());
            }
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    Ok(Tag {
        key: key.unwrap_or_default(),
        value: value.unwrap_or_default(),
    })
}

fn parse_expiration(node: Node<'_, '_>) -> Result<Expiration, Error> {
    let mut expiration = Expiration {
        set: true,
        ..Expiration::default()
    };
    for child in element_children(node) {
        match child.tag_name().name() {
            "Days" => {
                let days = parse_i32(text(child)?)?;
                if days <= 0 {
                    return Err(Error::LifecycleInvalidDays);
                }
                expiration.days = Some(days);
            }
            "Date" => expiration.date = Some(parse_expiration_date(text(child)?)?),
            "ExpiredObjectDeleteMarker" => {
                expiration.delete_marker = parse_bool_flag(text(child)?)?
            }
            "ExpiredObjectAllVersions" => expiration.delete_all = parse_bool_flag(text(child)?)?,
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    Ok(expiration)
}

fn parse_del_marker_expiration(node: Node<'_, '_>) -> Result<DelMarkerExpiration, Error> {
    let mut days = 0;
    for child in element_children(node) {
        match child.tag_name().name() {
            "Days" => {
                days = parse_i32(text(child)?)?;
            }
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    if days <= 0 {
        return Err(Error::InvalidDaysDelMarkerExpiration);
    }
    Ok(DelMarkerExpiration { days })
}

fn parse_transition(node: Node<'_, '_>) -> Result<Transition, Error> {
    let mut transition = Transition {
        set: true,
        ..Transition::default()
    };
    for child in element_children(node) {
        match child.tag_name().name() {
            "Days" => {
                let days = parse_i32(text(child)?)?;
                if days < 0 {
                    return Err(Error::TransitionInvalidDays);
                }
                transition.days = Some(days);
            }
            "Date" => transition.date = Some(parse_transition_date(text(child)?)?),
            "StorageClass" => transition.storage_class = text(child)?.to_owned(),
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    Ok(transition)
}

fn parse_noncurrent_version_expiration_node(
    node: Node<'_, '_>,
) -> Result<NoncurrentVersionExpiration, Error> {
    let mut expiration = NoncurrentVersionExpiration {
        set: true,
        ..NoncurrentVersionExpiration::default()
    };
    for child in element_children(node) {
        match child.tag_name().name() {
            "NoncurrentDays" => {
                let days = parse_i32(text(child)?)?;
                if days <= 0 {
                    if days < 0 {
                        expiration.noncurrent_days = Some(days);
                    }
                } else {
                    expiration.noncurrent_days = Some(days);
                }
            }
            "NewerNoncurrentVersions" | "MaxNoncurrentVersions" => {
                expiration.newer_noncurrent_versions = parse_i32(text(child)?)?;
            }
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    Ok(expiration)
}

fn parse_noncurrent_version_transition(
    node: Node<'_, '_>,
) -> Result<NoncurrentVersionTransition, Error> {
    let mut transition = NoncurrentVersionTransition {
        set: true,
        ..NoncurrentVersionTransition::default()
    };
    for child in element_children(node) {
        match child.tag_name().name() {
            "NoncurrentDays" => transition.noncurrent_days = parse_i32(text(child)?)?,
            "StorageClass" => transition.storage_class = text(child)?.to_owned(),
            "Days" => {}
            _ => return Err(Error::UnknownXmlTag),
        }
    }
    Ok(transition)
}

fn parse_bool_flag(value: &str) -> Result<Boolean, Error> {
    match value.trim() {
        "true" => Ok(Boolean {
            val: true,
            set: true,
        }),
        "false" => Ok(Boolean {
            val: false,
            set: true,
        }),
        _ => Err(Error::XmlNotWellFormed),
    }
}

fn parse_expiration_date(value: &str) -> Result<DateTime<Utc>, Error> {
    let date = DateTime::parse_from_rfc3339(value)
        .map_err(|_| Error::LifecycleInvalidDate)?
        .with_timezone(&Utc);
    validate_midnight(date).map_err(|_| Error::LifecycleDateNotMidnight)?;
    Ok(date)
}

fn parse_transition_date(value: &str) -> Result<DateTime<Utc>, Error> {
    let date = DateTime::parse_from_rfc3339(value)
        .map_err(|_| Error::TransitionInvalidDate)?
        .with_timezone(&Utc);
    validate_midnight(date).map_err(|_| Error::TransitionDateNotMidnight)?;
    Ok(date)
}

fn parse_rfc3339_datetime(value: &str) -> Result<DateTime<Utc>, Error> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| Error::Parse("invalid RFC3339 timestamp".to_owned()))
}

fn validate_midnight(date: DateTime<Utc>) -> Result<(), ()> {
    if date.hour() == 0
        && date.minute() == 0
        && date.second() == 0
        && date.timestamp_subsec_nanos() == 0
    {
        Ok(())
    } else {
        Err(())
    }
}

fn element_children<'a, 'input>(node: Node<'a, 'input>) -> Vec<Node<'a, 'input>> {
    node.children().filter(Node::is_element).collect()
}

fn text<'a: 'input, 'input>(node: Node<'a, 'input>) -> Result<&'input str, Error> {
    Ok(node.text().unwrap_or("").trim())
}

fn parse_i32(value: &str) -> Result<i32, Error> {
    value.trim().parse().map_err(|_| Error::XmlNotWellFormed)
}

fn parse_i64(value: &str) -> Result<i64, Error> {
    value.trim().parse().map_err(|_| Error::XmlNotWellFormed)
}
