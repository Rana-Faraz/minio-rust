use std::io::Read;

use super::{parse_name, Arn, Error, Name, RulesMap, TargetList};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FilterRule {
    pub name: String,
    pub value: String,
}

impl FilterRule {
    pub fn unmarshal_xml(data: &[u8]) -> Result<Self, Error> {
        let xml = std::str::from_utf8(data).map_err(|_| invalid_filter_value("<non-utf8>"))?;
        let inner = extract_inner(xml, "FilterRule")?;
        let name = extract_required_text(inner, "Name")?;
        let value = extract_required_text(inner, "Value")?;

        if name != "prefix" && name != "suffix" {
            return Err(invalid_filter_name(&name));
        }

        validate_filter_rule_value(&value)?;

        Ok(Self { name, value })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FilterRuleList {
    pub rules: Vec<FilterRule>,
}

impl FilterRuleList {
    pub fn unmarshal_xml(data: &[u8]) -> Result<Self, Error> {
        let xml = std::str::from_utf8(data).map_err(|_| invalid_xml("<non-utf8>"))?;
        let inner = extract_inner(xml, "S3Key")?;
        let mut rules = Vec::new();

        for rule_xml in extract_blocks(inner, "FilterRule") {
            rules.push(FilterRule::unmarshal_xml(rule_xml.as_bytes())?);
        }

        let prefix_count = rules.iter().filter(|rule| rule.name == "prefix").count();
        if prefix_count > 1 {
            return Err(filter_name_prefix());
        }

        let suffix_count = rules.iter().filter(|rule| rule.name == "suffix").count();
        if suffix_count > 1 {
            return Err(filter_name_suffix());
        }

        Ok(Self { rules })
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn pattern(&self) -> String {
        let mut prefix = "";
        let mut suffix = "";

        for rule in &self.rules {
            match rule.name.as_str() {
                "prefix" => prefix = &rule.value,
                "suffix" => suffix = &rule.value,
                _ => {}
            }
        }

        super::new_pattern(prefix, suffix)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct S3Key {
    pub rule_list: FilterRuleList,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Queue {
    pub id: String,
    pub filter: S3Key,
    pub events: Vec<Name>,
    pub arn: Arn,
}

impl Queue {
    pub fn unmarshal_xml(data: &[u8]) -> Result<Self, Error> {
        let xml = std::str::from_utf8(data).map_err(|_| invalid_xml("<non-utf8>"))?;
        let inner = extract_inner(xml, "QueueConfiguration")?;
        let id = extract_optional_text(inner, "Id").unwrap_or_default();
        let filter = extract_optional_block(inner, "Filter")
            .map(parse_filter)
            .transpose()?
            .unwrap_or_default();
        let arn_text = extract_required_text(inner, "Queue")?;
        let arn = Arn::parse(&arn_text)?;

        let events: Result<Vec<_>, _> = extract_texts(inner, "Event")
            .into_iter()
            .map(|event_name| parse_name(&event_name))
            .collect();
        let events = events?;
        if events.is_empty() {
            return Err(Error("missing event name(s)".to_owned()));
        }

        for (index, event_name) in events.iter().enumerate() {
            if events[index + 1..].contains(event_name) {
                return Err(duplicate_event_name(*event_name));
            }
        }

        Ok(Self {
            id,
            filter,
            events,
            arn,
        })
    }

    pub fn validate(&self, region: &str, target_list: Option<&TargetList>) -> Result<(), Error> {
        if !self.arn.region.is_empty() && !region.is_empty() && self.arn.region != region {
            return Err(unknown_region(&self.arn.region));
        }

        let Some(target_list) = target_list else {
            return Err(arn_not_found(&self.arn));
        };

        if !target_list.exists(&self.arn.target_id) {
            return Err(arn_not_found(&self.arn));
        }

        Ok(())
    }

    pub fn set_region(&mut self, region: &str) {
        self.arn.region = region.to_owned();
    }

    pub fn to_rules_map(&self) -> RulesMap {
        let pattern = self.filter.rule_list.pattern();
        RulesMap::new(&self.events, &pattern, self.arn.target_id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub xmlns: String,
    pub queue_list: Vec<Queue>,
}

impl Config {
    pub fn unmarshal_xml(data: &[u8]) -> Result<Self, Error> {
        let xml = std::str::from_utf8(data).map_err(|_| invalid_xml("<non-utf8>"))?;
        let root = find_root(xml, "NotificationConfiguration")?;
        let inner = &xml[root.inner_start..root.inner_end];

        if !extract_blocks(inner, "CloudFunctionConfiguration").is_empty()
            || !extract_blocks(inner, "TopicConfiguration").is_empty()
        {
            return Err(unsupported_configuration());
        }

        let mut queue_list = Vec::new();
        for queue_xml in extract_blocks(inner, "QueueConfiguration") {
            queue_list.push(Queue::unmarshal_xml(queue_xml.as_bytes())?);
        }

        if !queue_list.is_empty() {
            for i in 0..queue_list.len().saturating_sub(1) {
                for j in i + 1..queue_list.len() {
                    let q1 = queue_list[i].clone();
                    let mut q2 = queue_list[j].clone();
                    if !q2.arn.region.is_empty() && q1.arn.region.is_empty() {
                        q2.arn.region.clear();
                    }
                    if q1 == q2 {
                        return Err(duplicate_queue_configuration(&q1));
                    }
                }
            }
        }

        Ok(Self {
            xmlns: root
                .attrs
                .get("xmlns")
                .map(ToOwned::to_owned)
                .unwrap_or_default(),
            queue_list,
        })
    }

    pub fn validate(&self, region: &str, target_list: Option<&TargetList>) -> Result<(), Error> {
        for queue in &self.queue_list {
            queue.validate(region, target_list)?;
        }
        Ok(())
    }

    pub fn set_region(&mut self, region: &str) {
        for queue in &mut self.queue_list {
            queue.set_region(region);
        }
    }

    pub fn to_rules_map(&self) -> RulesMap {
        let mut rules_map = RulesMap::default();
        for queue in &self.queue_list {
            rules_map.add(&queue.to_rules_map());
        }
        rules_map
    }
}

pub fn parse_config<R: Read>(
    mut reader: R,
    region: &str,
    target_list: Option<&TargetList>,
) -> Result<Config, Error> {
    let mut xml = String::new();
    reader
        .read_to_string(&mut xml)
        .map_err(|error| Error(error.to_string()))?;

    let mut config = Config::unmarshal_xml(xml.as_bytes())?;
    config.validate(region, target_list)?;
    config.set_region(region);

    if config.xmlns.is_empty() {
        config.xmlns = "http://s3.amazonaws.com/doc/2006-03-01/".to_owned();
    }

    Ok(config)
}

pub fn validate_filter_rule_value(value: &str) -> Result<(), Error> {
    validate_filter_rule_value_bytes(value.as_bytes())
}

pub fn validate_filter_rule_value_bytes(value: &[u8]) -> Result<(), Error> {
    let text = std::str::from_utf8(value).map_err(|_| invalid_filter_value("<non-utf8>"))?;

    for segment in text.split('/') {
        if segment == "." || segment == ".." {
            return Err(invalid_filter_value(text));
        }
    }

    if text.len() <= 1024 && !text.contains('\\') {
        return Ok(());
    }

    Err(invalid_filter_value(text))
}

fn parse_filter(xml: String) -> Result<S3Key, Error> {
    let inner = extract_inner(&xml, "Filter")?;
    let rule_list = extract_optional_block(inner, "S3Key")
        .map(|s3_key| FilterRuleList::unmarshal_xml(s3_key.as_bytes()))
        .transpose()?
        .unwrap_or_default();
    Ok(S3Key { rule_list })
}

fn invalid_filter_name(value: &str) -> Error {
    Error(format!("invalid filter name '{}'", value))
}

fn filter_name_prefix() -> Error {
    Error("more than one prefix in filter rule".to_owned())
}

fn filter_name_suffix() -> Error {
    Error("more than one suffix in filter rule".to_owned())
}

fn invalid_filter_value(value: &str) -> Error {
    Error(format!("invalid filter value '{}'", value))
}

fn duplicate_event_name(value: Name) -> Error {
    Error(format!("duplicate event name '{}' found", value))
}

fn unsupported_configuration() -> Error {
    Error("topic or cloud function configuration is not supported".to_owned())
}

fn duplicate_queue_configuration(queue: &Queue) -> Error {
    Error(format!("duplicate queue configuration {:?}", queue))
}

fn unknown_region(region: &str) -> Error {
    Error(format!("unknown region '{}'", region))
}

fn arn_not_found(arn: &Arn) -> Error {
    Error(format!("ARN '{}' not found", arn))
}

fn invalid_xml(value: &str) -> Error {
    Error(format!("invalid XML '{}'", value))
}

fn extract_optional_text(xml: &str, tag: &str) -> Option<String> {
    extract_optional_block(xml, tag)
        .and_then(|block| extract_inner(&block, tag).ok().map(str::to_owned))
}

fn extract_required_text(xml: &str, tag: &str) -> Result<String, Error> {
    extract_optional_text(xml, tag).ok_or_else(|| invalid_xml(xml))
}

fn extract_texts(xml: &str, tag: &str) -> Vec<String> {
    extract_blocks(xml, tag)
        .into_iter()
        .filter_map(|block| extract_inner(&block, tag).ok().map(str::to_owned))
        .collect()
}

fn extract_optional_block(xml: &str, tag: &str) -> Option<String> {
    find_block(xml, tag).map(|root| xml[root.start..root.end].to_owned())
}

fn extract_blocks(xml: &str, tag: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut offset = 0;

    while let Some(root) = find_block(&xml[offset..], tag) {
        let start = offset + root.start;
        let end = offset + root.end;
        blocks.push(xml[start..end].to_owned());
        offset = end;
    }

    blocks
}

fn extract_inner<'a>(xml: &'a str, tag: &str) -> Result<&'a str, Error> {
    let root = find_root(xml, tag)?;
    Ok(xml[root.inner_start..root.inner_end].trim())
}

#[derive(Debug, Clone, Copy)]
struct BlockRoot<'a> {
    start: usize,
    end: usize,
    inner_start: usize,
    inner_end: usize,
    attrs: Attrs<'a>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Attrs<'a> {
    raw: &'a str,
}

impl<'a> Attrs<'a> {
    fn get(&self, key: &str) -> Option<&'a str> {
        let needle = format!("{key}=\"");
        let start = self.raw.find(&needle)? + needle.len();
        let tail = &self.raw[start..];
        let end = tail.find('"')?;
        Some(&tail[..end])
    }
}

fn find_root<'a>(xml: &'a str, tag: &str) -> Result<BlockRoot<'a>, Error> {
    find_block(xml, tag).ok_or_else(|| invalid_xml(xml))
}

fn find_block<'a>(xml: &'a str, tag: &str) -> Option<BlockRoot<'a>> {
    let open = format!("<{tag}");
    let start = xml.find(&open)?;
    let open_end = start + xml[start..].find('>')?;
    let attrs = &xml[start + open.len()..open_end];
    let inner_start = open_end + 1;
    let close = format!("</{tag}>");
    let inner_end = inner_start + xml[inner_start..].find(&close)?;
    let end = inner_end + close.len();

    Some(BlockRoot {
        start,
        end,
        inner_start,
        inner_end,
        attrs: Attrs { raw: attrs },
    })
}
