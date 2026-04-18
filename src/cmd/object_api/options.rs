use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GetObjectAttributesOptions {
    pub object_attributes: BTreeSet<String>,
}

pub fn get_and_validate_attributes_opts(header_values: &[String]) -> GetObjectAttributesOptions {
    let mut opts = GetObjectAttributesOptions::default();
    for value in header_values {
        for item in value.split(',') {
            let trimmed = item.trim();
            if !trimmed.is_empty() {
                opts.object_attributes.insert(trimmed.to_string());
            }
        }
    }
    opts
}
