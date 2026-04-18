use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostPolicyCondition {
    Equals { field: String, value: String },
    StartsWith { field: String, value: String },
    ContentLengthRange { min: i64, max: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostPolicyForm {
    pub expiration: String,
    pub conditions: Vec<PostPolicyCondition>,
}

impl PostPolicyForm {
    pub fn bucket(&self) -> Option<&str> {
        self.conditions
            .iter()
            .find_map(|condition| match condition {
                PostPolicyCondition::Equals { field, value }
                    if field == "bucket" || field == "$bucket" =>
                {
                    Some(value.as_str())
                }
                _ => None,
            })
    }
}

pub fn parse_post_policy_form(input: &str) -> Result<PostPolicyForm, String> {
    let value: Value = serde_json::from_str(input).map_err(|err| err.to_string())?;
    let expiration = value
        .get("expiration")
        .and_then(Value::as_str)
        .ok_or_else(|| "post policy is missing expiration".to_string())?
        .to_string();

    let raw_conditions = value
        .get("conditions")
        .and_then(Value::as_array)
        .ok_or_else(|| "post policy is missing conditions".to_string())?;

    let mut conditions = Vec::new();
    for condition in raw_conditions {
        match condition {
            Value::Object(map) => {
                for (field, value) in map {
                    let value = value.as_str().ok_or_else(|| {
                        "post policy object condition must be a string".to_string()
                    })?;
                    conditions.push(PostPolicyCondition::Equals {
                        field: field.clone(),
                        value: value.to_string(),
                    });
                }
            }
            Value::Array(items) => {
                if items.is_empty() {
                    return Err("post policy condition cannot be empty".to_string());
                }
                let op = items[0]
                    .as_str()
                    .ok_or_else(|| "post policy operation must be a string".to_string())?;
                match op {
                    "eq" => {
                        if items.len() != 3 {
                            return Err("eq condition must have 3 elements".to_string());
                        }
                        conditions.push(PostPolicyCondition::Equals {
                            field: items[1]
                                .as_str()
                                .ok_or_else(|| "eq field must be a string".to_string())?
                                .trim_start_matches('$')
                                .to_string(),
                            value: items[2]
                                .as_str()
                                .ok_or_else(|| "eq value must be a string".to_string())?
                                .to_string(),
                        });
                    }
                    "starts-with" => {
                        if items.len() != 3 {
                            return Err("starts-with condition must have 3 elements".to_string());
                        }
                        conditions.push(PostPolicyCondition::StartsWith {
                            field: items[1]
                                .as_str()
                                .ok_or_else(|| "starts-with field must be a string".to_string())?
                                .trim_start_matches('$')
                                .to_string(),
                            value: items[2]
                                .as_str()
                                .ok_or_else(|| "starts-with value must be a string".to_string())?
                                .to_string(),
                        });
                    }
                    "content-length-range" => {
                        if items.len() != 3 {
                            return Err(
                                "content-length-range condition must have 3 elements".to_string()
                            );
                        }
                        conditions.push(PostPolicyCondition::ContentLengthRange {
                            min: items[1].as_i64().ok_or_else(|| {
                                "content-length-range min must be an integer".to_string()
                            })?,
                            max: items[2].as_i64().ok_or_else(|| {
                                "content-length-range max must be an integer".to_string()
                            })?,
                        });
                    }
                    _ => return Err(format!("unsupported post policy operator: {op}")),
                }
            }
            _ => return Err("post policy condition must be an object or array".to_string()),
        }
    }

    Ok(PostPolicyForm {
        expiration,
        conditions,
    })
}
