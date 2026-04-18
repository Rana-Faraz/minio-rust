use std::collections::BTreeMap;

use crate::cmd::{
    HandlerCredentials, HandlerResponse, LocalObjectLayer, ObjectApiHandlers, RequestAuth,
};

#[derive(Debug)]
pub struct ObjectLambdaHandlers {
    inner: ObjectApiHandlers,
}

impl ObjectLambdaHandlers {
    pub fn new(layer: LocalObjectLayer, credentials: HandlerCredentials) -> Self {
        Self {
            inner: ObjectApiHandlers::new(layer, credentials),
        }
    }

    pub fn get_object_lambda(
        &self,
        bucket: &str,
        object: &str,
        auth: &RequestAuth,
        transform: Option<&str>,
    ) -> HandlerResponse {
        let mut response = self.inner.get_object(bucket, object, auth, None);
        if response.status != 200 && response.status != 206 {
            return response;
        }

        match transform.unwrap_or("identity") {
            "identity" => response,
            "uppercase" => {
                response.body = response.body.iter().map(u8::to_ascii_uppercase).collect();
                response.headers.insert(
                    "content-length".to_string(),
                    response.body.len().to_string(),
                );
                response
            }
            "reverse" => {
                response.body.reverse();
                response.headers.insert(
                    "content-length".to_string(),
                    response.body.len().to_string(),
                );
                response
            }
            _ => HandlerResponse {
                status: 400,
                headers: BTreeMap::new(),
                body: b"unsupported object lambda transform".to_vec(),
            },
        }
    }
}
