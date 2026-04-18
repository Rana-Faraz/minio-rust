use super::*;
use std::sync::Arc;

mod auth;
mod bucket;
mod copy;
mod multipart;
mod object;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestAuthKind {
    Anonymous,
    SignedV2,
    SignedV4,
    PresignedV2,
    PresignedV4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestAuth {
    pub kind: RequestAuthKind,
    pub access_key: String,
    pub secret_key: String,
    pub prevalidated: bool,
}

impl RequestAuth {
    pub fn anonymous() -> Self {
        Self {
            kind: RequestAuthKind::Anonymous,
            access_key: String::new(),
            secret_key: String::new(),
            prevalidated: false,
        }
    }

    pub fn signed_v2(access_key: &str, secret_key: &str) -> Self {
        Self {
            kind: RequestAuthKind::SignedV2,
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            prevalidated: false,
        }
    }

    pub fn signed_v4(access_key: &str, secret_key: &str) -> Self {
        Self {
            kind: RequestAuthKind::SignedV4,
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            prevalidated: false,
        }
    }

    pub fn presigned_v2(access_key: &str, secret_key: &str) -> Self {
        Self {
            kind: RequestAuthKind::PresignedV2,
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            prevalidated: false,
        }
    }

    pub fn presigned_v4(access_key: &str, secret_key: &str) -> Self {
        Self {
            kind: RequestAuthKind::PresignedV4,
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            prevalidated: false,
        }
    }

    pub fn validated(kind: RequestAuthKind, access_key: &str) -> Self {
        Self {
            kind,
            access_key: access_key.to_string(),
            secret_key: String::new(),
            prevalidated: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerCredentials {
    pub access_key: String,
    pub secret_key: String,
}

impl HandlerCredentials {
    pub fn new(access_key: &str, secret_key: &str) -> Self {
        Self {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HandlerResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl HandlerResponse {
    fn status_only(status: u16) -> Self {
        Self {
            status,
            ..Self::default()
        }
    }

    fn xml(status: u16, body: String) -> Self {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/xml".to_string());
        Self {
            status,
            headers,
            body: body.into_bytes(),
        }
    }
}

#[derive(Debug)]
pub struct ObjectApiHandlers {
    layer: Option<Arc<LocalObjectLayer>>,
    credentials: HandlerCredentials,
    replication_targets: BTreeMap<String, ReplicationRemoteTarget>,
    replication_service: Option<ReplicationService>,
}

impl ObjectApiHandlers {
    pub fn new(layer: LocalObjectLayer, credentials: HandlerCredentials) -> Self {
        Self::from_shared_layer(Arc::new(layer), credentials)
    }

    pub fn from_shared_layer(
        layer: Arc<LocalObjectLayer>,
        credentials: HandlerCredentials,
    ) -> Self {
        Self {
            layer: Some(layer),
            credentials,
            replication_targets: BTreeMap::new(),
            replication_service: None,
        }
    }

    pub fn without_layer(credentials: HandlerCredentials) -> Self {
        Self {
            layer: None,
            credentials,
            replication_targets: BTreeMap::new(),
            replication_service: None,
        }
    }

    pub fn with_replication_targets(
        mut self,
        replication_targets: BTreeMap<String, ReplicationRemoteTarget>,
    ) -> Self {
        self.replication_targets = replication_targets;
        self
    }

    pub fn with_replication_service(mut self, replication_service: ReplicationService) -> Self {
        self.replication_service = Some(replication_service);
        self
    }

    pub fn layer(&self) -> Option<&LocalObjectLayer> {
        self.layer.as_deref()
    }

    pub fn shared_layer(&self) -> Option<Arc<LocalObjectLayer>> {
        self.layer.clone()
    }

    pub fn replication_service(&self) -> Option<&ReplicationService> {
        self.replication_service.as_ref()
    }

    fn require_layer(&self) -> Result<&LocalObjectLayer, HandlerResponse> {
        self.layer.as_deref().ok_or_else(|| {
            api_error_response(
                503,
                "XMinioServerNotInitialized",
                "server not initialized",
                "",
                "",
                "",
            )
        })
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn api_error_response(
    status: u16,
    code: &str,
    message: &str,
    resource: &str,
    bucket: &str,
    key: &str,
) -> HandlerResponse {
    HandlerResponse::xml(
        status,
        format!(
            "<Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource><BucketName>{}</BucketName><Key>{}</Key></Error>",
            xml_escape(code),
            xml_escape(message),
            xml_escape(resource),
            xml_escape(bucket),
            xml_escape(key),
        ),
    )
}
