use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub const FLAG_CRC_XXH3: Flags = Flags(1 << 0);
pub const FLAG_EOF: Flags = Flags(1 << 1);
pub const FLAG_STATELESS: Flags = Flags(1 << 2);
pub const FLAG_PAYLOAD_IS_ERR: Flags = Flags(1 << 3);
pub const FLAG_PAYLOAD_IS_ZERO: Flags = Flags(1 << 4);
pub const FLAG_SUBROUTE: Flags = Flags(1 << 5);

pub const OP_CONNECT: Op = Op(1);
pub const OP_CONNECT_RESPONSE: Op = Op(2);
pub const OP_PING: Op = Op(3);
pub const OP_PONG: Op = Op(4);
pub const OP_CONNECT_MUX: Op = Op(5);
pub const OP_MUX_CONNECT_ERROR: Op = Op(6);
pub const OP_REQUEST: Op = Op(14);
pub const OP_RESPONSE: Op = Op(15);

fn marshal_named<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value).map_err(|err| err.to_string())
}

fn unmarshal_named<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    rmp_serde::from_slice(bytes).map_err(|err| err.to_string())
}

macro_rules! impl_msg_codec {
    ($ty:ty) => {
        impl $ty {
            pub fn marshal_msg(&self) -> Result<Vec<u8>, String> {
                marshal_named(self)
            }

            pub fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
                *self = unmarshal_named(bytes)?;
                Ok(&[])
            }

            pub fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
                writer
                    .write_all(&self.marshal_msg()?)
                    .map_err(|err| err.to_string())
            }

            pub fn decode(&mut self, reader: &mut impl Read) -> Result<(), String> {
                let mut bytes = Vec::new();
                reader
                    .read_to_end(&mut bytes)
                    .map_err(|err| err.to_string())?;
                self.unmarshal_msg(&bytes)?;
                Ok(())
            }

            pub fn msgsize(&self) -> usize {
                self.marshal_msg().map(|bytes| bytes.len()).unwrap_or(0)
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct MSS(pub Option<BTreeMap<String, String>>);

impl MSS {
    pub fn new() -> Self {
        Self(Some(BTreeMap::new()))
    }

    pub fn with_map(values: BTreeMap<String, String>) -> Self {
        Self(Some(values))
    }

    pub fn get(&self, key: &str) -> &str {
        self.0
            .as_ref()
            .and_then(|values| values.get(key))
            .map(String::as_str)
            .unwrap_or("")
    }

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0
            .get_or_insert_with(BTreeMap::new)
            .insert(key.into(), value.into());
    }

    pub fn recycle(&mut self) {
        self.0 = None;
    }

    pub fn to_query(&self) -> String {
        let Some(values) = &self.0 else {
            return String::new();
        };
        if values.is_empty() {
            return String::new();
        }
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in values {
            serializer.append_pair(key, value);
        }
        format!("?{}", serializer.finish())
    }
}

impl_msg_codec!(MSS);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Bytes(pub Option<Vec<u8>>);

impl Bytes {
    pub fn with_bytes(bytes: Vec<u8>) -> Self {
        Self(Some(bytes))
    }

    pub fn as_slice(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }

    pub fn recycle(&mut self) {
        self.0 = None;
    }
}

impl_msg_codec!(Bytes);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Flags(pub u8);

impl Flags {
    pub fn set(&mut self, flags: Flags) {
        self.0 |= flags.0;
    }

    pub fn clear(&mut self, flags: Flags) {
        self.0 &= !flags.0;
    }

    pub fn contains(self, flags: Flags) -> bool {
        self.0 & flags.0 == flags.0
    }
}

impl fmt::Display for Flags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if self.contains(FLAG_CRC_XXH3) {
            parts.push("CRC");
        }
        if self.contains(FLAG_EOF) {
            parts.push("EOF");
        }
        if self.contains(FLAG_STATELESS) {
            parts.push("SL");
        }
        if self.contains(FLAG_PAYLOAD_IS_ERR) {
            parts.push("ERR");
        }
        if self.contains(FLAG_PAYLOAD_IS_ZERO) {
            parts.push("ZERO");
        }
        if self.contains(FLAG_SUBROUTE) {
            parts.push("SUB");
        }
        write!(f, "[{}]", parts.join(","))
    }
}

impl_msg_codec!(Flags);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct HandlerID(pub u8);

impl_msg_codec!(HandlerID);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Op(pub u8);

impl_msg_codec!(Op);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConnectReq {
    #[serde(rename = "ID")]
    pub id: [u8; 16],
    #[serde(rename = "Host")]
    pub host: String,
    #[serde(rename = "Time")]
    pub time_unix_ms: i64,
    #[serde(rename = "Token")]
    pub token: String,
}

impl ConnectReq {
    pub fn op(&self) -> Op {
        OP_CONNECT
    }
}

impl_msg_codec!(ConnectReq);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConnectResp {
    #[serde(rename = "ID")]
    pub id: [u8; 16],
    #[serde(rename = "Accepted")]
    pub accepted: bool,
    #[serde(rename = "RejectedReason")]
    pub rejected_reason: String,
}

impl ConnectResp {
    pub fn op(&self) -> Op {
        OP_CONNECT_RESPONSE
    }
}

impl_msg_codec!(ConnectResp);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Message {
    #[serde(rename = "MuxID")]
    pub mux_id: u64,
    #[serde(rename = "Seq")]
    pub seq: u32,
    #[serde(rename = "DeadlineMS")]
    pub deadline_ms: u32,
    #[serde(rename = "Handler")]
    pub handler: HandlerID,
    #[serde(rename = "Op")]
    pub op: Op,
    #[serde(rename = "Flags")]
    pub flags: Flags,
    #[serde(rename = "Payload")]
    pub payload: Option<Vec<u8>>,
}

impl Message {
    pub fn set_zero_payload_flag(&mut self) {
        self.flags.clear(FLAG_PAYLOAD_IS_ZERO);
        if matches!(self.payload.as_ref(), Some(payload) if payload.is_empty()) {
            self.flags.set(FLAG_PAYLOAD_IS_ZERO);
        }
    }
}

impl_msg_codec!(Message);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MuxConnectError {
    #[serde(rename = "Error")]
    pub error: String,
}

impl MuxConnectError {
    pub fn op(&self) -> Op {
        OP_MUX_CONNECT_ERROR
    }
}

impl_msg_codec!(MuxConnectError);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PingMsg {
    #[serde(rename = "t")]
    pub t_unix_ms: i64,
}

impl PingMsg {
    pub fn op(&self) -> Op {
        OP_PING
    }
}

impl_msg_codec!(PingMsg);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PongMsg {
    #[serde(rename = "nf")]
    pub not_found: bool,
    #[serde(rename = "e")]
    pub err: Option<String>,
    #[serde(rename = "t")]
    pub t_unix_ms: i64,
}

impl PongMsg {
    pub fn op(&self) -> Op {
        OP_PONG
    }
}

impl_msg_codec!(PongMsg);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TestRequest {
    #[serde(rename = "Num")]
    pub num: i32,
    #[serde(rename = "String")]
    pub string: String,
}

impl_msg_codec!(TestRequest);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TestResponse {
    #[serde(rename = "OrgNum")]
    pub org_num: i32,
    #[serde(rename = "OrgString")]
    pub org_string: String,
    #[serde(rename = "Embedded")]
    pub embedded: TestRequest,
}

impl_msg_codec!(TestResponse);

#[derive(Debug, Default)]
struct ConnectionState {
    connected: bool,
    waiters: Vec<Sender<()>>,
}

#[derive(Debug, Clone)]
pub struct Connection {
    pub local: String,
    pub remote: String,
    state: Arc<(Mutex<ConnectionState>, Condvar)>,
}

impl Connection {
    pub fn new(local: impl Into<String>, remote: impl Into<String>) -> Self {
        Self {
            local: local.into(),
            remote: remote.into(),
            state: Arc::new((
                Mutex::new(ConnectionState {
                    connected: true,
                    waiters: Vec::new(),
                }),
                Condvar::new(),
            )),
        }
    }

    pub fn should_connect(&self) -> bool {
        let h0 = deterministic_hash(&(self.local.as_str(), self.remote.as_str()));
        let h1 = deterministic_hash(&(self.remote.as_str(), self.local.as_str()));
        if h0 == h1 {
            return self.local < self.remote;
        }
        h0 < h1
    }

    pub fn is_connected(&self) -> bool {
        let (state, _) = &*self.state;
        state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .connected
    }

    pub fn wait_for_connect(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (state, cv) = &*self.state;
        let mut guard = state.lock().unwrap_or_else(|err| err.into_inner());
        while !guard.connected {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let wait = deadline.saturating_duration_since(now);
            let (next, result) = cv
                .wait_timeout(guard, wait)
                .unwrap_or_else(|err| err.into_inner());
            guard = next;
            if result.timed_out() && !guard.connected {
                return false;
            }
        }
        true
    }

    pub fn disconnect(&self) {
        let (state, cv) = &*self.state;
        let waiters = {
            let mut guard = state.lock().unwrap_or_else(|err| err.into_inner());
            guard.connected = false;
            std::mem::take(&mut guard.waiters)
        };
        for waiter in waiters {
            let _ = waiter.send(());
        }
        cv.notify_all();
    }

    pub fn reconnect(&self) {
        let (state, cv) = &*self.state;
        let mut guard = state.lock().unwrap_or_else(|err| err.into_inner());
        guard.connected = true;
        cv.notify_all();
    }

    pub fn request(&self) -> PendingOperation {
        self.register_pending()
    }

    pub fn new_stream(&self) -> PendingOperation {
        self.register_pending()
    }

    fn register_pending(&self) -> PendingOperation {
        let (tx, rx) = mpsc::channel();
        let (state, _) = &*self.state;
        let mut guard = state.lock().unwrap_or_else(|err| err.into_inner());
        if guard.connected {
            guard.waiters.push(tx);
        } else {
            let _ = tx.send(());
        }
        PendingOperation { rx }
    }
}

#[derive(Debug)]
pub struct PendingOperation {
    rx: Receiver<()>,
}

impl PendingOperation {
    pub fn wait(self) -> Result<(), String> {
        self.rx
            .recv()
            .map_err(|_| "pending operation unexpectedly completed".to_owned())?;
        Err("remote disconnected".to_owned())
    }
}

fn deterministic_hash(value: &impl Hash) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
