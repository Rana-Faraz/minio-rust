use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use minio_rust::internal::grid::{TestRequest, TestResponse, MSS};

pub const SOURCE_FILE: &str = "internal/grid/grid_test.go";

const HANDLER_TEST: u8 = 1;
const HANDLER_TEST2: u8 = 2;
const TEST_PAYLOAD: &str = "Hello Grid World!";

#[derive(Debug, Clone, PartialEq, Eq)]
enum GridError {
    Remote(String),
    UnknownHandler,
    Canceled,
    DeadlineExceeded,
    Disconnected,
}

impl GridError {
    fn remote(msg: impl Into<String>) -> Self {
        Self::Remote(msg.into())
    }
}

#[derive(Clone)]
struct CancelToken {
    inner: Arc<CancelInner>,
}

struct CancelInner {
    done: AtomicBool,
    reason: Mutex<Option<GridError>>,
    cv: Condvar,
}

impl CancelToken {
    fn new() -> Self {
        Self {
            inner: Arc::new(CancelInner {
                done: AtomicBool::new(false),
                reason: Mutex::new(None),
                cv: Condvar::new(),
            }),
        }
    }

    fn cancel(&self, reason: GridError) {
        let mut guard = self
            .inner
            .reason
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if guard.is_none() {
            *guard = Some(reason);
            self.inner.done.store(true, Ordering::SeqCst);
            self.inner.cv.notify_all();
        }
    }

    fn reason(&self) -> Option<GridError> {
        self.inner
            .reason
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    fn wait(&self) -> GridError {
        let mut guard = self
            .inner
            .reason
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        while guard.is_none() {
            guard = self
                .inner
                .cv
                .wait(guard)
                .unwrap_or_else(|err| err.into_inner());
        }
        guard.clone().expect("cancel reason")
    }
}

#[derive(Clone)]
struct StreamCtx {
    cancel: CancelToken,
    subroute: Option<String>,
}

impl StreamCtx {
    fn canceled(&self) -> Option<GridError> {
        self.cancel.reason()
    }

    fn wait_canceled(&self) -> GridError {
        self.cancel.wait()
    }

    fn subroute(&self) -> Option<&str> {
        self.subroute.as_deref()
    }

    fn send(
        &self,
        tx: &SyncSender<Result<Vec<u8>, GridError>>,
        mut msg: Vec<u8>,
    ) -> Result<(), GridError> {
        loop {
            if let Some(err) = self.canceled() {
                return Err(err);
            }
            match tx.try_send(Ok(msg)) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(again)) => {
                    msg = match again {
                        Ok(msg) => msg,
                        Err(err) => return Err(err),
                    };
                    thread::sleep(Duration::from_millis(1));
                }
                Err(TrySendError::Disconnected(_)) => return Err(GridError::Disconnected),
            }
        }
    }

    fn recv(&self, rx: &Receiver<Vec<u8>>) -> Result<Option<Vec<u8>>, GridError> {
        loop {
            if let Some(err) = self.canceled() {
                return Err(err);
            }
            match rx.recv_timeout(Duration::from_millis(5)) {
                Ok(value) => return Ok(Some(value)),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return Ok(None),
            }
        }
    }
}

type SingleHandler = Arc<dyn Fn(Vec<u8>) -> Result<Vec<u8>, GridError> + Send + Sync>;
type StreamHandler = Arc<
    dyn Fn(
            StreamCtx,
            Vec<u8>,
            Receiver<Vec<u8>>,
            SyncSender<Result<Vec<u8>, GridError>>,
        ) -> Result<(), GridError>
        + Send
        + Sync,
>;

#[derive(Clone)]
struct StreamRegistration {
    in_capacity: usize,
    out_capacity: usize,
    handler: StreamHandler,
}

#[derive(Default)]
struct ManagerHandlers {
    singles: HashMap<u8, SingleHandler>,
    streams: HashMap<(u8, Option<String>), StreamRegistration>,
}

struct TestManager {
    host: String,
    handlers: Mutex<ManagerHandlers>,
}

impl TestManager {
    fn new(host: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            host: host.into(),
            handlers: Mutex::new(ManagerHandlers::default()),
        })
    }

    fn register_single(
        &self,
        handler_id: u8,
        handler: impl Fn(Vec<u8>) -> Result<Vec<u8>, GridError> + Send + Sync + 'static,
    ) {
        self.handlers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .singles
            .insert(handler_id, Arc::new(handler));
    }

    fn register_stream(
        &self,
        handler_id: u8,
        subroute: Option<&str>,
        in_capacity: usize,
        out_capacity: usize,
        handler: impl Fn(
                StreamCtx,
                Vec<u8>,
                Receiver<Vec<u8>>,
                SyncSender<Result<Vec<u8>, GridError>>,
            ) -> Result<(), GridError>
            + Send
            + Sync
            + 'static,
    ) {
        self.handlers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .streams
            .insert(
                (handler_id, subroute.map(str::to_owned)),
                StreamRegistration {
                    in_capacity,
                    out_capacity,
                    handler: Arc::new(handler),
                },
            );
    }

    fn connection_to(self: &Arc<Self>, remote: &Arc<Self>) -> TestConnection {
        TestConnection {
            local: self.clone(),
            remote: remote.clone(),
            subroute: None,
        }
    }
}

#[derive(Clone)]
struct TestConnection {
    local: Arc<TestManager>,
    remote: Arc<TestManager>,
    subroute: Option<String>,
}

impl TestConnection {
    fn subroute(&self, value: impl Into<String>) -> Self {
        Self {
            local: self.local.clone(),
            remote: self.remote.clone(),
            subroute: Some(value.into()),
        }
    }

    fn request(&self, handler_id: u8, payload: Vec<u8>) -> Result<Vec<u8>, GridError> {
        let handler = self
            .remote
            .handlers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .singles
            .get(&handler_id)
            .cloned();
        match handler {
            Some(handler) => handler(payload),
            None => Err(GridError::remote("unknown handler")),
        }
    }

    fn new_stream(
        &self,
        handler_id: u8,
        payload: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Result<TestStream, GridError> {
        let registration = self
            .remote
            .handlers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .streams
            .get(&(handler_id, self.subroute.clone()))
            .cloned()
            .ok_or(GridError::UnknownHandler)?;

        let (req_tx, req_rx) = mpsc::sync_channel(registration.in_capacity);
        let (resp_tx, resp_rx) = mpsc::sync_channel(registration.out_capacity);
        let cancel = CancelToken::new();
        let server_ctx = StreamCtx {
            cancel: cancel.clone(),
            subroute: self.subroute.clone(),
        };

        if let Some(timeout) = timeout {
            let cancel_for_deadline = cancel.clone();
            thread::spawn(move || {
                thread::sleep(timeout);
                cancel_for_deadline.cancel(GridError::DeadlineExceeded);
            });
        }

        thread::spawn(move || {
            let result =
                (registration.handler)(server_ctx.clone(), payload, req_rx, resp_tx.clone());
            match result {
                Err(err) => {
                    let _ = resp_tx.send(Err(err));
                }
                Ok(()) => {
                    if let Some(err) = server_ctx.canceled() {
                        let _ = resp_tx.send(Err(err));
                    }
                }
            }
        });

        Ok(TestStream {
            requests: Some(req_tx),
            responses: resp_rx,
            cancel,
        })
    }
}

struct TestStream {
    requests: Option<SyncSender<Vec<u8>>>,
    responses: Receiver<Result<Vec<u8>, GridError>>,
    cancel: CancelToken,
}

impl TestStream {
    fn send(&self, payload: Vec<u8>) {
        self.requests
            .as_ref()
            .expect("stream requests available")
            .send(payload)
            .expect("send request");
    }

    fn close_requests(&mut self) {
        self.requests.take();
    }

    fn recv(&self) -> Option<Result<Vec<u8>, GridError>> {
        self.responses.recv().ok()
    }

    fn results<F>(&self, mut f: F) -> Result<(), GridError>
    where
        F: FnMut(Vec<u8>) -> Result<(), GridError>,
    {
        while let Ok(result) = self.responses.recv() {
            match result {
                Ok(msg) => f(msg)?,
                Err(err) => return Err(err),
            }
        }
        match self.cancel.reason() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn cancel(&self) {
        self.cancel.cancel(GridError::Canceled);
    }
}

fn setup_test_grid() -> (Arc<TestManager>, Arc<TestManager>, TestConnection) {
    let local = TestManager::new("host-0");
    let remote = TestManager::new("host-1");
    let conn = local.connection_to(&remote);
    (local, remote, conn)
}

fn mss(entries: &[(&str, &str)]) -> MSS {
    let mut map = BTreeMap::new();
    for (k, v) in entries {
        map.insert((*k).to_owned(), (*v).to_owned());
    }
    MSS::with_map(map)
}

fn clone_mss(source: &MSS) -> MSS {
    source.clone()
}

fn register_single_echo_and_error(local: &Arc<TestManager>, remote: &Arc<TestManager>) {
    for manager in [local, remote] {
        manager.register_single(HANDLER_TEST, Ok::<_, GridError>);
        manager.register_single(HANDLER_TEST2, |payload| {
            Err(GridError::remote(
                String::from_utf8(payload).expect("utf8 error payload"),
            ))
        });
    }
}

fn run_single_roundtrip_case(conn: &TestConnection, payload: Vec<u8>) {
    let resp = conn
        .request(HANDLER_TEST, payload.clone())
        .expect("roundtrip ok");
    assert_eq!(resp, payload);
}

fn run_single_roundtrip_err_case(conn: &TestConnection, payload: Vec<u8>) {
    let err = conn
        .request(HANDLER_TEST2, payload.clone())
        .expect_err("remote error");
    assert_eq!(
        err,
        GridError::remote(String::from_utf8(payload).expect("utf8"))
    );
}

fn register_generic_single_handlers(local: &Arc<TestManager>, remote: &Arc<TestManager>) {
    for manager in [local, remote] {
        manager.register_single(HANDLER_TEST, |payload| {
            let mut req = TestRequest::default();
            req.unmarshal_msg(&payload).expect("decode generic request");
            let resp = TestResponse {
                org_num: req.num,
                org_string: req.string.clone(),
                embedded: req,
            };
            resp.marshal_msg().map_err(GridError::remote)
        });
        manager.register_single(HANDLER_TEST2, |payload| {
            let mut req = TestRequest::default();
            req.unmarshal_msg(&payload).expect("decode generic request");
            Err(GridError::remote(req.string))
        });
    }
}

fn register_recycle_single_handlers(local: &Arc<TestManager>, remote: &Arc<TestManager>) {
    for manager in [local, remote] {
        manager.register_single(HANDLER_TEST, |payload| {
            let mut req = MSS::default();
            req.unmarshal_msg(&payload).expect("decode MSS");
            clone_mss(&req).marshal_msg().map_err(GridError::remote)
        });
        manager.register_single(HANDLER_TEST2, |payload| {
            let mut req = MSS::default();
            req.unmarshal_msg(&payload).expect("decode MSS");
            Err(GridError::remote(req.get("err").to_owned()))
        });
    }
}

fn register_stream_roundtrip(local: &Arc<TestManager>, remote: &Arc<TestManager>) {
    for manager in [local, remote] {
        manager.register_stream(HANDLER_TEST, None, 1, 1, |ctx, payload, request, resp| {
            while let Some(incoming) = ctx.recv(&request)? {
                let mut out = payload.clone();
                out.extend_from_slice(&incoming);
                ctx.send(&resp, out)?;
            }
            Ok(())
        });
        manager.register_stream(HANDLER_TEST2, None, 1, 1, |ctx, payload, request, _resp| {
            if let Some(incoming) = ctx.recv(&request)? {
                let mut out = payload.clone();
                out.extend_from_slice(&incoming);
                return Err(GridError::remote(
                    String::from_utf8(out).expect("utf8 remote error"),
                ));
            }
            Ok(())
        });
    }
}

fn run_stream_roundtrip(conn: &TestConnection) {
    let mut stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("new stream");
    for n in 0..=10 {
        stream.send(n.to_string().into_bytes());
        let resp = stream.recv().expect("stream response").expect("stream ok");
        assert_eq!(resp, format!("{TEST_PAYLOAD}{n}").into_bytes());
    }
    stream.close_requests();
    assert!(stream.recv().is_none());
}

fn register_stream_cancel(local: &Arc<TestManager>, remote: &Arc<TestManager>) {
    for manager in [local, remote] {
        manager.register_stream(
            HANDLER_TEST,
            None,
            0,
            1,
            |ctx, _payload, _request, _resp| {
                let _ = ctx.wait_canceled();
                Ok(())
            },
        );
        manager.register_stream(
            HANDLER_TEST2,
            None,
            1,
            1,
            |ctx, _payload, request, _resp| loop {
                match ctx.recv(&request) {
                    Ok(Some(_)) => continue,
                    Ok(None) => return Ok(()),
                    Err(_) => return Ok(()),
                }
            },
        );
    }
}

fn run_stream_cancel(conn: &TestConnection, handler_id: u8, fill_requests: bool) -> GridError {
    let stream = conn
        .new_stream(handler_id, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("new cancel stream");
    if fill_requests {
        for _ in 0..3 {
            if let Some(requests) = &stream.requests {
                let _ = requests.try_send(b"Hello".to_vec());
            }
        }
    }
    stream.cancel();
    stream.results(|_| Ok(())).expect_err("canceled stream")
}

fn register_stream_deadline(local: &Arc<TestManager>, remote: &Arc<TestManager>) {
    for manager in [local, remote] {
        manager.register_stream(
            HANDLER_TEST,
            None,
            0,
            1,
            |ctx, _payload, _request, _resp| {
                let _ = ctx.wait_canceled();
                Ok(())
            },
        );
        manager.register_stream(
            HANDLER_TEST2,
            None,
            1,
            1,
            |ctx, _payload, _request, _resp| {
                let _ = ctx.wait_canceled();
                Ok(())
            },
        );
    }
}

fn run_stream_deadline(conn: &TestConnection, handler_id: u8) -> GridError {
    let stream = conn
        .new_stream(
            handler_id,
            TEST_PAYLOAD.as_bytes().to_vec(),
            Some(Duration::from_millis(50)),
        )
        .expect("deadline stream");
    stream
        .results(|_| Ok(()))
        .expect_err("deadline should fire")
}

fn register_server_out_congestion(local: &Arc<TestManager>, remote: &Arc<TestManager>) {
    for manager in [local, remote] {
        manager.register_stream(HANDLER_TEST, None, 0, 1, |ctx, _payload, _request, resp| {
            for i in 0..100u8 {
                ctx.send(&resp, vec![i])?;
            }
            Ok(())
        });
        manager.register_single(HANDLER_TEST2, Ok::<_, GridError>);
    }
}

fn register_server_in_congestion(
    local: &Arc<TestManager>,
    remote: &Arc<TestManager>,
    gate: Arc<AtomicBool>,
) {
    for manager in [local, remote] {
        let gate_clone = gate.clone();
        manager.register_stream(
            HANDLER_TEST,
            None,
            5,
            5,
            move |ctx, _payload, request, resp| {
                while !gate_clone.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(1));
                }
                let mut expected = 0u8;
                while let Some(incoming) = ctx.recv(&request)? {
                    assert_eq!(incoming[0], expected);
                    expected = expected.wrapping_add(1);
                    ctx.send(&resp, incoming)?;
                }
                Ok(())
            },
        );
        manager.register_single(HANDLER_TEST2, Ok::<_, GridError>);
    }
}

fn register_generic_stream(
    local: &Arc<TestManager>,
    remote: &Arc<TestManager>,
    subroute: Option<&str>,
) {
    for manager in [local, remote] {
        manager.register_stream(
            HANDLER_TEST,
            subroute,
            1,
            1,
            |ctx, payload, request, resp| {
                if ctx.subroute().is_some() {
                    assert_eq!(ctx.subroute(), Some("subroute/1"));
                }
                let mut prefix = TestRequest::default();
                prefix.unmarshal_msg(&payload).expect("decode prefix");
                while let Some(incoming) = ctx.recv(&request)? {
                    let mut req = TestRequest::default();
                    req.unmarshal_msg(&incoming).expect("decode request");
                    let response = TestResponse {
                        org_num: req.num + prefix.num,
                        org_string: prefix.string.clone() + &req.string,
                        embedded: req,
                    };
                    ctx.send(&resp, response.marshal_msg().expect("encode response"))?;
                }
                Ok(())
            },
        );
    }
}

fn run_generic_stream(conn: &TestConnection) {
    let prefix = TestRequest {
        num: 1,
        string: TEST_PAYLOAD.to_owned(),
    };
    let mut stream = conn
        .new_stream(
            HANDLER_TEST,
            prefix.marshal_msg().expect("encode prefix"),
            None,
        )
        .expect("new generic stream");
    for i in 0..10 {
        let req = TestRequest {
            num: i,
            string: TEST_PAYLOAD.to_owned(),
        };
        stream.send(req.marshal_msg().expect("encode generic request"));
        let bytes = stream
            .recv()
            .expect("generic response")
            .expect("generic ok");
        let mut decoded = TestResponse::default();
        decoded.unmarshal_msg(&bytes).expect("decode response");
        assert_eq!(decoded.org_num, i + 1);
        assert_eq!(decoded.org_string, format!("{TEST_PAYLOAD}{TEST_PAYLOAD}"));
        assert_eq!(decoded.embedded.num, i);
    }
    stream.close_requests();
    assert!(stream.recv().is_none());
}

fn register_response_blocked(
    local: &Arc<TestManager>,
    remote: &Arc<TestManager>,
    sent: Arc<AtomicBool>,
) {
    for manager in [local, remote] {
        let sent = sent.clone();
        manager.register_stream(
            HANDLER_TEST,
            None,
            0,
            1,
            move |ctx, _payload, _request, resp| {
                for i in 0..100u8 {
                    if i == 1 {
                        sent.store(true, Ordering::SeqCst);
                    }
                    match ctx.send(&resp, vec![i]) {
                        Ok(()) => {}
                        Err(err) => return Err(err),
                    }
                }
                Ok(())
            },
        );
    }
}

fn register_no_ping(
    local: &Arc<TestManager>,
    remote: &Arc<TestManager>,
    in_capacity: usize,
    started: Arc<AtomicBool>,
) {
    for manager in [local, remote] {
        let started = started.clone();
        manager.register_stream(
            HANDLER_TEST,
            None,
            in_capacity,
            1,
            move |ctx, _payload, _request, _resp| {
                started.store(true, Ordering::SeqCst);
                let err = ctx.wait_canceled();
                Err(err)
            },
        );
    }
}

fn register_ping_running(
    local: &Arc<TestManager>,
    remote: &Arc<TestManager>,
    in_capacity: usize,
    block_resp: bool,
    started: Arc<AtomicBool>,
) {
    for manager in [local, remote] {
        let started = started.clone();
        manager.register_stream(
            HANDLER_TEST,
            None,
            in_capacity,
            1,
            move |ctx, _payload, request, resp| {
                started.store(true, Ordering::SeqCst);
                while ctx.canceled().is_none() {
                    if block_resp {
                        let _ = ctx.send(&resp, vec![1]);
                        thread::sleep(Duration::from_millis(10));
                    } else {
                        match ctx.recv(&request) {
                            Ok(Some(_)) => thread::sleep(Duration::from_millis(10)),
                            Ok(None) => thread::sleep(Duration::from_millis(10)),
                            Err(err) => return Err(err),
                        }
                    }
                }
                Err(ctx.wait_canceled())
            },
        );
    }
}

#[test]
fn test_single_roundtrip_line_37() {
    let (local, remote, conn) = setup_test_grid();
    register_single_echo_and_error(&local, &remote);
    run_single_roundtrip_case(&conn, TEST_PAYLOAD.as_bytes().to_vec());
    run_single_roundtrip_err_case(&conn, TEST_PAYLOAD.as_bytes().to_vec());
}

#[test]
fn subtest_test_single_roundtrip_local_to_remote_line_81() {
    let (local, remote, conn) = setup_test_grid();
    register_single_echo_and_error(&local, &remote);
    run_single_roundtrip_case(&conn, TEST_PAYLOAD.as_bytes().to_vec());
}

#[test]
fn subtest_test_single_roundtrip_local_to_remote_err_line_93() {
    let (local, remote, conn) = setup_test_grid();
    register_single_echo_and_error(&local, &remote);
    run_single_roundtrip_err_case(&conn, TEST_PAYLOAD.as_bytes().to_vec());
}

#[test]
fn subtest_test_single_roundtrip_local_to_remote_huge_line_107() {
    let (local, remote, conn) = setup_test_grid();
    register_single_echo_and_error(&local, &remote);
    run_single_roundtrip_case(&conn, vec![b'?'; 1 << 20]);
}

#[test]
fn subtest_test_single_roundtrip_local_to_remote_err_huge_line_119() {
    let (local, remote, conn) = setup_test_grid();
    register_single_echo_and_error(&local, &remote);
    run_single_roundtrip_err_case(&conn, vec![b'!'; 1 << 10]);
}

#[test]
fn test_single_roundtrip_not_ready_line_134() {
    let (local, _remote, conn) = setup_test_grid();
    local.register_single(HANDLER_TEST, Ok::<_, GridError>);
    local.register_single(HANDLER_TEST2, |payload| {
        Err(GridError::remote(String::from_utf8(payload).expect("utf8")))
    });
    assert_eq!(
        conn.request(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec()),
        Err(GridError::remote("unknown handler"))
    );
    assert!(matches!(
        conn.new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None),
        Err(GridError::UnknownHandler)
    ));
}

#[test]
fn subtest_test_single_roundtrip_not_ready_local_to_remote_line_166() {
    let (local, _remote, conn) = setup_test_grid();
    local.register_single(HANDLER_TEST, Ok::<_, GridError>);
    assert_eq!(
        conn.request(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec()),
        Err(GridError::remote("unknown handler"))
    );
}

#[test]
fn test_single_roundtrip_generics_line_182() {
    let (local, remote, conn) = setup_test_grid();
    register_generic_single_handlers(&local, &remote);

    let req = TestRequest {
        num: 1,
        string: TEST_PAYLOAD.to_owned(),
    };
    let bytes = conn
        .request(HANDLER_TEST, req.marshal_msg().expect("encode request"))
        .expect("generic roundtrip");
    let mut resp = TestResponse::default();
    resp.unmarshal_msg(&bytes).expect("decode response");
    assert_eq!(resp.org_num, 1);
    assert_eq!(resp.org_string, TEST_PAYLOAD);

    let err = conn
        .request(
            HANDLER_TEST2,
            TestRequest {
                num: 1,
                string: TEST_PAYLOAD.to_owned(),
            }
            .marshal_msg()
            .expect("encode request"),
        )
        .expect_err("generic remote err");
    assert_eq!(err, GridError::remote(TEST_PAYLOAD));
}

#[test]
fn test_single_roundtrip_generics_recycle_line_251() {
    let (local, remote, conn) = setup_test_grid();
    register_recycle_single_handlers(&local, &remote);

    let bytes = conn
        .request(
            HANDLER_TEST,
            mss(&[("test", TEST_PAYLOAD)])
                .marshal_msg()
                .expect("encode MSS"),
        )
        .expect("MSS roundtrip");
    let mut resp = MSS::default();
    resp.unmarshal_msg(&bytes).expect("decode MSS");
    assert_eq!(resp.get("test"), TEST_PAYLOAD);

    let err = conn
        .request(
            HANDLER_TEST2,
            mss(&[("err", TEST_PAYLOAD)])
                .marshal_msg()
                .expect("encode MSS"),
        )
        .expect_err("MSS remote err");
    assert_eq!(err, GridError::remote(TEST_PAYLOAD));
}

#[test]
fn test_stream_suite_line_313() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_roundtrip(&local, &remote);
    run_stream_roundtrip(&conn);
}

#[test]
fn subtest_test_stream_suite_test_stream_roundtrip_line_333() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_roundtrip(&local, &remote);
    run_stream_roundtrip(&conn);
}

#[test]
fn subtest_test_stream_suite_test_stream_cancel_line_339() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_cancel(&local, &remote);
    assert_eq!(
        run_stream_cancel(&conn, HANDLER_TEST, false),
        GridError::Canceled
    );
}

#[test]
fn subtest_test_stream_suite_test_stream_deadline_line_345() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_deadline(&local, &remote);
    assert_eq!(
        run_stream_deadline(&conn, HANDLER_TEST),
        GridError::DeadlineExceeded
    );
}

#[test]
fn subtest_test_stream_suite_test_server_out_congestion_line_351() {
    let (local, remote, conn) = setup_test_grid();
    register_server_out_congestion(&local, &remote);
    let stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("congestion stream");
    for _ in 0..100 {
        let resp = conn
            .request(HANDLER_TEST2, TEST_PAYLOAD.as_bytes().to_vec())
            .expect("parallel request");
        assert_eq!(resp, TEST_PAYLOAD.as_bytes());
    }
    let mut got = 0u8;
    stream
        .results(|msg| {
            assert_eq!(msg[0], got);
            got = got.wrapping_add(1);
            Ok(())
        })
        .expect("drain responses");
    assert_eq!(got, 100);
}

#[test]
fn subtest_test_stream_suite_test_server_in_congestion_line_357() {
    let (local, remote, conn) = setup_test_grid();
    let gate = Arc::new(AtomicBool::new(false));
    register_server_in_congestion(&local, &remote, gate.clone());
    let mut stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("input congestion stream");
    let send_handle = {
        let tx = stream.requests.as_ref().expect("requests channel").clone();
        thread::spawn(move || {
            for i in 0..100u8 {
                tx.send(vec![i]).expect("send queued input");
            }
        })
    };
    for _ in 0..100 {
        let resp = conn
            .request(HANDLER_TEST2, TEST_PAYLOAD.as_bytes().to_vec())
            .expect("parallel request");
        assert_eq!(resp, TEST_PAYLOAD.as_bytes());
    }
    gate.store(true, Ordering::SeqCst);
    let mut got = 0u8;
    while got < 100 {
        let msg = stream
            .recv()
            .expect("queued response")
            .expect("response ok");
        assert_eq!(msg[0], got);
        got = got.wrapping_add(1);
    }
    send_handle.join().expect("sender join");
    stream.close_requests();
    assert!(stream.recv().is_none());
    assert_eq!(got, 100);
}

#[test]
fn subtest_test_stream_suite_test_generics_stream_roundtrip_line_363() {
    let (local, remote, conn) = setup_test_grid();
    register_generic_stream(&local, &remote, None);
    run_generic_stream(&conn);
}

#[test]
fn subtest_test_stream_suite_test_generics_stream_roundtrip_subroute_line_369() {
    let (local, remote, conn) = setup_test_grid();
    register_generic_stream(&local, &remote, Some("subroute/1"));
    run_generic_stream(&conn.subroute("subroute/1"));
}

#[test]
fn subtest_test_stream_suite_test_server_stream_response_blocked_line_375() {
    let (local, remote, conn) = setup_test_grid();
    let sent = Arc::new(AtomicBool::new(false));
    register_response_blocked(&local, &remote, sent.clone());
    let stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("blocked response stream");
    while !sent.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    let stop = Arc::new(AtomicBool::new(false));
    let stop_reader = stop.clone();
    let read_handle = thread::spawn(move || {
        let _ = stream.results(|_| {
            while !stop_reader.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        });
    });
    thread::sleep(Duration::from_millis(20));
    stop.store(true, Ordering::SeqCst);
    read_handle.join().expect("blocked reader join");
}

#[test]
fn subtest_test_stream_suite_test_server_stream_oneway_no_ping_line_381() {
    let (local, remote, conn) = setup_test_grid();
    let started = Arc::new(AtomicBool::new(false));
    register_no_ping(&local, &remote, 0, started.clone());
    let stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("no ping stream");
    while !started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    stream.cancel();
    let err = stream
        .results(|_| Ok(()))
        .expect_err("expected cancellation");
    assert_eq!(err, GridError::Canceled);
}

#[test]
fn subtest_test_stream_suite_test_server_stream_twoway_no_ping_line_387() {
    let (local, remote, conn) = setup_test_grid();
    let started = Arc::new(AtomicBool::new(false));
    register_no_ping(&local, &remote, 1, started.clone());
    let stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("twoway no ping stream");
    while !started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    stream.cancel();
    let err = stream
        .results(|_| Ok(()))
        .expect_err("expected cancellation");
    assert_eq!(err, GridError::Canceled);
}

#[test]
fn subtest_test_stream_suite_test_server_stream_twoway_ping_line_393() {
    let (local, remote, conn) = setup_test_grid();
    let started = Arc::new(AtomicBool::new(false));
    register_ping_running(&local, &remote, 1, true, started.clone());
    let stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("ping stream");
    while !started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(50));
    stream.cancel();
    assert_eq!(
        stream.results(|_| Ok(())).expect_err("expected cancel"),
        GridError::Canceled
    );
}

#[test]
fn subtest_test_stream_suite_test_server_stream_twoway_ping_req_line_399() {
    let (local, remote, conn) = setup_test_grid();
    let started = Arc::new(AtomicBool::new(false));
    register_ping_running(&local, &remote, 1, false, started.clone());
    let mut stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("ping req stream");
    while !started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    if let Some(requests) = &stream.requests {
        let _ = requests.try_send(vec![1]);
    }
    thread::sleep(Duration::from_millis(50));
    stream.cancel();
    stream.close_requests();
    assert_eq!(
        stream.results(|_| Ok(())).expect_err("expected cancel"),
        GridError::Canceled
    );
}

#[test]
fn subtest_test_stream_suite_test_server_stream_twoway_ping_resp_line_405() {
    let (local, remote, conn) = setup_test_grid();
    let started = Arc::new(AtomicBool::new(false));
    register_ping_running(&local, &remote, 1, true, started.clone());
    let stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("ping resp stream");
    while !started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(50));
    stream.cancel();
    assert_eq!(
        stream.results(|_| Ok(())).expect_err("expected cancel"),
        GridError::Canceled
    );
}

#[test]
fn subtest_test_stream_suite_test_server_stream_twoway_ping_req_resp_line_411() {
    let (local, remote, conn) = setup_test_grid();
    let started = Arc::new(AtomicBool::new(false));
    register_ping_running(&local, &remote, 1, true, started.clone());
    let mut stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("ping req resp stream");
    while !started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    if let Some(requests) = &stream.requests {
        let _ = requests.try_send(vec![1]);
    }
    thread::sleep(Duration::from_millis(50));
    stream.cancel();
    stream.close_requests();
    assert_eq!(
        stream.results(|_| Ok(())).expect_err("expected cancel"),
        GridError::Canceled
    );
}

#[test]
fn subtest_test_stream_suite_test_server_stream_oneway_ping_line_417() {
    let (local, remote, conn) = setup_test_grid();
    let started = Arc::new(AtomicBool::new(false));
    register_ping_running(&local, &remote, 0, false, started.clone());
    let stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("oneway ping stream");
    while !started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(50));
    stream.cancel();
    assert_eq!(
        stream.results(|_| Ok(())).expect_err("expected cancel"),
        GridError::Canceled
    );
}

#[test]
fn subtest_test_stream_suite_test_server_stream_oneway_ping_unblocked_line_423() {
    let (local, remote, conn) = setup_test_grid();
    let started = Arc::new(AtomicBool::new(false));
    register_ping_running(&local, &remote, 0, false, started.clone());
    let stream = conn
        .new_stream(HANDLER_TEST, TEST_PAYLOAD.as_bytes().to_vec(), None)
        .expect("oneway ping unblocked stream");
    while !started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(50));
    stream.cancel();
    assert_eq!(
        stream.results(|_| Ok(())).expect_err("expected cancel"),
        GridError::Canceled
    );
}

#[test]
fn subtest_file_scope_unbuffered_line_586() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_cancel(&local, &remote);
    assert_eq!(
        run_stream_cancel(&conn, HANDLER_TEST, false),
        GridError::Canceled
    );
}

#[test]
fn subtest_file_scope_buffered_line_589() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_cancel(&local, &remote);
    assert_eq!(
        run_stream_cancel(&conn, HANDLER_TEST2, false),
        GridError::Canceled
    );
}

#[test]
fn subtest_file_scope_buffered_line_592() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_cancel(&local, &remote);
    assert_eq!(
        run_stream_cancel(&conn, HANDLER_TEST2, true),
        GridError::Canceled
    );
}

#[test]
fn subtest_file_scope_unbuffered_line_682() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_deadline(&local, &remote);
    assert_eq!(
        run_stream_deadline(&conn, HANDLER_TEST),
        GridError::DeadlineExceeded
    );
}

#[test]
fn subtest_file_scope_buffered_line_686() {
    let (local, remote, conn) = setup_test_grid();
    register_stream_deadline(&local, &remote);
    assert_eq!(
        run_stream_deadline(&conn, HANDLER_TEST2),
        GridError::DeadlineExceeded
    );
}
