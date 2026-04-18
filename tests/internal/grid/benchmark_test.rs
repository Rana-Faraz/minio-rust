use minio_rust::internal::grid::{
    Connection, Flags, HandlerID, Message, Op, TestRequest, TestResponse,
};

pub const SOURCE_FILE: &str = "internal/grid/benchmark_test.go";

fn sample_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn request_bytes_smoke(servers: usize, parallelism: usize) {
    let payload = sample_payload(512);
    let mut ops = 0usize;
    for src in 0..servers {
        for dst in 0..servers {
            if src == dst {
                continue;
            }
            for seq in 0..parallelism.max(1) {
                let mut message = Message {
                    mux_id: (src as u64) << 32 | dst as u64,
                    seq: seq as u32,
                    deadline_ms: 5_000,
                    handler: HandlerID(2),
                    op: Op(15),
                    flags: Flags(0),
                    payload: Some(payload.clone()),
                };
                message.set_zero_payload_flag();
                let bytes = message.marshal_msg().expect("marshal message");
                let mut decoded = Message::default();
                decoded.unmarshal_msg(&bytes).expect("unmarshal message");
                assert_eq!(decoded.payload, Some(payload.clone()));
                ops += 1;
            }
        }
    }
    assert!(ops > 0);
}

fn request_rpc_smoke(servers: usize, parallelism: usize) {
    let mut ops = 0usize;
    for src in 0..servers {
        for dst in 0..servers {
            if src == dst {
                continue;
            }
            for n in 0..parallelism.max(1) {
                let req = TestRequest {
                    num: (src + dst + n) as i32,
                    string: format!("rpc-{src}-{dst}-{n}"),
                };
                let resp = TestResponse {
                    org_num: req.num,
                    org_string: req.string.clone(),
                    embedded: req.clone(),
                };
                let req_bytes = req.marshal_msg().expect("marshal request");
                let resp_bytes = resp.marshal_msg().expect("marshal response");
                let mut decoded_req = TestRequest::default();
                let mut decoded_resp = TestResponse::default();
                decoded_req
                    .unmarshal_msg(&req_bytes)
                    .expect("unmarshal request");
                decoded_resp
                    .unmarshal_msg(&resp_bytes)
                    .expect("unmarshal response");
                assert_eq!(decoded_req, req);
                assert_eq!(decoded_resp, resp);
                ops += 1;
            }
        }
    }
    assert!(ops > 0);
}

fn stream_responses_smoke(servers: usize, parallelism: usize) {
    let payload = sample_payload(128);
    let responses = 10usize;
    let mut ops = 0usize;
    for src in 0..servers {
        for dst in 0..servers {
            if src == dst {
                continue;
            }
            for seq in 0..parallelism.max(1) {
                for i in 0..responses {
                    let response_payload = [vec![i as u8], payload.clone()].concat();
                    let message = Message {
                        mux_id: (src as u64) << 32 | dst as u64,
                        seq: seq as u32,
                        deadline_ms: 5_000,
                        handler: HandlerID(1),
                        op: Op(4),
                        flags: Flags(0),
                        payload: Some(response_payload.clone()),
                    };
                    let bytes = message.marshal_msg().expect("marshal stream response");
                    let mut decoded = Message::default();
                    decoded
                        .unmarshal_msg(&bytes)
                        .expect("unmarshal stream response");
                    assert_eq!(decoded.payload, Some(response_payload));
                    ops += 1;
                }
            }
        }
    }
    assert!(ops >= responses);
}

fn stream_requests_smoke(servers: usize, parallelism: usize) {
    let payload = sample_payload(128);
    let requests = 10usize;
    let mut ops = 0usize;
    for src in 0..servers {
        for dst in 0..servers {
            if src == dst {
                continue;
            }
            let conn = Connection::new(format!("host-{src}"), format!("host-{dst}"));
            assert!(conn.wait_for_connect(std::time::Duration::from_millis(10)));
            for _ in 0..parallelism.max(1) {
                for _ in 0..requests {
                    let pending = conn.request();
                    conn.disconnect();
                    assert_eq!(pending.wait(), Err("remote disconnected".to_owned()));
                    conn.reconnect();
                    let message = Message {
                        mux_id: 1,
                        seq: 1,
                        deadline_ms: 1_000,
                        handler: HandlerID(1),
                        op: Op(3),
                        flags: Flags(0),
                        payload: Some(payload.clone()),
                    };
                    let bytes = message.marshal_msg().expect("marshal stream request");
                    let mut decoded = Message::default();
                    decoded
                        .unmarshal_msg(&bytes)
                        .expect("unmarshal stream request");
                    assert_eq!(decoded.payload, Some(payload.clone()));
                    ops += 1;
                }
            }
        }
    }
    assert!(ops >= requests);
}

fn stream_twoway_smoke(servers: usize, parallelism: usize) {
    let payload = sample_payload(96);
    let messages = 10usize;
    let mut ops = 0usize;
    for src in 0..servers {
        for dst in 0..servers {
            if src == dst {
                continue;
            }
            for seq in 0..parallelism.max(1) {
                for i in 0..messages {
                    let outbound = Message {
                        mux_id: (src as u64) << 32 | dst as u64,
                        seq: (seq * messages + i) as u32,
                        deadline_ms: 2_500,
                        handler: HandlerID(9),
                        op: Op(9),
                        flags: Flags(0),
                        payload: Some(payload.clone()),
                    };
                    let inbound = Message {
                        mux_id: outbound.mux_id,
                        seq: outbound.seq,
                        deadline_ms: outbound.deadline_ms,
                        handler: outbound.handler,
                        op: outbound.op,
                        flags: outbound.flags,
                        payload: outbound.payload.clone(),
                    };
                    let mut decoded = Message::default();
                    decoded
                        .unmarshal_msg(&inbound.marshal_msg().expect("marshal twoway"))
                        .expect("unmarshal twoway");
                    assert_eq!(decoded.payload, outbound.payload);
                    ops += 2;
                }
            }
        }
    }
    assert!(ops >= messages * 2);
}

#[test]
fn benchmark_requests_line_33() {
    for servers in [2, 4, 8] {
        request_bytes_smoke(servers, 1);
        request_rpc_smoke(servers, 1);
    }
}

#[test]
fn subbenchmark_benchmark_requests_servers_line_35() {
    request_bytes_smoke(8, 1);
    request_rpc_smoke(8, 1);
}

#[test]
fn subbenchmark_file_scope_bytes_line_77() {
    request_bytes_smoke(4, 1);
}

#[test]
fn subbenchmark_file_scope_par_line_79() {
    request_bytes_smoke(4, 4);
}

#[test]
fn subbenchmark_file_scope_rpc_line_134() {
    request_rpc_smoke(4, 1);
}

#[test]
fn subbenchmark_file_scope_par_line_136() {
    request_rpc_smoke(4, 4);
}

#[test]
fn benchmark_stream_line_196() {
    for servers in [2, 4, 8] {
        stream_requests_smoke(servers, 1);
        stream_responses_smoke(servers, 1);
        stream_twoway_smoke(servers, 1);
    }
}

#[test]
fn subbenchmark_benchmark_stream_test_name_line_206() {
    stream_requests_smoke(4, 1);
    stream_responses_smoke(4, 1);
    stream_twoway_smoke(4, 1);
}

#[test]
fn subbenchmark_benchmark_stream_servers_line_208() {
    stream_requests_smoke(8, 1);
    stream_responses_smoke(8, 1);
    stream_twoway_smoke(8, 1);
}

#[test]
fn subbenchmark_file_scope_par_line_262() {
    stream_responses_smoke(4, 4);
}

#[test]
fn subbenchmark_file_scope_par_line_373() {
    stream_requests_smoke(4, 4);
}

#[test]
fn subbenchmark_file_scope_par_line_489() {
    stream_twoway_smoke(4, 4);
}
