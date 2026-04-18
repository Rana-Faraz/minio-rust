use super::*;
use crate::internal::event::target::{Host, NatsArgs, NsqArgs};
use crate::internal::event::{Config as EventConfig, Name, TargetId};
use rustls::pki_types::PrivateKeyDer;
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;
use tiny_http::{Response, Server};

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn fixture(path: &str) -> String {
    format!("{}/tests/fixtures/nats/{path}", env!("CARGO_MANIFEST_DIR"))
}

fn ensure_rustls_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_certificates(path: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    let file = std::fs::File::open(path).expect("certificate file should open");
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("certificates should parse")
}

fn load_private_key(path: &str) -> PrivateKeyDer<'static> {
    let file = std::fs::File::open(path).expect("private key file should open");
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .expect("private key should parse")
        .expect("private key should exist")
}

fn notification_config_xml() -> &'static str {
    r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <QueueConfiguration>
    <Id>hook</Id>
    <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
    <Event>s3:ObjectCreated:Put</Event>
    <Filter>
      <S3Key>
        <FilterRule>
          <Name>prefix</Name>
          <Value>photos/</Value>
        </FilterRule>
      </S3Key>
    </Filter>
  </QueueConfiguration>
  <QueueConfiguration>
    <Id>queue</Id>
    <Queue>arn:minio:sqs:us-east-1:2:queue</Queue>
    <Event>s3:ObjectCreated:Put</Event>
    <Event>s3:ObjectRemoved:Delete</Event>
  </QueueConfiguration>
</NotificationConfiguration>
"#
}

#[test]
fn notification_registry_validates_config_and_matches_targets() {
    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(WebhookNotificationTarget {
            target_id: TargetId::new("1", "webhook"),
            endpoint: "http://127.0.0.1:9/events".to_string(),
            auth_token: String::new(),
        })
        .expect("register webhook");
    let queue = InMemoryQueueTarget::new(TargetId::new("2", "queue"));
    registry.register(queue).expect("register queue");

    let config = EventConfig::unmarshal_xml(notification_config_xml().as_bytes())
        .expect("notification config");
    registry
        .validate_config(&config)
        .expect("config should validate");

    let event = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "photos/image.jpg");
    let matched = registry.matching_target_ids(&config, &event);
    let matched = matched
        .into_iter()
        .map(|target| target.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        matched,
        vec!["1:webhook".to_string(), "2:queue".to_string()]
    );

    let other = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "docs/readme.txt");
    let matched = registry
        .matching_target_ids(&config, &other)
        .into_iter()
        .map(|target| target.to_string())
        .collect::<Vec<_>>();
    assert_eq!(matched, vec!["2:queue".to_string()]);
}

#[test]
fn notification_registry_dispatches_to_webhook_and_queue_targets() {
    let server = Server::http("127.0.0.1:0").expect("webhook server");
    let addr = server.server_addr().to_ip().expect("socket addr");
    let (tx, rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let mut request = server.recv().expect("receive webhook request");
        let auth = request
            .headers()
            .iter()
            .find(|header| header.field.equiv("authorization"))
            .map(|header| header.value.as_str().to_string())
            .unwrap_or_default();
        let mut body = String::new();
        request
            .as_reader()
            .read_to_string(&mut body)
            .expect("read webhook body");
        request
            .respond(Response::from_string("ok"))
            .expect("respond webhook");
        tx.send((auth, body)).expect("send webhook");
    });

    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(WebhookNotificationTarget {
            target_id: TargetId::new("1", "webhook"),
            endpoint: format!("http://{addr}/events"),
            auth_token: "secret-token".to_string(),
        })
        .expect("register webhook");
    let queue = InMemoryQueueTarget::new(TargetId::new("2", "queue"));
    registry
        .register(queue.clone())
        .expect("register queue target");

    let config = EventConfig::unmarshal_xml(notification_config_xml().as_bytes())
        .expect("notification config");
    let mut event = NotificationEvent::new(Name::ObjectCreatedPut, "photos", "photos/image.jpg");
    event.version_id = "version-1".to_string();
    event
        .metadata
        .insert("content-type".to_string(), "image/jpeg".to_string());

    let report = registry
        .dispatch(&config, &event)
        .expect("dispatch notifications");
    assert_eq!(
        report.matched_targets,
        vec!["1:webhook".to_string(), "2:queue".to_string()]
    );
    assert_eq!(report.deliveries.len(), 2);
    assert!(report.deliveries.iter().all(|delivery| delivery.delivered));

    let queued = queue.snapshot();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].event.event_name, Name::ObjectCreatedPut);
    assert_eq!(queued[0].event.bucket, "photos");
    assert_eq!(queued[0].event.object, "photos/image.jpg");

    let (auth, body) = rx.recv().expect("webhook body");
    assert_eq!(auth, "Bearer secret-token");
    assert!(body.contains("s3:ObjectCreated:Put"));
    assert!(body.contains("photos/photos/image.jpg"));
    assert!(body.contains("\"versionId\":\"version-1\""));

    join.join().expect("webhook thread");
}

#[test]
fn notification_registry_reports_partial_target_failures() {
    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(WebhookNotificationTarget {
            target_id: TargetId::new("1", "webhook"),
            endpoint: "http://127.0.0.1:1/unreachable".to_string(),
            auth_token: String::new(),
        })
        .expect("register webhook");
    let queue = InMemoryQueueTarget::new(TargetId::new("2", "queue"));
    registry
        .register(queue.clone())
        .expect("register queue target");

    let config = EventConfig::unmarshal_xml(notification_config_xml().as_bytes())
        .expect("notification config");
    let event = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "photos/retry.jpg");

    let report = registry
        .dispatch(&config, &event)
        .expect("dispatch should return report");
    assert_eq!(report.deliveries.len(), 2);
    assert!(report.deliveries.iter().any(|delivery| {
        delivery.target_id == "1:webhook" && !delivery.delivered && !delivery.error.is_empty()
    }));
    assert!(report
        .deliveries
        .iter()
        .any(|delivery| { delivery.target_id == "2:queue" && delivery.delivered }));

    let queued = queue.drain();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].event.object, "photos/retry.jpg");
}

#[test]
fn notification_registry_supports_shared_registration_and_xml_dispatch() {
    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(WebhookNotificationTarget {
            target_id: TargetId::new("1", "webhook"),
            endpoint: "http://127.0.0.1:9/events".to_string(),
            auth_token: String::new(),
        })
        .expect("register webhook");
    let queue = Arc::new(InMemoryQueueTarget::new(TargetId::new("2", "queue")));
    registry
        .register_shared(queue.clone())
        .expect("register shared queue");

    let event = NotificationEvent::new(Name::ObjectRemovedDelete, "bucket", "docs/readme.txt");
    let report = registry
        .dispatch_xml(notification_config_xml().as_bytes(), &event)
        .expect("dispatch xml");

    assert_eq!(report.matched_targets, vec!["2:queue".to_string()]);
    assert_eq!(report.deliveries.len(), 1);
    assert!(report.deliveries[0].delivered);

    let snapshot = queue.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].event.event_name, Name::ObjectRemovedDelete);
}

#[test]
fn notification_registry_dispatches_to_nats_target() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("nats listener");
    let port = listener.local_addr().expect("listener addr").port();
    let (tx, rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept nats");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set nats read timeout");
        stream.write_all(b"INFO {}\r\n").expect("write nats info");
        let mut frames = String::new();
        let mut buffer = [0u8; 4096];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    frames.push_str(&String::from_utf8_lossy(&buffer[..read]));
                    if frames.contains("PUB minio.events ") {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read nats frames: {error}"),
            }
        }
        tx.send(frames).expect("send nats frames");
    });

    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(NatsNotificationTarget {
            target_id: TargetId::new("3", "nats"),
            args: NatsArgs {
                enable: true,
                address: Host::new("127.0.0.1", port),
                subject: "minio.events".to_string(),
                ..Default::default()
            },
        })
        .expect("register nats");

    let config = EventConfig::unmarshal_xml(
        r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <QueueConfiguration>
    <Id>nats</Id>
    <Queue>arn:minio:sqs:us-east-1:3:nats</Queue>
    <Event>s3:ObjectCreated:Put</Event>
  </QueueConfiguration>
</NotificationConfiguration>
"#
        .as_bytes(),
    )
    .expect("notification config");
    let event = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "photos/image.jpg");

    let report = registry.dispatch(&config, &event).expect("dispatch nats");
    assert_eq!(report.matched_targets, vec!["3:nats".to_string()]);
    assert_eq!(report.deliveries.len(), 1);
    assert!(report.deliveries[0].delivered);
    assert_eq!(
        report.deliveries[0]
            .detail
            .get("subject")
            .map(String::as_str),
        Some("minio.events")
    );

    let frames = rx.recv().expect("nats frames");
    assert!(frames.contains("CONNECT "));
    assert!(frames.contains("PUB minio.events "));
    assert!(frames.contains("s3:ObjectCreated:Put"));
    join.join().expect("nats thread");
}

#[test]
fn notification_registry_dispatches_to_nats_tls_target() {
    ensure_rustls_provider();
    let listener = TcpListener::bind("127.0.0.1:0").expect("tls nats listener");
    let port = listener.local_addr().expect("listener addr").port();
    let cert = fixture("certs/nats_server_cert.pem");
    let key = fixture("certs/nats_server_key.pem");
    let (tx, rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept tls nats");
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(load_certificates(&cert), load_private_key(&key))
            .expect("server config");
        let connection = ServerConnection::new(Arc::new(config)).expect("server connection");
        let mut tls = StreamOwned::new(connection, stream);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).expect("handshake");
        }
        tls.write_all(b"INFO {}\r\n").expect("write nats info");
        tls.flush().expect("flush nats info");
        let mut frames = String::new();
        let mut buffer = [0u8; 4096];
        loop {
            match tls.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    frames.push_str(&String::from_utf8_lossy(&buffer[..read]));
                    if frames.contains("PUB tls.events ") {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read tls nats frames: {error}"),
            }
        }
        tx.send(frames).expect("send tls nats frames");
    });

    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(NatsNotificationTarget {
            target_id: TargetId::new("33", "nats"),
            args: NatsArgs {
                enable: true,
                address: Host::new("127.0.0.1", port),
                subject: "tls.events".to_string(),
                secure: true,
                tls_skip_verify: true,
                ..Default::default()
            },
        })
        .expect("register tls nats");

    let config = EventConfig::unmarshal_xml(
        r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <QueueConfiguration>
    <Id>nats</Id>
    <Queue>arn:minio:sqs:us-east-1:33:nats</Queue>
    <Event>s3:ObjectCreated:Put</Event>
  </QueueConfiguration>
</NotificationConfiguration>
"#
        .as_bytes(),
    )
    .expect("notification config");
    let event = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "photos/image.jpg");

    let report = registry
        .dispatch(&config, &event)
        .expect("dispatch tls nats");
    assert_eq!(report.matched_targets, vec!["33:nats".to_string()]);
    assert_eq!(report.deliveries.len(), 1);
    assert!(report.deliveries[0].delivered);
    assert_eq!(
        report.deliveries[0]
            .detail
            .get("subject")
            .map(String::as_str),
        Some("tls.events")
    );

    let frames = rx.recv().expect("tls nats frames");
    assert!(frames.contains("CONNECT "));
    assert!(frames.contains("PUB tls.events "));
    assert!(frames.contains("s3:ObjectCreated:Put"));
    join.join().expect("tls nats thread");
}

#[test]
fn notification_registry_dispatches_to_nsq_target() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("nsq listener");
    let port = listener.local_addr().expect("listener addr").port();
    let (tx, rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept nsq");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set nsq read timeout");
        let mut frames = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    frames.extend_from_slice(&buffer[..read]);
                    if String::from_utf8_lossy(&frames).contains("s3:ObjectCreated:Put") {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read nsq frames: {error}"),
            }
        }
        tx.send(frames).expect("send nsq frames");
    });

    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(NsqNotificationTarget {
            target_id: TargetId::new("4", "nsq"),
            args: NsqArgs {
                enable: true,
                nsqd_address: Host::new("127.0.0.1", port),
                topic: "minio-events".to_string(),
                ..Default::default()
            },
        })
        .expect("register nsq");

    let config = EventConfig::unmarshal_xml(
        r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <QueueConfiguration>
    <Id>nsq</Id>
    <Queue>arn:minio:sqs:us-east-1:4:nsq</Queue>
    <Event>s3:ObjectCreated:Put</Event>
  </QueueConfiguration>
</NotificationConfiguration>
"#
        .as_bytes(),
    )
    .expect("notification config");
    let event = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "photos/image.jpg");

    let report = registry.dispatch(&config, &event).expect("dispatch nsq");
    assert_eq!(report.matched_targets, vec!["4:nsq".to_string()]);
    assert_eq!(report.deliveries.len(), 1);
    assert!(report.deliveries[0].delivered);
    assert_eq!(
        report.deliveries[0].detail.get("topic").map(String::as_str),
        Some("minio-events")
    );

    let frames = rx.recv().expect("nsq frames");
    assert!(frames.starts_with(b"  V2"));
    assert!(String::from_utf8_lossy(&frames).contains("PUB minio-events\n"));
    assert!(String::from_utf8_lossy(&frames).contains("s3:ObjectCreated:Put"));
    join.join().expect("nsq thread");
}

#[test]
fn notification_registry_dispatches_to_nsq_tls_target() {
    ensure_rustls_provider();
    let listener = TcpListener::bind("127.0.0.1:0").expect("tls nsq listener");
    let port = listener.local_addr().expect("listener addr").port();
    let cert = fixture("certs/nats_server_cert.pem");
    let key = fixture("certs/nats_server_key.pem");
    let (tx, rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept tls nsq");
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(load_certificates(&cert), load_private_key(&key))
            .expect("server config");
        let connection = ServerConnection::new(Arc::new(config)).expect("server connection");
        let mut tls = StreamOwned::new(connection, stream);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).expect("handshake");
        }
        let mut frames = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            match tls.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    frames.extend_from_slice(&buffer[..read]);
                    if String::from_utf8_lossy(&frames).contains("s3:ObjectCreated:Put") {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read tls nsq frames: {error}"),
            }
        }
        tx.send(frames).expect("send tls nsq frames");
    });

    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(NsqNotificationTarget {
            target_id: TargetId::new("5", "nsq"),
            args: NsqArgs {
                enable: true,
                nsqd_address: Host::new("127.0.0.1", port),
                topic: "tls-events".to_string(),
                tls: crate::internal::event::target::NsqTlsArgs {
                    enable: true,
                    skip_verify: true,
                },
                ..Default::default()
            },
        })
        .expect("register tls nsq");

    let config = EventConfig::unmarshal_xml(
        r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <QueueConfiguration>
    <Id>nsq</Id>
    <Queue>arn:minio:sqs:us-east-1:5:nsq</Queue>
    <Event>s3:ObjectCreated:Put</Event>
  </QueueConfiguration>
</NotificationConfiguration>
"#
        .as_bytes(),
    )
    .expect("notification config");
    let event = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "photos/image.jpg");

    let report = registry
        .dispatch(&config, &event)
        .expect("dispatch tls nsq");
    assert_eq!(report.matched_targets, vec!["5:nsq".to_string()]);
    assert_eq!(report.deliveries.len(), 1);
    assert!(report.deliveries[0].delivered);
    assert_eq!(
        report.deliveries[0].detail.get("topic").map(String::as_str),
        Some("tls-events")
    );

    let frames = rx.recv().expect("tls nsq frames");
    assert!(String::from_utf8_lossy(&frames).contains("PUB tls-events\n"));
    assert!(String::from_utf8_lossy(&frames).contains("s3:ObjectCreated:Put"));
    join.join().expect("tls nsq thread");
}

#[test]
fn notification_registry_dispatches_to_elasticsearch_target() {
    let server = Server::http("127.0.0.1:0").expect("elastic server");
    let addr = server.server_addr().to_ip().expect("socket addr");
    let (tx, rx) = mpsc::channel();
    let receiver = thread::spawn(move || {
        let mut request = server.recv().expect("elastic request");
        let url = request.url().to_string();
        let auth = request
            .headers()
            .iter()
            .find(|header| {
                header
                    .field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("authorization")
            })
            .map(|header| header.value.as_str().to_string())
            .unwrap_or_default();
        let mut body = Vec::new();
        request
            .as_reader()
            .read_to_end(&mut body)
            .expect("read elastic body");
        request
            .respond(Response::from_string("{\"result\":\"created\"}"))
            .expect("respond elastic");
        tx.send((url, auth, body)).expect("send elastic payload");
    });

    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(ElasticsearchNotificationTarget {
            target_id: TargetId::new("6", "elasticsearch"),
            args: ElasticsearchArgs {
                enable: true,
                endpoint: format!("http://{}", addr),
                index: "minio-events".to_string(),
                username: "elastic".to_string(),
                password: "secret".to_string(),
                ..Default::default()
            },
        })
        .expect("register elastic");

    let config = EventConfig::unmarshal_xml(
        r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <QueueConfiguration>
    <Id>elastic</Id>
    <Queue>arn:minio:sqs:us-east-1:6:elasticsearch</Queue>
    <Event>s3:ObjectCreated:Put</Event>
  </QueueConfiguration>
</NotificationConfiguration>
"#
        .as_bytes(),
    )
    .expect("notification config");
    let event = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "photos/image.jpg");

    let report = registry
        .dispatch(&config, &event)
        .expect("dispatch elastic");
    assert_eq!(report.matched_targets, vec!["6:elasticsearch".to_string()]);
    assert_eq!(report.deliveries.len(), 1);
    assert!(report.deliveries[0].delivered);
    assert_eq!(
        report.deliveries[0].detail.get("index").map(String::as_str),
        Some("minio-events")
    );

    let (url, auth, body) = rx.recv().expect("elastic payload");
    assert_eq!(url, "/minio-events/_doc");
    assert!(auth.starts_with("Basic "), "{auth}");
    let body = String::from_utf8(body).expect("elastic body utf8");
    assert!(body.contains("s3:ObjectCreated:Put"));
    assert!(body.contains("photos/image.jpg"));
    receiver.join().expect("join elastic thread");
}

#[test]
fn notification_registry_dispatches_to_redis_target() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("redis listener");
    let port = listener.local_addr().expect("listener addr").port();
    let (tx, rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept redis");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set redis timeout");
        let mut frames = Vec::new();
        let mut buffer = [0u8; 4096];
        let mut replied_auth = false;
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    frames.extend_from_slice(&buffer[..read]);
                    if frames.windows(5).any(|window| window == b"RPUSH") {
                        stream.write_all(b"+OK\r\n").expect("write redis ok");
                        break;
                    } else if frames.windows(4).any(|window| window == b"AUTH") && !replied_auth {
                        stream.write_all(b"+OK\r\n").expect("write auth ok");
                        replied_auth = true;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("read redis frames: {error}"),
            }
        }
        tx.send(frames).expect("send redis frames");
    });

    let mut registry = NotificationTargetRegistry::new("us-east-1");
    registry
        .register(RedisNotificationTarget {
            target_id: TargetId::new("7", "redis"),
            args: RedisArgs {
                enable: true,
                address: format!("127.0.0.1:{port}"),
                key: "minio-events".to_string(),
                password: "secret".to_string(),
                ..Default::default()
            },
        })
        .expect("register redis");

    let config = EventConfig::unmarshal_xml(
        r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <QueueConfiguration>
    <Id>redis</Id>
    <Queue>arn:minio:sqs:us-east-1:7:redis</Queue>
    <Event>s3:ObjectCreated:Put</Event>
  </QueueConfiguration>
</NotificationConfiguration>
"#
        .as_bytes(),
    )
    .expect("notification config");
    let event = NotificationEvent::new(Name::ObjectCreatedPut, "bucket", "photos/image.jpg");

    let report = registry.dispatch(&config, &event).expect("dispatch redis");
    assert_eq!(report.matched_targets, vec!["7:redis".to_string()]);
    assert_eq!(report.deliveries.len(), 1);
    assert!(report.deliveries[0].delivered);
    assert_eq!(
        report.deliveries[0].detail.get("key").map(String::as_str),
        Some("minio-events")
    );

    let frames = String::from_utf8(rx.recv().expect("redis frames")).expect("redis utf8");
    assert!(frames.contains("AUTH"));
    assert!(frames.contains("RPUSH"));
    assert!(frames.contains("minio-events"));
    assert!(frames.contains("s3:ObjectCreated:Put"));
    join.join().expect("redis thread");
}

#[test]
fn notification_loader_reads_nats_tls_skip_verify_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_NATS_ENABLE_19", "on");
        std::env::set_var("MINIO_NOTIFY_NATS_ADDRESS_19", "127.0.0.1:4222");
        std::env::set_var("MINIO_NOTIFY_NATS_SUBJECT_19", "tls.events");
        std::env::set_var("MINIO_NOTIFY_NATS_SECURE_19", "on");
        std::env::set_var("MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY_19", "on");
    }

    let targets = load_nats_targets_from_env().expect("load nats targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "19:nats")
        .expect("configured target");
    assert!(target.args.secure);
    assert!(target.args.tls_skip_verify);

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_NATS_ENABLE_19");
        std::env::remove_var("MINIO_NOTIFY_NATS_ADDRESS_19");
        std::env::remove_var("MINIO_NOTIFY_NATS_SUBJECT_19");
        std::env::remove_var("MINIO_NOTIFY_NATS_SECURE_19");
        std::env::remove_var("MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY_19");
    }
}

#[test]
fn notification_loader_reads_elasticsearch_target_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_ENABLE_29", "on");
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_URL_29", "http://127.0.0.1:9200");
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_INDEX_29", "minio-events");
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_USERNAME_29", "elastic");
        std::env::set_var("MINIO_NOTIFY_ELASTICSEARCH_PASSWORD_29", "secret");
    }

    let targets = load_elasticsearch_targets_from_env().expect("load elastic targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "29:elasticsearch")
        .expect("configured target");
    assert_eq!(target.args.endpoint, "http://127.0.0.1:9200");
    assert_eq!(target.args.index, "minio-events");
    assert_eq!(target.args.username, "elastic");

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_ENABLE_29");
        std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_URL_29");
        std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_INDEX_29");
        std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_USERNAME_29");
        std::env::remove_var("MINIO_NOTIFY_ELASTICSEARCH_PASSWORD_29");
    }
}

#[test]
fn notification_loader_reads_redis_target_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_REDIS_ENABLE_39", "on");
        std::env::set_var("MINIO_NOTIFY_REDIS_ADDRESS_39", "127.0.0.1:6379");
        std::env::set_var("MINIO_NOTIFY_REDIS_KEY_39", "minio-events");
        std::env::set_var("MINIO_NOTIFY_REDIS_PASSWORD_39", "secret");
    }

    let targets = load_redis_targets_from_env().expect("load redis targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "39:redis")
        .expect("configured target");
    assert_eq!(target.args.address, "127.0.0.1:6379");
    assert_eq!(target.args.key, "minio-events");
    assert_eq!(target.args.password, "secret");

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_REDIS_ENABLE_39");
        std::env::remove_var("MINIO_NOTIFY_REDIS_ADDRESS_39");
        std::env::remove_var("MINIO_NOTIFY_REDIS_KEY_39");
        std::env::remove_var("MINIO_NOTIFY_REDIS_PASSWORD_39");
    }
}

#[test]
fn notification_loader_reads_nsq_tls_skip_verify_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_NSQ_ENABLE_9", "on");
        std::env::set_var("MINIO_NOTIFY_NSQ_NSQD_ADDRESS_9", "127.0.0.1:4150");
        std::env::set_var("MINIO_NOTIFY_NSQ_TOPIC_9", "tls-topic");
        std::env::set_var("MINIO_NOTIFY_NSQ_TLS_ENABLE_9", "on");
        std::env::set_var("MINIO_NOTIFY_NSQ_TLS_SKIP_VERIFY_9", "on");
    }

    let targets = load_nsq_targets_from_env().expect("load nsq targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "9:nsq")
        .expect("configured target");
    assert!(target.args.tls.enable);
    assert!(target.args.tls.skip_verify);

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_NSQ_ENABLE_9");
        std::env::remove_var("MINIO_NOTIFY_NSQ_NSQD_ADDRESS_9");
        std::env::remove_var("MINIO_NOTIFY_NSQ_TOPIC_9");
        std::env::remove_var("MINIO_NOTIFY_NSQ_TLS_ENABLE_9");
        std::env::remove_var("MINIO_NOTIFY_NSQ_TLS_SKIP_VERIFY_9");
    }
}

#[test]
fn notification_loader_reads_mysql_target_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_MYSQL_ENABLE_49", "on");
        std::env::set_var("MINIO_NOTIFY_MYSQL_TABLE_49", "events");
        std::env::set_var("MINIO_NOTIFY_MYSQL_HOST_49", "127.0.0.1");
        std::env::set_var("MINIO_NOTIFY_MYSQL_PORT_49", "3306");
        std::env::set_var("MINIO_NOTIFY_MYSQL_DATABASE_49", "minio");
        std::env::set_var("MINIO_NOTIFY_MYSQL_USERNAME_49", "minio");
    }

    let targets = load_mysql_targets_from_env().expect("load mysql targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "49:mysql")
        .expect("configured target");
    assert_eq!(target.args.table, "events");
    assert_eq!(target.args.host, "127.0.0.1");
    assert_eq!(target.args.port, "3306");
    assert_eq!(target.args.database, "minio");
    assert_eq!(target.args.username, "minio");

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_MYSQL_ENABLE_49");
        std::env::remove_var("MINIO_NOTIFY_MYSQL_TABLE_49");
        std::env::remove_var("MINIO_NOTIFY_MYSQL_HOST_49");
        std::env::remove_var("MINIO_NOTIFY_MYSQL_PORT_49");
        std::env::remove_var("MINIO_NOTIFY_MYSQL_DATABASE_49");
        std::env::remove_var("MINIO_NOTIFY_MYSQL_USERNAME_49");
    }
}

#[test]
fn notification_loader_reads_postgresql_target_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_POSTGRES_ENABLE_59", "on");
        std::env::set_var("MINIO_NOTIFY_POSTGRES_TABLE_59", "events");
        std::env::set_var("MINIO_NOTIFY_POSTGRES_HOST_59", "127.0.0.1");
        std::env::set_var("MINIO_NOTIFY_POSTGRES_PORT_59", "5432");
        std::env::set_var("MINIO_NOTIFY_POSTGRES_DATABASE_59", "minio");
        std::env::set_var("MINIO_NOTIFY_POSTGRES_USERNAME_59", "minio");
    }

    let targets = load_postgresql_targets_from_env().expect("load postgresql targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "59:postgresql")
        .expect("configured target");
    assert_eq!(target.args.table, "events");
    assert_eq!(target.args.host, "127.0.0.1");
    assert_eq!(target.args.port, "5432");
    assert_eq!(target.args.database, "minio");
    assert_eq!(target.args.username, "minio");

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_POSTGRES_ENABLE_59");
        std::env::remove_var("MINIO_NOTIFY_POSTGRES_TABLE_59");
        std::env::remove_var("MINIO_NOTIFY_POSTGRES_HOST_59");
        std::env::remove_var("MINIO_NOTIFY_POSTGRES_PORT_59");
        std::env::remove_var("MINIO_NOTIFY_POSTGRES_DATABASE_59");
        std::env::remove_var("MINIO_NOTIFY_POSTGRES_USERNAME_59");
    }
}

#[test]
fn mysql_sql_builders_match_reference_shape() {
    assert_eq!(
        mysql_update_row("events"),
        "INSERT INTO events (key_name, value) VALUES (?, CAST(? AS JSON)) ON DUPLICATE KEY UPDATE value=VALUES(value);"
    );
    assert_eq!(
        mysql_insert_row("events"),
        "INSERT INTO events (event_time, event_data) VALUES (?, CAST(? AS JSON));"
    );
    assert_eq!(
        mysql_delete_row("events"),
        "DELETE FROM events WHERE key_hash = SHA2(?, 256);"
    );
}

#[test]
fn postgresql_sql_builders_match_reference_shape() {
    assert_eq!(
        postgresql_update_row("events"),
        "INSERT INTO events (key, value) VALUES ($1, CAST($2 AS JSONB)) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;"
    );
    assert_eq!(
        postgresql_insert_row("events"),
        "INSERT INTO events (event_time, event_data) VALUES ($1, CAST($2 AS JSONB));"
    );
    assert_eq!(
        postgresql_delete_row("events"),
        "DELETE FROM events WHERE key = $1;"
    );
}

#[test]
fn notification_loader_reads_amqp_target_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_AMQP_ENABLE_69", "on");
        std::env::set_var(
            "MINIO_NOTIFY_AMQP_URL_69",
            "amqp://guest:guest@127.0.0.1:5672",
        );
        std::env::set_var("MINIO_NOTIFY_AMQP_EXCHANGE_69", "minio.events");
        std::env::set_var("MINIO_NOTIFY_AMQP_ROUTING_KEY_69", "objects");
    }

    let targets = load_amqp_targets_from_env().expect("load amqp targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "69:amqp")
        .expect("configured target");
    assert_eq!(target.args.url, "amqp://guest:guest@127.0.0.1:5672");
    assert_eq!(target.args.exchange, "minio.events");
    assert_eq!(target.args.routing_key, "objects");

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_AMQP_ENABLE_69");
        std::env::remove_var("MINIO_NOTIFY_AMQP_URL_69");
        std::env::remove_var("MINIO_NOTIFY_AMQP_EXCHANGE_69");
        std::env::remove_var("MINIO_NOTIFY_AMQP_ROUTING_KEY_69");
    }
}

#[test]
fn notification_loader_reads_mqtt_target_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_MQTT_ENABLE_79", "on");
        std::env::set_var("MINIO_NOTIFY_MQTT_BROKER_79", "tcp://127.0.0.1:1883");
        std::env::set_var("MINIO_NOTIFY_MQTT_TOPIC_79", "minio/events");
        std::env::set_var("MINIO_NOTIFY_MQTT_QOS_79", "1");
        std::env::set_var("MINIO_NOTIFY_MQTT_KEEP_ALIVE_INTERVAL_79", "10s");
    }

    let targets = load_mqtt_targets_from_env().expect("load mqtt targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "79:mqtt")
        .expect("configured target");
    assert_eq!(target.args.broker, "tcp://127.0.0.1:1883");
    assert_eq!(target.args.topic, "minio/events");
    assert_eq!(target.args.qos, 1);
    assert_eq!(target.args.keep_alive_secs, 10);

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_MQTT_ENABLE_79");
        std::env::remove_var("MINIO_NOTIFY_MQTT_BROKER_79");
        std::env::remove_var("MINIO_NOTIFY_MQTT_TOPIC_79");
        std::env::remove_var("MINIO_NOTIFY_MQTT_QOS_79");
        std::env::remove_var("MINIO_NOTIFY_MQTT_KEEP_ALIVE_INTERVAL_79");
    }
}

#[test]
fn notification_loader_reads_kafka_target_from_env() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("MINIO_NOTIFY_KAFKA_ENABLE_89", "on");
        std::env::set_var(
            "MINIO_NOTIFY_KAFKA_BROKERS_89",
            "127.0.0.1:9092,127.0.0.1:9093",
        );
        std::env::set_var("MINIO_NOTIFY_KAFKA_TOPIC_89", "minio.events");
        std::env::set_var("MINIO_NOTIFY_KAFKA_TLS_89", "on");
        std::env::set_var("MINIO_NOTIFY_KAFKA_TLS_SKIP_VERIFY_89", "on");
        std::env::set_var("MINIO_NOTIFY_KAFKA_SASL_89", "on");
        std::env::set_var("MINIO_NOTIFY_KAFKA_SASL_USERNAME_89", "minio");
        std::env::set_var("MINIO_NOTIFY_KAFKA_SASL_PASSWORD_89", "minio-secret");
        std::env::set_var("MINIO_NOTIFY_KAFKA_SASL_MECHANISM_89", "SCRAM-SHA-256");
        std::env::set_var("MINIO_NOTIFY_KAFKA_BATCH_SIZE_89", "500");
        std::env::set_var("MINIO_NOTIFY_KAFKA_BATCH_COMMIT_TIMEOUT_89", "2s");
        std::env::set_var("MINIO_NOTIFY_KAFKA_PRODUCER_COMPRESSION_CODEC_89", "gzip");
    }

    let targets = load_kafka_targets_from_env().expect("load kafka targets");
    let target = targets
        .into_iter()
        .find(|target| target.id().to_string() == "89:kafka")
        .expect("configured target");
    assert_eq!(
        target.args.brokers,
        vec!["127.0.0.1:9092".to_string(), "127.0.0.1:9093".to_string()]
    );
    assert_eq!(target.args.topic, "minio.events");
    assert!(target.args.tls.enable);
    assert!(target.args.tls.skip_verify);
    assert!(target.args.sasl.enable);
    assert_eq!(target.args.sasl.username, "minio");
    assert_eq!(target.args.sasl.mechanism, "SCRAM-SHA-256");
    assert_eq!(target.args.batch_size, 500);
    assert_eq!(target.args.batch_commit_timeout_ms, 2_000);
    assert_eq!(target.args.producer.compression, "gzip");

    unsafe {
        std::env::remove_var("MINIO_NOTIFY_KAFKA_ENABLE_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_BROKERS_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_TOPIC_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_TLS_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_TLS_SKIP_VERIFY_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_SASL_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_SASL_USERNAME_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_SASL_PASSWORD_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_SASL_MECHANISM_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_BATCH_SIZE_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_BATCH_COMMIT_TIMEOUT_89");
        std::env::remove_var("MINIO_NOTIFY_KAFKA_PRODUCER_COMPRESSION_CODEC_89");
    }
}
