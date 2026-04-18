use super::config::{
    console_address_from_args_or_env, console_address_from_env, normalize_bind_address,
    reachable_address, validate_console_address, MinioServerConfig,
};
use super::handlers::{rehydrate_replication_resync_queue, route_request};
use super::state::{
    load_notification_history_state, now_ms, replication_queue_path, startup_message,
    ServerAdminState, ServerHandle,
};
use super::*;
pub fn run_cli(args: Vec<String>) -> Result<(), String> {
    let config = MinioServerConfig::from_cli_args(&args)?;
    run_server_with_console(config, console_address_from_args_or_env(&args)?)
}

pub fn run_server(config: MinioServerConfig) -> Result<(), String> {
    run_server_with_console(config, console_address_from_env())
}

fn run_server_with_console(
    config: MinioServerConfig,
    console_address: Option<String>,
) -> Result<(), String> {
    let handle = spawn_server_with_console(config, console_address)?;
    println!("{}", startup_message(&handle));

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

pub fn spawn_server(config: MinioServerConfig) -> Result<ServerHandle, String> {
    spawn_server_with_console(config, console_address_from_env())
}

fn spawn_server_with_console(
    config: MinioServerConfig,
    console_address: Option<String>,
) -> Result<ServerHandle, String> {
    let mut notification_targets = NotificationTargetRegistry::new("us-east-1");
    notification_targets.register_webhooks_from_env()?;
    let _ = notification_targets.register_queues_from_env()?;
    let _ = notification_targets.register_elasticsearch_from_env()?;
    let _ = notification_targets.register_redis_from_env()?;
    let _ = notification_targets.register_mysql_from_env()?;
    let _ = notification_targets.register_postgresql_from_env()?;
    let _ = notification_targets.register_amqp_from_env()?;
    let _ = notification_targets.register_mqtt_from_env()?;
    let _ = notification_targets.register_kafka_from_env()?;
    let _ = notification_targets.register_nats_from_env()?;
    let _ = notification_targets.register_nsq_from_env()?;
    spawn_server_impl(
        config,
        notification_targets,
        load_replication_remote_targets_from_env(),
        console_address,
    )
}

fn spawn_server_impl(
    config: MinioServerConfig,
    notification_targets: NotificationTargetRegistry,
    replication_targets: BTreeMap<String, ReplicationRemoteTarget>,
    console_address: Option<String>,
) -> Result<ServerHandle, String> {
    for disk in &config.disks {
        fs::create_dir_all(disk).map_err(|err| format!("create disk {}: {err}", disk.display()))?;
    }
    let layer = Arc::new(new_object_layer(config.disks.clone())?);
    let credentials = HandlerCredentials::new(&config.root_user, &config.root_password);
    let kms = KmsServiceFacade::from_current_env()?;
    let replication_service = ReplicationService::new_persistent(
        ReplicationBackoffConfig {
            initial_backoff_ms: 1_000,
            max_backoff_ms: 30_000,
            default_max_attempts: 5,
        },
        replication_queue_path(&layer),
        now_ms(),
    )?;
    let persisted_resync_targets = load_all_bucket_replication_resync_records(&layer)?;
    let handlers = ObjectApiHandlers::from_shared_layer(layer.clone(), credentials.clone())
        .with_replication_targets(replication_targets.clone())
        .with_replication_service(replication_service.clone());
    let worker_layer = layer.clone();
    let worker_targets = replication_targets.clone();
    let admin_state = ServerAdminState::new(
        Credentials::new(&config.root_user, &config.root_password),
        layer.clone(),
        replication_service.clone(),
        kms,
        notification_targets,
        replication_targets,
    );
    let persisted_notification_history = load_notification_history_state(&layer)?;
    {
        let mut notifications = admin_state
            .notifications
            .lock()
            .expect("notifications lock");
        *notifications = persisted_notification_history.notifications;
    }
    {
        let mut deliveries = admin_state
            .queue_deliveries
            .lock()
            .expect("queue deliveries lock");
        *deliveries = persisted_notification_history.queue_deliveries;
    }
    {
        let mut deliveries = admin_state
            .webhook_deliveries
            .lock()
            .expect("webhook deliveries lock");
        *deliveries = persisted_notification_history.webhook_deliveries;
    }
    {
        let mut deliveries = admin_state
            .elasticsearch_deliveries
            .lock()
            .expect("elasticsearch deliveries lock");
        *deliveries = persisted_notification_history.elasticsearch_deliveries;
    }
    {
        let mut deliveries = admin_state
            .redis_deliveries
            .lock()
            .expect("redis deliveries lock");
        *deliveries = persisted_notification_history.redis_deliveries;
    }
    {
        let mut deliveries = admin_state
            .mysql_deliveries
            .lock()
            .expect("mysql deliveries lock");
        *deliveries = persisted_notification_history.mysql_deliveries;
    }
    {
        let mut deliveries = admin_state
            .postgresql_deliveries
            .lock()
            .expect("postgresql deliveries lock");
        *deliveries = persisted_notification_history.postgresql_deliveries;
    }
    {
        let mut deliveries = admin_state
            .amqp_deliveries
            .lock()
            .expect("amqp deliveries lock");
        *deliveries = persisted_notification_history.amqp_deliveries;
    }
    {
        let mut deliveries = admin_state
            .mqtt_deliveries
            .lock()
            .expect("mqtt deliveries lock");
        *deliveries = persisted_notification_history.mqtt_deliveries;
    }
    {
        let mut deliveries = admin_state
            .kafka_deliveries
            .lock()
            .expect("kafka deliveries lock");
        *deliveries = persisted_notification_history.kafka_deliveries;
    }
    {
        let mut deliveries = admin_state
            .nats_deliveries
            .lock()
            .expect("nats deliveries lock");
        *deliveries = persisted_notification_history.nats_deliveries;
    }
    {
        let mut deliveries = admin_state
            .nsq_deliveries
            .lock()
            .expect("nsq deliveries lock");
        *deliveries = persisted_notification_history.nsq_deliveries;
    }
    {
        let mut tracked = admin_state
            .resync_targets
            .lock()
            .expect("resync targets lock");
        *tracked = persisted_resync_targets;
    }
    rehydrate_replication_resync_queue(&admin_state)?;
    let replication_worker = replication_service.spawn_worker(
        ReplicationWorkerConfig::default(),
        now_ms,
        move |entry| retry_replication_entry_for_layer(&worker_layer, &worker_targets, entry),
    );

    if let Some(console_address) = console_address.as_deref() {
        validate_console_address(&config.address, console_address)?;
    }

    let bind_address = normalize_bind_address(&config.address);
    let listener = TcpListener::bind(&bind_address)
        .map_err(|err| format!("bind {}: {err}", config.address))?;
    let bound_address = listener
        .local_addr()
        .map_err(|err| format!("resolve bound address: {err}"))?
        .to_string();
    let address = reachable_address(&config.address, &bound_address);
    let server = Server::from_listener(listener, None).map_err(|err| err.to_string())?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let stop = Arc::clone(&shutdown);
    let mut joins = Vec::new();
    joins.push(thread::spawn(move || {
        server_loop(server, handlers, credentials, admin_state, stop)
    }));

    let mut bound_console_address = None;
    if let Some(console_address) = console_address {
        let console_bind_address = normalize_bind_address(&console_address);
        let console_listener = TcpListener::bind(&console_bind_address)
            .map_err(|err| format!("bind {}: {err}", console_address))?;
        let console_bound_address = console_listener
            .local_addr()
            .map_err(|err| format!("resolve bound console address: {err}"))?
            .to_string();
        let console_server =
            Server::from_listener(console_listener, None).map_err(|err| err.to_string())?;
        let console_stop = Arc::clone(&shutdown);
        let api_address = address.clone();
        let console_address_clone = reachable_address(&console_address, &console_bound_address);
        let console_address_for_thread = console_address_clone.clone();
        joins.push(thread::spawn(move || {
            console_loop(
                console_server,
                console_stop,
                &api_address,
                &console_address_for_thread,
            )
        }));
        bound_console_address = Some(console_address_clone);
    }

    Ok(ServerHandle {
        address,
        console_address: bound_console_address,
        root_user: config.root_user.clone(),
        root_password: config.root_password.clone(),
        shutdown,
        replication_worker: Some(replication_worker),
        joins,
    })
}

fn console_loop(
    server: Server,
    stop: Arc<AtomicBool>,
    api_address: &str,
    console_address: &str,
) -> Result<(), String> {
    while !stop.load(Ordering::SeqCst) {
        let request = match server.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(request)) => request,
            Ok(None) => continue,
            Err(err) => return Err(err.to_string()),
        };

        let response = route_console_request(
            &request.method().clone(),
            request.url(),
            api_address,
            console_address,
        );
        let mut tiny_response =
            Response::from_data(response.body).with_status_code(StatusCode(response.status));
        for (name, value) in response.headers {
            if let Ok(header) = Header::from_bytes(name.as_bytes(), value.as_bytes()) {
                tiny_response = tiny_response.with_header(header);
            }
        }
        request
            .respond(tiny_response)
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

fn route_console_request(
    method: &Method,
    raw_url: &str,
    api_address: &str,
    console_address: &str,
) -> HandlerResponse {
    if *method != Method::Get {
        return HandlerResponse {
            status: 405,
            headers: BTreeMap::new(),
            body: Vec::new(),
        };
    }

    match raw_url {
        "/" | "/index.html" => {
            let mut headers = BTreeMap::new();
            headers.insert(
                "content-type".to_string(),
                "text/html; charset=utf-8".to_string(),
            );
            HandlerResponse {
                status: 200,
                headers,
                body: format!(
                    "<!doctype html><html><head><title>MinIO Console</title></head><body><h1>MinIO Rust Console</h1><p>Console address: {console_address}</p><p>API address: {api_address}</p></body></html>"
                )
                .into_bytes(),
            }
        }
        "/api/v1/login" => {
            let mut headers = BTreeMap::new();
            headers.insert("content-type".to_string(), "application/json".to_string());
            HandlerResponse {
                status: 200,
                headers,
                body: serde_json::to_vec(&serde_json::json!({
                    "status": "unsupported",
                    "message": "MinIO Rust does not yet implement the full browser console API",
                }))
                .unwrap_or_default(),
            }
        }
        "/minio/health/live" | "/minio/health/ready" => HandlerResponse {
            status: 200,
            headers: BTreeMap::new(),
            body: Vec::new(),
        },
        _ => HandlerResponse {
            status: 404,
            headers: BTreeMap::new(),
            body: b"not found".to_vec(),
        },
    }
}

fn server_loop(
    server: Server,
    handlers: ObjectApiHandlers,
    credentials: HandlerCredentials,
    admin_state: ServerAdminState,
    shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
    while !shutdown.load(Ordering::SeqCst) {
        let mut request = match server.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(request)) => request,
            Ok(None) => continue,
            Err(err) => return Err(err.to_string()),
        };

        let method = request.method().clone();
        let raw_url = request.url().to_string();
        let headers = request.headers().to_vec();
        let mut body = Vec::new();
        request
            .as_reader()
            .read_to_end(&mut body)
            .map_err(|err| err.to_string())?;

        let response = route_request(
            &method,
            &raw_url,
            &headers,
            &body,
            &handlers,
            &credentials,
            &admin_state,
        )?;
        let mut tiny_response =
            Response::from_data(response.body).with_status_code(StatusCode(response.status));
        for (name, value) in response.headers {
            if let Ok(header) = Header::from_bytes(name.as_bytes(), value.as_bytes()) {
                tiny_response = tiny_response.with_header(header);
            }
        }
        request
            .respond(tiny_response)
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn spawn_server_with_webhook_targets(
    config: MinioServerConfig,
    webhook_targets: BTreeMap<String, WebhookNotificationTarget>,
) -> Result<ServerHandle, String> {
    let mut notification_targets = NotificationTargetRegistry::new("us-east-1");
    for target in webhook_targets.into_values() {
        notification_targets.register(target)?;
    }
    spawn_server_impl(
        config,
        notification_targets,
        BTreeMap::new(),
        console_address_from_env(),
    )
}

#[cfg(test)]
pub(super) fn spawn_server_with_replication_targets(
    config: MinioServerConfig,
    replication_targets: BTreeMap<String, ReplicationRemoteTarget>,
) -> Result<ServerHandle, String> {
    spawn_server_impl(
        config,
        NotificationTargetRegistry::new("us-east-1"),
        replication_targets,
        console_address_from_env(),
    )
}
