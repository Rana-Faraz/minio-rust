pub fn strip_standard_ports(input: &str) -> String {
    input
        .replace(":80/", "/")
        .replace(":443/", "/")
        .replace(":80", "")
        .replace(":443", "")
}

pub fn print_server_common_message(
    api_endpoints: &[String],
    console_endpoints: &[String],
) -> String {
    let api = api_endpoints
        .iter()
        .map(|endpoint| strip_standard_ports(endpoint))
        .collect::<Vec<_>>()
        .join(", ");
    let console = console_endpoints
        .iter()
        .map(|endpoint| strip_standard_ports(endpoint))
        .collect::<Vec<_>>()
        .join(", ");

    format!("API: {api}\nConsole: {console}")
}

pub fn print_cli_access_msg(
    alias: &str,
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> String {
    format!(
        "mc alias set {alias} {} {access_key} {secret_key}",
        strip_standard_ports(endpoint)
    )
}

pub fn print_startup_message(
    api_endpoints: &[String],
    console_endpoints: &[String],
    alias: &str,
    access_key: &str,
    secret_key: &str,
) -> String {
    let endpoint = api_endpoints
        .first()
        .cloned()
        .unwrap_or_else(|| "http://127.0.0.1:9000".to_string());
    format!(
        "{}\n{}",
        print_server_common_message(api_endpoints, console_endpoints),
        print_cli_access_msg(alias, &endpoint, access_key, secret_key)
    )
}
