pub fn extract_prefix_and_suffix(path: &str, users_path: &str) -> (String, String) {
    let normalized_users = users_path.trim_matches('/');
    let normalized_path = path.trim_matches('/');

    if normalized_users.is_empty() {
        return (String::new(), normalized_path.to_string());
    }

    if normalized_path == normalized_users {
        return (format!("{normalized_users}/"), String::new());
    }

    if let Some(suffix) = normalized_path.strip_prefix(&format!("{normalized_users}/")) {
        return (format!("{normalized_users}/"), suffix.to_string());
    }

    (String::new(), normalized_path.to_string())
}
