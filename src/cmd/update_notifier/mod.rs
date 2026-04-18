use std::time::Duration;

fn humanize_older(older: Duration) -> String {
    let seconds = older.as_secs();
    if seconds == 0 {
        return "now".to_string();
    }
    if seconds < 60 {
        let unit = if seconds == 1 { "second" } else { "seconds" };
        return format!("{seconds} {unit} before the latest release");
    }

    let minutes = seconds / 60;
    if minutes < 60 {
        let unit = if minutes == 1 { "minute" } else { "minutes" };
        return format!("{minutes} {unit} before the latest release");
    }

    let hours = minutes / 60;
    if hours < 24 {
        let unit = if hours == 1 { "hour" } else { "hours" };
        return format!("{hours} {unit} before the latest release");
    }

    let days = hours / 24;
    if days < 7 {
        let unit = if days == 1 { "day" } else { "days" };
        return format!("{days} {unit} before the latest release");
    }
    if days < 30 {
        let weeks = days / 7;
        let unit = if weeks == 1 { "week" } else { "weeks" };
        return format!("{weeks} {unit} before the latest release");
    }
    if days < 360 {
        let months = days / 30;
        let unit = if months == 1 { "month" } else { "months" };
        return format!("{months} {unit} before the latest release");
    }

    let years = (days / 365).max(1);
    let unit = if years == 1 { "year" } else { "years" };
    format!("{years} {unit} before the latest release")
}

pub fn prepare_update_message(dl_url: &str, older: Duration) -> String {
    if dl_url.is_empty() {
        return String::new();
    }
    format!(
        "You are running an older version of MinIO released {}\nUpdate: {}",
        humanize_older(older),
        dl_url
    )
}

pub fn prepare_update_message_signed(dl_url: &str, older_secs: i64) -> String {
    if dl_url.is_empty() || older_secs < 0 {
        return String::new();
    }
    prepare_update_message(dl_url, Duration::from_secs(older_secs as u64))
}
