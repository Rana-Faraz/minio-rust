use minio_rust::cmd::prepare_update_message_signed;

pub const SOURCE_FILE: &str = "cmd/update-notifier_test.go";

#[test]
fn test_prepare_update_message_line_30() {
    let cases = [
        (
            72 * 60 * 60,
            "my_download_url",
            "3 days before the latest release",
        ),
        (
            3 * 60 * 60,
            "https://my_download_url_is_huge/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "3 hours before the latest release",
        ),
        (-72 * 60 * 60, "another_update_url", ""),
        (0, "another_update_url", "now"),
        (60 * 60, "", ""),
        (0, "my_download_url", "now"),
        (1, "my_download_url", "1 second before the latest release"),
        (
            37,
            "my_download_url",
            "37 seconds before the latest release",
        ),
        (60, "my_download_url", "1 minute before the latest release"),
        (61, "my_download_url", "1 minute before the latest release"),
        (
            37 * 60,
            "my_download_url",
            "37 minutes before the latest release",
        ),
        (
            60 * 60,
            "my_download_url",
            "1 hour before the latest release",
        ),
        (
            61 * 60,
            "my_download_url",
            "1 hour before the latest release",
        ),
        (
            122 * 60,
            "my_download_url",
            "2 hours before the latest release",
        ),
        (
            24 * 60 * 60,
            "my_download_url",
            "1 day before the latest release",
        ),
        (
            25 * 60 * 60,
            "my_download_url",
            "1 day before the latest release",
        ),
        (
            49 * 60 * 60,
            "my_download_url",
            "2 days before the latest release",
        ),
        (
            7 * 24 * 60 * 60,
            "my_download_url",
            "1 week before the latest release",
        ),
        (
            8 * 24 * 60 * 60,
            "my_download_url",
            "1 week before the latest release",
        ),
        (
            15 * 24 * 60 * 60,
            "my_download_url",
            "2 weeks before the latest release",
        ),
        (
            30 * 24 * 60 * 60,
            "my_download_url",
            "1 month before the latest release",
        ),
        (
            31 * 24 * 60 * 60,
            "my_download_url",
            "1 month before the latest release",
        ),
        (
            61 * 24 * 60 * 60,
            "my_download_url",
            "2 months before the latest release",
        ),
        (
            360 * 24 * 60 * 60,
            "my_download_url",
            "1 year before the latest release",
        ),
        (
            361 * 24 * 60 * 60,
            "my_download_url",
            "1 year before the latest release",
        ),
        (
            2 * 365 * 24 * 60 * 60,
            "my_download_url",
            "2 years before the latest release",
        ),
    ];

    let plain = "You are running an older version of MinIO released";
    for (older_secs, dl_url, expected_substr) in cases {
        let output = prepare_update_message_signed(dl_url, older_secs);
        match (dl_url.is_empty(), older_secs <= 0) {
            (true, _) => assert!(output.is_empty(), "{output}"),
            (false, true) if older_secs < 0 => assert!(output.is_empty(), "{output}"),
            _ => {
                assert!(
                    output.contains(&format!("{plain} {expected_substr}")),
                    "{output}"
                );
                assert!(output.contains(&format!("Update: {dl_url}")), "{output}");
            }
        }
    }
}
