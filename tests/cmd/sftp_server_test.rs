// Rust test snapshot derived from cmd/sftp-server_test.go.

use minio_rust::cmd::{SftpAuthError, SftpAuthService};

pub const SOURCE_FILE: &str = "cmd/sftp-server_test.go";

#[test]
fn test_sftpauthentication_line_64() {
    let valid_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDVGk/SRz4fwTPK0+Ra7WYUGf3o08YkpI0yTMPpHwYoq dillon@example.io";
    let invalid_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDES4saDDRpoHDVmiYESEQrCYhw8EK7Utj/A/lqxiqZlP6Il3aN2fWu6uJQdWAovZxNeXUf8LIujisW1mJWGZPql0SLKVq6IZ707OAGmKA59IXfF5onRoU9+K4UDL7BJFfix6/3F5OV2WB3ChFrOrXhJ0CZ0sVAfGcV4q72kS19YjZNX3fqCc2HF8UQEaZGKIkw5MtdZI9a1P2bqnPuPGJybRFUzyoQXPge45QT5jnpcsAXOuXcGxbjuqaaHXFNTSKAkCU93TcjAbqUMkTz2mnFz/MnrKJTECN3Fy0GPCCQ5dxmG8p8DyMiNl7JYkX2r3XYgxmioCzkcg8fDs5p0CaQcipu+MA7iK7APKq7v4Zr/wNltXHI3DE9S8J88Hxb2FZAyEhCRfcgGmCVfoZxVNCRHNkGYzfe63BkxtnseUCzpYEhKv02H5u9rjFpdMY37kDfHDVqBbgutdMij+tQAEp1kyqi6TQL+4XHjPHkLaeekW07yB+VI90dK1A9dzTpOvE= liza@example.io";

    let mut auth = SftpAuthService::new();
    auth.add_internal_user("svc-user", "svc-secret");

    let perms = auth
        .ssh_password_auth("svc-user=svc", "svc-secret")
        .expect("internal svc login");
    assert_eq!(perms.access_key, "svc-user");

    let perms = auth
        .ssh_password_auth("svc-user", "svc-secret")
        .expect("internal direct login");
    assert_eq!(perms.secret_key, "svc-secret");

    assert_eq!(
        auth.ssh_password_auth("svc-user=svc", "invalid"),
        Err(SftpAuthError::Authentication)
    );
    assert_eq!(
        auth.ssh_password_auth("svc-user", "invalid"),
        Err(SftpAuthError::Authentication)
    );

    auth.set_ldap_enabled(true);
    auth.add_ldap_user("dillon", "dillon", Some(valid_key), false)
        .expect("ldap user without policies");
    auth.add_ldap_user("fahim", "fahim", None, true)
        .expect("ldap user without pubkey");

    assert_eq!(
        auth.ssh_password_auth("dillon=ldap", "dillon"),
        Err(SftpAuthError::UserHasNoPolicies)
    );
    assert_eq!(
        auth.ssh_password_auth("dillon", "dillon"),
        Err(SftpAuthError::NoSuchUser)
    );
    assert_eq!(
        auth.ssh_password_auth("dillon_error", "dillon_error"),
        Err(SftpAuthError::NoSuchUser)
    );
    assert_eq!(
        auth.ssh_password_auth("dillon=svc", "dillon"),
        Err(SftpAuthError::NoSuchUser)
    );
    assert_eq!(
        auth.ssh_password_auth("dillon", "dillon_error"),
        Err(SftpAuthError::NoSuchUser)
    );

    auth.add_ldap_user("dillon", "dillon", Some(valid_key), true)
        .expect("ldap user with policies");

    let perms = auth
        .ssh_password_auth("dillon=ldap", "dillon")
        .expect("ldap suffixed login");
    assert_eq!(perms.access_key, "dillon");

    let perms = auth
        .ssh_password_auth("dillon", "dillon")
        .expect("ldap fallback login");
    assert_eq!(perms.access_key, "dillon");

    assert!(auth.ssh_pubkey_auth("dillon=ldap", valid_key).is_ok());
    assert!(auth.ssh_pubkey_auth("dillon", valid_key).is_ok());

    assert_eq!(
        auth.ssh_pubkey_auth("dillon=ldap", invalid_key),
        Err(SftpAuthError::Authentication)
    );
    assert_eq!(
        auth.ssh_pubkey_auth("dillon", invalid_key),
        Err(SftpAuthError::NoSuchUser)
    );

    assert_eq!(
        auth.ssh_pubkey_auth("fahim=ldap", valid_key),
        Err(SftpAuthError::Authentication)
    );
    assert_eq!(
        auth.ssh_pubkey_auth("fahim", valid_key),
        Err(SftpAuthError::NoSuchUser)
    );
}

#[test]
fn subtest_test_sftpauthentication_line_66() {
    let valid_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDVGk/SRz4fwTPK0+Ra7WYUGf3o08YkpI0yTMPpHwYoq dillon@example.io";

    for (name, configure_ldap, username, secret, expected) in [
        (
            "internal service account",
            false,
            "svc-user=svc",
            "svc-secret",
            Ok("svc-user"),
        ),
        ("ldap explicit", true, "dillon=ldap", "dillon", Ok("dillon")),
        (
            "ldap forced internal failure",
            true,
            "dillon=svc",
            "dillon",
            Err(SftpAuthError::NoSuchUser),
        ),
    ] {
        let mut auth = SftpAuthService::new();
        auth.add_internal_user("svc-user", "svc-secret");
        if configure_ldap {
            auth.set_ldap_enabled(true);
            auth.add_ldap_user("dillon", "dillon", Some(valid_key), true)
                .expect(name);
        }
        let result = auth
            .ssh_password_auth(username, secret)
            .map(|perms| perms.access_key);
        assert_eq!(result, expected.map(str::to_string), "{name}");
    }
}
