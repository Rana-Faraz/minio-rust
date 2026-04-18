use std::fs;

use minio_rust::internal::config::certs::{
    load_x509_key_pair, parse_public_cert_file, ENV_CERT_PASSWORD,
};
use tempfile::tempdir;

const VALID_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDiTCCAnGgAwIBAgIJAIb84Z5Mh31iMA0GCSqGSIb3DQEBCwUAMFsxCzAJBgNV
BAYTAlVTMQ4wDAYDVQQIDAVzdGF0ZTERMA8GA1UEBwwIbG9jYXRpb24xFTATBgNV
BAoMDG9yZ2FuaXphdGlvbjESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTE3MTIxODE4
NTcyM1oXDTI3MTIxNjE4NTcyM1owWzELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBXN0
YXRlMREwDwYDVQQHDAhsb2NhdGlvbjEVMBMGA1UECgwMb3JnYW5pemF0aW9uMRIw
EAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDgr1Cru8xjZsaSt0UBikFg0uUKCecVT7SmL437duO5aZ9f+TBZcFrdIOooPTw9
ZbPC8FIs8IJRXYi5R3u0FrWknbBeR2gN4jkidgckqtdtJmieeLoF7HuyCepX/2jz
EWRI6V6hBZmDJ4obG23Qn9n7OGkf7NdqHAzjA4vfLMtJC340Iozo33rNdiLXoMRr
z19jBHE/ATVdNFzsN2CCrH7iJEe9gbR/JM1w8H/5SIXDn0QTVKBAjJNWe1Wqmxvq
zxd2QXFRGgvF5+FwxV/aRTRPTg5YjeCZ5q8p9NyPxnb9uM9Kyo+aCQAO/1G7pWdI
r/3pNb83DFYnfY8HPpJuX0t3AgMBAAGjUDBOMB0GA1UdDgQWBBQ2/bSCHscnoV+0
d+YJxLu4XLSNIDAfBgNVHSMEGDAWgBQ2/bSCHscnoV+0d+YJxLu4XLSNIDAMBgNV
HRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQC6p4gPwmkoDtRsP1c8IWgXFka+
Q59oe79ZK1RqDE6ZZu0rgw07rPzKr4ofW4hTxnx7PUgKOhWLq9VvwEC/9tDbD0Gw
SKknRZZOiEE3qUZbwNtHMd4UBzpzChTRC6RcwC5zT1/WICMUHxa4b8E2umJuf3Qd
5Y23sXEESx5evr49z6DLcVe2i70o2wJeWs2kaXqhCJt0X7z0rnYqjfFdvxd8dyzt
1DXmE45cLadpWHDg26DMsdchamgnqEo79YUxkH6G/Cb8ZX4igQ/CsxCDOKvccjHO
OncDtuIpK8O7OyfHP3+MBpUFG4P6Ctn7RVcZe9fQweTpfAy18G+loVzuUeOD
-----END CERTIFICATE-----"#;

const ENCRYPTED_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDiTCCAnGgAwIBAgIJAK5m5S7EE46kMA0GCSqGSIb3DQEBCwUAMFsxCzAJBgNV
BAYTAlVTMQ4wDAYDVQQIDAVzdGF0ZTERMA8GA1UEBwwIbG9jYXRpb24xFTATBgNV
BAoMDG9yZ2FuaXphdGlvbjESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTE3MTIxODE4
MDUyOFoXDTI3MTIxNjE4MDUyOFowWzELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBXN0
YXRlMREwDwYDVQQHDAhsb2NhdGlvbjEVMBMGA1UECgwMb3JnYW5pemF0aW9uMRIw
EAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQDPJfYY5Dhsntrqwyu7ZgKM/zrlKEjCwGHhWJBdZdeZCHQlY8ISrtDxxp2XMmI6
HsszalEhNF9fk3vSXWclTuomG03fgGzP4R6QpcwGUCxhRF1J+0b64Yi8pw2uEGsR
GuMwLhGorcWalNoihgHc0BQ4vO8aaTNTX7iD06olesP6vGNu/S8h0VomE+0v9qYc
VF66Zaiv/6OmxAtDpElJjVd0mY7G85BlDlFrVwzd7zhRiuJZ4iDg749Xt9GuuKla
Dvr14glHhP4dQgUbhluJmIHMdx2ZPjk+5FxaDK6I9IUpxczFDe4agDE6lKzU1eLd
cCXRWFOf6q9lTB1hUZfmWfTxAgMBAAGjUDBOMB0GA1UdDgQWBBTQh7lDTq+8salD
0HBNILochiiNaDAfBgNVHSMEGDAWgBTQh7lDTq+8salD0HBNILochiiNaDAMBgNV
HRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAqi9LycxcXKNSDXaPkCKvw7RQy
iMBDGm1kIY++p3tzbUGuaeu85TsswKnqd50AullEU+aQxRRJGfR8eSKzQJMBXLMQ
b4ptYCc5OrZtRHT8NaZ/df2tc6I88kN8dBu6ybcNGsevXA/iNX3kKLW7naxdr5jj
KUudWSuqDCjCmQa5bYb9H6DreLH2lUItSWBa/YmeZ3VSezDCd+XYO53QKwZVj8Jb
bulZmoo7e7HO1qecEzWKL10UYyEbG3UDPtw+NZc142ZYeEhXQ0dsstGAO5hf3hEl
kQyKGUTpDbKLuyYMFsoH73YLjBqNe+UEhPwE+FWpcky1Sp9RTx/oMLpiZaPR
-----END CERTIFICATE-----"#;

const VALID_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA4K9Qq7vMY2bGkrdFAYpBYNLlCgnnFU+0pi+N+3bjuWmfX/kw
WXBa3SDqKD08PWWzwvBSLPCCUV2IuUd7tBa1pJ2wXkdoDeI5InYHJKrXbSZonni6
Bex7sgnqV/9o8xFkSOleoQWZgyeKGxtt0J/Z+zhpH+zXahwM4wOL3yzLSQt+NCKM
6N96zXYi16DEa89fYwRxPwE1XTRc7Ddggqx+4iRHvYG0fyTNcPB/+UiFw59EE1Sg
QIyTVntVqpsb6s8XdkFxURoLxefhcMVf2kU0T04OWI3gmeavKfTcj8Z2/bjPSsqP
mgkADv9Ru6VnSK/96TW/NwxWJ32PBz6Sbl9LdwIDAQABAoIBABVh+d5uH/RxyoIZ
+PI9kx1A1NVQvfI0RK/wJKYC2YdCuw0qLOTGIY+b20z7DumU7TenIVrvhKdzrFhd
qjMoWh8RdsByMT/pAKD79JATxi64EgrK2IFJ0TfPY8L+JqHDTPT3aK8QVly5/ZW4
1YmePOOAqdiE9Lc/diaApuYVYD9SL/X7fYs1ezOB4oGXoz0rthX77zHMxcEurpK3
VgSnaq7FYTVY7GrFB+ASiAlDIyLwztz08Ijn8aG0QAZ8GFuPGSmPMXWjLwFhRZsa
Gfy5BYiA0bVSnQSPHzAnHu9HyGlsdouVPPvJB3SrvMl+BFhZiUuR8OGSob7z7hfI
hMyHbNECgYEA/gyG7sHAb5mPkhq9JkTv+LrMY5NDZKYcSlbvBlM3kd6Ib3Hxl+6T
FMq2TWIrh2+mT1C14htziHd05dF6St995Tby6CJxTj6a/2Odnfm+JcOou/ula4Sz
92nIGlGPTJXstDbHGnRCpk6AomXK02stydTyrCisOw1H+LyTG6aT0q8CgYEA4mkO
hfLJkgmJzWIhxHR901uWHz/LId0gC6FQCeaqWmRup6Bl97f0U6xokw4tw8DJOncF
yZpYRXUXhdv/FXCjtXvAhKIX5+e+3dlzPHIdekSfcY00ip/ifAS1OyVviJia+cna
eJgq8WLHxJZim9Ah93NlPyiqGPwtasub90qjZbkCgYEA35WK02o1wII3dvCNc7bM
M+3CoAglEdmXoF1uM/TdPUXKcbqoU3ymeXAGjYhOov3CMp/n0z0xqvLnMLPxmx+i
ny6DDYXyjlhO9WFogHYhwP636+mHJl8+PAsfDvqk0VRJZDmpdUDIv7DrSQGpRfRX
8f+2K4oIOlhv9RuRpI4wHwUCgYB8OjaMyn1NEsy4k2qBt4U+jhcdyEv1pbWqi/U1
qYm5FTgd44VvWVDHBGdQoMv9h28iFCJpzrU2Txv8B4y7v9Ujg+ZLIAFL7j0szt5K
wTZpWvO9Q0Qb98Q2VgL2lADRiyIlglrMJnoRfiisNfOfGKE6e+eGsxI5qUxmN5e5
JQvoiQKBgQCqgyuUBIu/Qsb3qUED/o0S5wCel43Yh/Rl+mxDinOUvJfKJSW2SyEk
+jDo0xw3Opg6ZC5Lj2V809LA/XteaIuyhRuqOopjhHIvIvrYGe+2O8q9/Mv40BYW
0BhJ/Gdseps0C6Z5mTT5Fee4YVlGZuyuNKmKTd4JmqInfBV3ncMWQg==
-----END RSA PRIVATE KEY-----"#;

const ENCRYPTED_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: AES-128-CBC,CC483BF11678C35F9F02A1AD85DAE285

nMDFd+Qxk1f+S7LwMitmMofNXYNbCY4L1QEqPOOx5wnjNF1wSxmEkL7+h8W4Y/vb
AQt/7TCcUSuSqEMl45nUIcCbhBos5wz+ShvFiez3qKwmR5HSURvqyN6PIJeAbU+h
uw/cvAQsCH1Cq+gYkDJqjrizPhGqg7mSkqyeST3PbOl+ZXc0wynIjA34JSwO3c5j
cF7XKHETtNGj1+AiLruX4wYZAJwQnK375fCoNVMO992zC6K83d8kvGMUgmJjkiIj
q3s4ymFGfoo0S/XNDQXgE5A5QjAKRKUyW2i7pHIIhTyOpeJQeFHDi2/zaZRxoCog
lD2/HKLi5xJtRelZaaGyEJ20c05VzaSZ+EtRIN33foNdyQQL6iAUU3hJ6JlcmRIB
bRfX4XPH1w9UfFU5ZKwUciCoDcL65bsyv/y56ItljBp7Ok+UUKl0H4myFNOSfsuU
IIj4neslnAvwQ8SN4XUpug+7pGF+2m/5UDwRzSUN1H2RfgWN95kqR+tYqCq/E+KO
i0svzFrljSHswsFoPBqKngI7hHwc9QTt5q4frXwj9I4F6HHrTKZnC5M4ef26sbJ1
r7JRmkt0h/GfcS355b0uoBTtF1R8tSJo85Zh47wE+ucdjEvy9/pjnzKqIoJo9bNZ
ri+ue7GhH5EUca1Kd10bH8FqTF+8AHh4yW6xMxSkSgFGp7KtraAVpdp+6kosymqh
dz9VMjA8i28btfkS2isRaCpyumaFYJ3DJMFYhmeyt6gqYovmRLX0qrBf8nrkFTAA
ZmykWsc8ErsCudxlDmKVemuyFL7jtm9IRPq+Jh+IrmixLJFx8PKkNAM6g+A8irx8
piw+yhRsVy5Jk2QeIqvbpxN6BfCNcix4sWkusiCJrAqQFuSm26Mhh53Ig1DXG4d3
6QY1T8tW80Q6JHUtDR+iOPqW6EmrNiEopzirvhGv9FicXZ0Lo2yKJueeeihWhFLL
GmlnCjWVMO4hoo8lWCHv95JkPxGMcecCacKKUbHlXzCGyw3+eeTEHMWMEhziLeBy
HZJ1/GReI3Sx7XlUCkG4468Yz3PpmbNIk/U5XKE7TGuxKmfcWQpu022iF/9DrKTz
KVhKimCBXJX345bCFe1rN2z5CV6sv87FkMs5Y+OjPw6qYFZPVKO2TdUUBcpXbQMg
UW+Kuaax9W7214Stlil727MjRCiH1+0yODg4nWj4pTSocA5R3pn5cwqrjMu97OmL
ESx4DHmy4keeSy3+AIAehCZlwgeLb70/xCSRhJMIMS9Q6bz8CPkEWN8bBZt95oeo
37LqZ7lNmq61fs1x1tq0VUnI9HwLFEnsiubp6RG0Yu8l/uImjjjXa/ytW2GXrfUi
zM22dOntu6u23iBxRBJRWdFTVUz7qrdu+PHavr+Y7TbCeiBwiypmz5llf823UIVx
btamI6ziAq2gKZhObIhut7sjaLkAyTLlNVkNN1WNaplAXpW25UFVk93MHbvZ27bx
9iLGs/qB2kDTUjffSQoHTLY1GoLxv83RgVspUGQjslztEEpWfYvGfVLcgYLv933B
aRW9BRoNZ0czKx7Lhuwjreyb5IcWDarhC8q29ZkkWsQQonaPb0kTEFJul80Yqk0k
-----END RSA PRIVATE KEY-----"#;

#[test]
fn parse_public_cert_file_matches_reference_cases() {
    let tempdir = tempdir().expect("tempdir should be created");
    let missing = tempdir.path().join("missing.crt");
    let empty = tempdir.path().join("empty.crt");
    let invalid_pem = tempdir.path().join("invalid-pem.crt");
    let invalid_cert = tempdir.path().join("invalid-cert.crt");
    let single = tempdir.path().join("single.crt");
    let chain = tempdir.path().join("chain.crt");

    fs::write(&empty, "").expect("empty cert file should be written");
    fs::write(
        &invalid_pem,
        "-----BEGIN GARBAGE-----\nhello\n-----END GARBAGE-----\n",
    )
    .expect("invalid pem should be written");
    fs::write(
        &invalid_cert,
        VALID_CERT.replace("DQEBCwUAA4IBAQC6", "DQEBCwUAA4IBAQ!!"),
    )
    .expect("invalid cert should be written");
    fs::write(&single, VALID_CERT).expect("single cert should be written");
    fs::write(&chain, format!("{VALID_CERT}\n{VALID_CERT}\n")).expect("chain should be written");

    let cases = [
        (missing.as_path(), 0usize, true),
        (empty.as_path(), 0, true),
        (invalid_pem.as_path(), 0, true),
        (invalid_cert.as_path(), 0, true),
        (single.as_path(), 1, false),
        (chain.as_path(), 2, false),
    ];

    for (path, expected_len, should_err) in cases {
        let result = parse_public_cert_file(path);
        assert_eq!(result.is_err(), should_err, "path {}", path.display());
        if let Ok(certs) = result {
            assert_eq!(certs.len(), expected_len, "path {}", path.display());
        }
    }
}

#[test]
fn load_x509_key_pair_matches_reference_cases() {
    let tempdir = tempdir().expect("tempdir should be created");
    let cert_path = tempdir.path().join("public.crt");
    let key_path = tempdir.path().join("private.key");
    let encrypted_cert_path = tempdir.path().join("public-encrypted.crt");
    let encrypted_key_path = tempdir.path().join("private-encrypted.key");

    fs::write(&cert_path, VALID_CERT).expect("cert should be written");
    fs::write(&key_path, VALID_KEY).expect("key should be written");
    fs::write(&encrypted_cert_path, ENCRYPTED_CERT).expect("encrypted cert should be written");
    fs::write(&encrypted_key_path, ENCRYPTED_KEY).expect("encrypted key should be written");

    std::env::remove_var(ENV_CERT_PASSWORD);
    let clear = load_x509_key_pair(&cert_path, &key_path);
    assert!(clear.is_ok(), "unencrypted pair should load: {clear:?}");

    std::env::remove_var(ENV_CERT_PASSWORD);
    let missing_password = load_x509_key_pair(&encrypted_cert_path, &encrypted_key_path);
    assert!(
        missing_password.is_err(),
        "encrypted pair should fail without password"
    );

    std::env::set_var(ENV_CERT_PASSWORD, "password");
    let wrong_password = load_x509_key_pair(&encrypted_cert_path, &encrypted_key_path);
    assert!(
        wrong_password.is_err(),
        "encrypted pair should fail with wrong password"
    );

    std::env::set_var(ENV_CERT_PASSWORD, "foobar");
    let decrypted = load_x509_key_pair(&encrypted_cert_path, &encrypted_key_path);
    assert!(
        decrypted.is_ok(),
        "encrypted pair should load with correct password: {decrypted:?}"
    );

    std::env::remove_var(ENV_CERT_PASSWORD);
}
