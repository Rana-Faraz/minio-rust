use minio_rust::internal::config::dns::{dns_join, msg_path, msg_unpath};

#[test]
fn dns_join_matches_reference_cases() {
    let cases = [
        (
            vec!["bla", "bliep", "example", "org"],
            "bla.bliep.example.org.",
        ),
        (vec!["example", "."], "example."),
        (vec!["example", "org."], "example.org.."),
        (vec!["."], "."),
    ];

    for (input, expected) in cases {
        assert_eq!(dns_join(&input), expected);
    }
}

#[test]
fn path_matches_reference_cases() {
    for prefix in ["mydns", "skydns"] {
        let result = msg_path("service.staging.skydns.local.", prefix);
        assert_eq!(result, format!("/{prefix}/local/skydns/staging/service"));
    }
}

#[test]
fn unpath_matches_reference_cases() {
    assert_eq!(
        msg_unpath("/skydns/local/cluster/staging/service/"),
        "service.staging.cluster.local.skydns"
    );
    assert_eq!(
        msg_unpath("/skydns/local/cluster/staging/service"),
        "service.staging.cluster.local.skydns"
    );
    assert_eq!(msg_unpath("/singleleveldomain/"), "singleleveldomain");
    assert_eq!(msg_unpath("/singleleveldomain"), "singleleveldomain");
    assert_eq!(msg_unpath("singleleveldomain"), "singleleveldomain");
}
