use minio_rust::cmd::cross_domain_xml;

pub const SOURCE_FILE: &str = "cmd/crossdomain-xml-handler_test.go";

#[test]
fn test_cross_xmlhandler_line_29() {
    let xml = cross_domain_xml();
    assert!(xml.starts_with("<?xml version=\"1.0\"?>"));
    assert!(xml.contains("<cross-domain-policy>"));
    assert!(xml.contains(r#"<allow-access-from domain="*" secure="false" />"#));
    assert!(xml.ends_with("</cross-domain-policy>"));
}
