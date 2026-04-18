pub const CROSSDOMAIN_XML_BODY: &str = r#"<?xml version="1.0"?><cross-domain-policy><allow-access-from domain="*" secure="false" /></cross-domain-policy>"#;

pub fn cross_domain_xml() -> &'static str {
    CROSSDOMAIN_XML_BODY
}
