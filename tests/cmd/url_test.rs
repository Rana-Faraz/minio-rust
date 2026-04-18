pub const SOURCE_FILE: &str = "cmd/url_test.go";

#[test]
fn benchmark_urlquery_form_line_25() {
    let req = url::Url::parse("http://localhost:9000/bucket/name?uploadId=upload&partNumber=1")
        .expect("url");
    let pairs = req.query_pairs().into_owned().collect::<Vec<_>>();
    for _ in 0..128 {
        let upload_id = pairs
            .iter()
            .find(|(k, _)| k == "uploadId")
            .map(|(_, v)| v.as_str());
        assert_eq!(upload_id, Some("upload"));
    }
}

#[test]
fn benchmark_urlquery_line_51() {
    let req = url::Url::parse("http://localhost:9000/bucket/name?uploadId=upload&partNumber=1")
        .expect("url");
    for _ in 0..128 {
        let upload_id = req
            .query_pairs()
            .find(|(k, _)| k == "uploadId")
            .map(|(_, v)| v.into_owned());
        assert_eq!(upload_id.as_deref(), Some("upload"));
    }
}
