use std::io::Cursor;

use minio_rust::internal::s3select::select::{generate_sample_csv_data, S3Select};

pub const SOURCE_FILE: &str = "internal/s3select/select_benchmark_test.go";

fn request_xml(query: &str) -> String {
    format!(
        r#"
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>{query}</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
"#
    )
}

fn run_select(count: usize, query: &str) -> String {
    let xml = request_xml(query);
    let csv_data = generate_sample_csv_data(count);
    let mut select =
        S3Select::from_xml(Cursor::new(xml.as_bytes())).expect("xml parse should succeed");
    select
        .open(Cursor::new(csv_data))
        .expect("open should succeed");
    let mut output = Vec::new();
    select
        .evaluate(&mut output)
        .expect("evaluate should succeed");
    String::from_utf8(output).expect("output should be utf8")
}

fn assert_select_all(count: usize) {
    let output = run_select(count, "select * from S3Object");
    assert_eq!(output.lines().count(), count);
    assert!(output.starts_with("0,name0000,age00,city0000"));
}

fn assert_single_col(count: usize) {
    let output = run_select(count, "select id from S3Object");
    let lines: Vec<_> = output.lines().collect();
    assert_eq!(lines.len(), count);
    assert_eq!(lines.first().copied(), Some("0"));
    let expected_last = (count - 1).to_string();
    assert_eq!(lines.last().copied(), Some(expected_last.as_str()));
}

fn assert_count(count: usize) {
    let output = run_select(count, "select count(*) from S3Object");
    assert_eq!(output.trim(), count.to_string());
}

#[test]
fn benchmark_select_all_100_k_line_128() {
    assert_select_all(128);
}

#[test]
fn benchmark_select_all_1_m_line_133() {
    assert_select_all(512);
}

#[test]
fn benchmark_select_all_2_m_line_138() {
    assert_select_all(1024);
}

#[test]
fn benchmark_select_all_10_m_line_143() {
    assert_select_all(2048);
}

#[test]
fn benchmark_single_col_100_k_line_152() {
    assert_single_col(128);
}

#[test]
fn benchmark_single_col_1_m_line_157() {
    assert_single_col(512);
}

#[test]
fn benchmark_single_col_2_m_line_162() {
    assert_single_col(1024);
}

#[test]
fn benchmark_single_col_10_m_line_167() {
    assert_single_col(2048);
}

#[test]
fn benchmark_aggregate_count_100_k_line_176() {
    assert_count(128);
}

#[test]
fn benchmark_aggregate_count_1_m_line_181() {
    assert_count(512);
}

#[test]
fn benchmark_aggregate_count_2_m_line_186() {
    assert_count(1024);
}

#[test]
fn benchmark_aggregate_count_10_m_line_191() {
    assert_count(2048);
}
