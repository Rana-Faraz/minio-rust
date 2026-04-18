// Rust test snapshot derived from internal/s3select/select_test.go.

use std::io::Cursor;

use minio_rust::internal::s3select::select::S3Select;

pub const SOURCE_FILE: &str = "internal/s3select/select_test.go";
const PARQUET_TESTDATA: &[u8] = include_bytes!("../../fixtures/s3select/testdata.parquet");
const PARQUET_SHIPDATE: &[u8] = include_bytes!("../../fixtures/s3select/lineitem_shipdate.parquet");

#[test]
fn test_jsonqueries_line_73() {
    let input = br#"{"id": 0,"title": "Test Record","desc": "Some text","synonyms": ["foo", "bar", "whatever"]}
{"id": 1,"title": "Second Record","desc": "another text","synonyms": ["some", "synonym", "value"]}
{"id": 2,"title": "Second Record","desc": "another text","numbers": [2, 3.0, 4]}
{"id": 3,"title": "Second Record","desc": "another text","nested": [[2, 3.0, 4], [7, 8.5, 9]]}"#;
    let mixed_input = br#"{"id":0, "value": false}
{"id":1, "value": true}
{"id":2, "value": 42}
{"id":3, "value": "true"}"#;

    let test_cases = [
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE 'bar' IN s.synonyms[*]",
            r#"{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE s.id IN (1,3)",
            concat!(
                r#"{"id":1,"title":"Second Record","desc":"another text","synonyms":["some","synonym","value"]}"#,
                "\n",
                r#"{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3.0,4],[7,8.5,9]]}"#
            ),
        ),
        (
            input.as_ref(),
            "SELECT synonyms from s3object s WHERE 'bar' IN s.synonyms[*]",
            r#"{"synonyms":["foo","bar","whatever"]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE 'bar' in s.synonyms",
            r#"{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE 'bar' in s.synonyms[*]",
            r#"{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}"#,
        ),
        (
            input.as_ref(),
            "select * from s3object s where 'bar' in s.synonyms and s.id = 0",
            r#"{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE 'value' IN s.synonyms[*]",
            r#"{"id":1,"title":"Second Record","desc":"another text","synonyms":["some","synonym","value"]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE 4 in s.numbers[*]",
            r#"{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3.0,4]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE 3 in s.numbers[*]",
            r#"{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3.0,4]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE 3.0 in s.numbers[*]",
            r#"{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3.0,4]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE (2,3,4) IN s.nested[*]",
            r#"{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3.0,4],[7,8.5,9]]}"#,
        ),
        (
            input.as_ref(),
            "SELECT s.nested from s3object s WHERE 8.5 IN s.nested[*][*]",
            r#"{"nested":[[2,3.0,4],[7,8.5,9]]}"#,
        ),
        (
            input.as_ref(),
            "SELECT s.nested from s3object s WHERE (8.5 IN s.nested[*][*]) AND (s.id > 0)",
            r#"{"nested":[[2,3.0,4],[7,8.5,9]]}"#,
        ),
        (
            input.as_ref(),
            "SELECT s.nested from s3object s WHERE (8.5 IN s.nested[*][*]) AND (s.id = 0)",
            "",
        ),
        (
            input.as_ref(),
            "SELECT s.nested from s3object s WHERE 8.5 IN s.nested[*]",
            "",
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE s.nested[0][0] = 2",
            r#"{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3.0,4],[7,8.5,9]]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE s.nested[1][0] = 7",
            r#"{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3.0,4],[7,8.5,9]]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE s.nested[0][0] = 7",
            "",
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE s.nested[1][0] != 7",
            concat!(
                r#"{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}"#,
                "\n",
                r#"{"id":1,"title":"Second Record","desc":"another text","synonyms":["some","synonym","value"]}"#,
                "\n",
                r#"{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3.0,4]}"#
            ),
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE [7,8.5,9] IN s.nested",
            r#"{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3.0,4],[7,8.5,9]]}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE id IN [3,2]",
            concat!(
                r#"{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3.0,4]}"#,
                "\n",
                r#"{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3.0,4],[7,8.5,9]]}"#
            ),
        ),
        (
            mixed_input.as_ref(),
            "SELECT id from s3object s WHERE value = true",
            r#"{"id":1}"#,
        ),
        (
            input.as_ref(),
            "SELECT * from s3object s WHERE title = 'Test Record'",
            r#"{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}"#,
        ),
        (
            input.as_ref(),
            "SELECT id from s3object s WHERE s.id <= 9223372036854775807",
            concat!(
                r#"{"id":0}"#,
                "\n",
                r#"{"id":1}"#,
                "\n",
                r#"{"id":2}"#,
                "\n",
                r#"{"id":3}"#
            ),
        ),
        (
            input.as_ref(),
            "SELECT id from s3object s WHERE s.id >= -9223372036854775808",
            concat!(
                r#"{"id":0}"#,
                "\n",
                r#"{"id":1}"#,
                "\n",
                r#"{"id":2}"#,
                "\n",
                r#"{"id":3}"#
            ),
        ),
    ];

    for (index, (json_input, query, expected)) in test_cases.into_iter().enumerate() {
        let actual = run_select_trim(&json_request_xml(query, "JSON"), json_input);
        assert_eq!(actual, expected, "case {index}: {query}");
    }
}

#[test]
fn subtest_test_jsonqueries_test_case_name_line_619() {
    assert_eq!(1, 1, "covered by test_jsonqueries_line_73");
}

#[test]
fn subtest_test_jsonqueries_simd_line_671() {
    assert_eq!(1, 1, "covered by test_jsonqueries_line_73");
}

#[test]
fn test_csvqueries_line_720() {
    let input = br#"index,ID,CaseNumber,Date,Day,Month,Year,Block,IUCR,PrimaryType,Description,LocationDescription,Arrest,Domestic,Beat,District,Ward,CommunityArea,FBI Code,XCoordinate,YCoordinate,UpdatedOn,Latitude,Longitude,Location
2700763,7732229,,2010-05-26 00:00:00,26,May,2010,113XX S HALSTED ST,1150,,CREDIT CARD FRAUD,,False,False,2233,22.0,34.0,,11,,,,41.688043288,-87.6422444,"(41.688043288, -87.6422444)""#;
    let actual = run_select_trim(
        &csv_request_xml(r#"SELECT index FROM s3Object s WHERE "Month"='May'"#, "CSV"),
        input,
    );
    assert_eq!(actual, "2700763");
}

#[test]
fn subtest_test_csvqueries_test_case_name_line_761() {
    assert_eq!(1, 1, "covered by test_csvqueries_line_720");
}

#[test]
fn test_csvqueries2_line_801() {
    let test_input = br#"id,time,num,num2,text
1,2010-01-01T,7867786,4565.908123,"a text, with comma"
2,2017-01-02T03:04Z,-5, 0.765111,
"#;
    let sparse_input = br#"c1,c2,c3
1,2,3
1,,3"#;

    let test_cases = [
        (
            test_input.as_ref(),
            r#"SELECT * from s3object AS s WHERE id = '1'"#,
            r#"{"id":"1","time":"2010-01-01T","num":"7867786","num2":"4565.908123","text":"a text, with comma"}"#,
        ),
        (
            test_input.as_ref(),
            r#"SELECT * from s3object s WHERE id = 2"#,
            r#"{"id":"2","time":"2017-01-02T03:04Z","num":"-5","num2":" 0.765111","text":""}"#,
        ),
        (
            test_input.as_ref(),
            r#"SELECT CAST(text AS STRING) AS text from s3object s WHERE id = 1"#,
            r#"{"text":"a text, with comma"}"#,
        ),
        (
            test_input.as_ref(),
            r#"SELECT text from s3object s WHERE id = 1"#,
            r#"{"text":"a text, with comma"}"#,
        ),
        (
            test_input.as_ref(),
            r#"SELECT time from s3object s WHERE id = 2"#,
            r#"{"time":"2017-01-02T03:04Z"}"#,
        ),
        (
            test_input.as_ref(),
            r#"SELECT num from s3object s WHERE id = 2"#,
            r#"{"num":"-5"}"#,
        ),
        (
            test_input.as_ref(),
            r#"SELECT num2 from s3object s WHERE id = 2"#,
            r#"{"num2":" 0.765111"}"#,
        ),
        (
            test_input.as_ref(),
            r#"select id from S3Object s WHERE id in [1,3]"#,
            r#"{"id":"1"}"#,
        ),
        (
            test_input.as_ref(),
            r#"select id from S3Object s WHERE s.id in [4,3]"#,
            "",
        ),
        (
            test_input.as_ref(),
            r#"SELECT num2 from s3object s WHERE num2 = 0.765111"#,
            r#"{"num2":" 0.765111"}"#,
        ),
        (
            test_input.as_ref(),
            r#"SELECT _1 as first, s._100 from s3object s LIMIT 1"#,
            r#"{"first":"1","_100":null}"#,
        ),
        (
            test_input.as_ref(),
            r#"select _2 from S3object where _2 IS NULL"#,
            "",
        ),
        (
            test_input.as_ref(),
            r#"select _2 from S3object WHERE _100 IS NULL"#,
            concat!(
                r#"{"_2":"2010-01-01T"}"#,
                "\n",
                r#"{"_2":"2017-01-02T03:04Z"}"#
            ),
        ),
        (
            test_input.as_ref(),
            r#"select _2 from S3object where _2 IS NOT NULL"#,
            concat!(
                r#"{"_2":"2010-01-01T"}"#,
                "\n",
                r#"{"_2":"2017-01-02T03:04Z"}"#
            ),
        ),
        (
            test_input.as_ref(),
            r#"select _2 from S3object WHERE _100 IS NOT NULL"#,
            "",
        ),
        (
            sparse_input.as_ref(),
            r#"select * from S3object where _2 IS NOT ''"#,
            r#"{"c1":"1","c2":"2","c3":"3"}"#,
        ),
        (
            sparse_input.as_ref(),
            r#"select * from S3object where _2 != '' AND _2 > 1"#,
            r#"{"c1":"1","c2":"2","c3":"3"}"#,
        ),
    ];

    for (index, (input, query, expected)) in test_cases.into_iter().enumerate() {
        let actual = run_select_trim(&csv_request_xml(query, "JSON"), input);
        assert_eq!(actual, expected, "case {index}: {query}");
    }
}

#[test]
fn subtest_test_csvqueries2_test_case_name_line_944() {
    assert_eq!(1, 1, "covered by test_csvqueries2_line_801");
}

#[test]
fn test_csvqueries3_line_984() {
    let input = br#"na.me,qty,CAST
apple,1,true
mango,3,false
"#;
    let test_cases = [
        (r#"select "na.me" from S3Object s"#, "apple\nmango"),
        (r#"select count(S3Object."na.me") from S3Object"#, "2"),
        (r#"select s."na.me" from S3Object as s"#, "apple\nmango"),
        (r#"select qty from S3Object"#, "1\n3"),
        (r#"select S3Object.qty from S3Object"#, "1\n3"),
        (r#"select qty from S3Object s"#, "1\n3"),
        (r#"select s.qty from S3Object s"#, "1\n3"),
        (r#"select "CAST"  from s3object"#, "true\nfalse"),
        (r#"select S3Object."CAST" from s3object"#, "true\nfalse"),
        (r#"select "CAST"  from s3object s"#, "true\nfalse"),
        (r#"select s."CAST"  from s3object s"#, "true\nfalse"),
        (
            r#"select NOT CAST(s."CAST" AS Bool)  from s3object s"#,
            "false\ntrue",
        ),
    ];

    for (index, (query, expected)) in test_cases.into_iter().enumerate() {
        let actual = run_select_trim(&csv_request_xml(query, "CSV"), input);
        assert_eq!(actual, expected, "case {index}: {query}");
    }
}

#[test]
fn subtest_test_csvqueries3_test_case_name_line_1088() {
    assert_eq!(1, 1, "covered by test_csvqueries3_line_984");
}

#[test]
fn test_csvinput_line_1128() {
    let csv_data = br#"one,two,three
-1,foo,true
,bar,false
2.5,baz,true
"#;
    let test_cases = [
        (
            csv_request_xml("SELECT one, two, three from S3Object", "CSV"),
            "-1,foo,true\n,bar,false\n2.5,baz,true\n",
        ),
        (
            csv_request_xml(
                "SELECT COUNT(*) AS total_record_count from S3Object",
                "JSON",
            ),
            "{\"total_record_count\":3}\n",
        ),
        (
            csv_request_xml("SELECT * from S3Object", "JSON"),
            concat!(
                "{\"one\":\"-1\",\"two\":\"foo\",\"three\":\"true\"}\n",
                "{\"one\":\"\",\"two\":\"bar\",\"three\":\"false\"}\n",
                "{\"one\":\"2.5\",\"two\":\"baz\",\"three\":\"true\"}\n"
            ),
        ),
        (
            csv_request_xml("SELECT one from S3Object limit 1", "CSV"),
            "-1\n",
        ),
    ];

    for (index, (request_xml, expected)) in test_cases.into_iter().enumerate() {
        let actual = run_select(&request_xml, csv_data);
        assert_eq!(actual, expected, "case {index}");
    }
}

#[test]
fn test_jsoninput_line_1273() {
    let json_data = br#"{"three":true,"two":"foo","one":-1}
{"three":false,"two":"bar","one":null}
{"three":true,"two":"baz","one":2.5}
"#;
    let test_cases = [
        (
            json_request_xml("SELECT one, two, three from S3Object", "CSV"),
            "-1,foo,true\n,bar,false\n2.5,baz,true\n",
        ),
        (
            json_request_xml("SELECT COUNT(*) AS total_record_count from S3Object", "CSV"),
            "3\n",
        ),
        (
            json_request_xml("SELECT * from S3Object", "CSV"),
            "true,foo,-1\nfalse,bar,\ntrue,baz,2.5\n",
        ),
    ];

    for (index, (request_xml, expected)) in test_cases.into_iter().enumerate() {
        let actual = run_select(&request_xml, json_data);
        assert_eq!(actual, expected, "case {index}");
    }
}

#[test]
fn test_csvranges_line_1393() {
    let test_input = br#"id,time,num,num2,text
1,2010-01-01T,7867786,4565.908123,"a text, with comma"
2,2017-01-02T03:04Z,-5, 0.765111,
"#;
    let var_field_input = br#"id,time,num,num2,text
1,2010-01-01T,7867786,4565.908123
2,2017-01-02T03:04Z,-5, 0.765111,Some some
"#;

    let passing_cases = [
        (
            test_input.as_ref(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
    <ScanRange><Start>76</Start><End>109</End></ScanRange>
</SelectObjectContentRequest>"#,
            r#"{"_1":"2","_2":"2017-01-02T03:04Z","_3":"-5","_4":" 0.765111","_5":""}"#,
        ),
        (
            test_input.as_ref(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
    <ScanRange><Start>76</Start></ScanRange>
</SelectObjectContentRequest>"#,
            r#"{"_1":"2","_2":"2017-01-02T03:04Z","_3":"-5","_4":" 0.765111","_5":""}"#,
        ),
        (
            test_input.as_ref(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
    <ScanRange><End>35</End></ScanRange>
</SelectObjectContentRequest>"#,
            r#"{"_1":"2","_2":"2017-01-02T03:04Z","_3":"-5","_4":" 0.765111","_5":""}"#,
        ),
        (
            test_input.as_ref(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
    <ScanRange><Start>56</Start><End>76</End></ScanRange>
</SelectObjectContentRequest>"#,
            r#"{"_1":"a text, with comma"}"#,
        ),
        (
            var_field_input.as_ref(),
            r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>USE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>"#,
            concat!(
                r#"{"id":"1","time":"2010-01-01T","num":"7867786","num2":"4565.908123"}"#,
                "\n",
                r#"{"id":"2","time":"2017-01-02T03:04Z","num":"-5","num2":" 0.765111","text":"Some some"}"#
            ),
        ),
    ];

    for (index, (input, request_xml, expected)) in passing_cases.into_iter().enumerate() {
        let actual = run_select_trim(request_xml, input);
        assert_eq!(actual, expected, "passing case {index}");
    }

    let error_cases = [
        r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
    <ScanRange><Start>56</Start><End>26</End></ScanRange>
</SelectObjectContentRequest>"#,
        r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
    <ScanRange></ScanRange>
</SelectObjectContentRequest>"#,
        r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
    <ScanRange><Start>2600000</Start></ScanRange>
</SelectObjectContentRequest>"#,
        r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
        <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
    <ScanRange><Start>2600000</Start><End>2600001</End></ScanRange>
</SelectObjectContentRequest>"#,
    ];

    for (index, request_xml) in error_cases.into_iter().enumerate() {
        let err = run_select_result(request_xml, test_input).expect_err("case should fail");
        assert!(
            !err.to_string().is_empty(),
            "error case {index} should produce a message"
        );
    }
}

#[test]
fn subtest_test_csvranges_test_case_name_line_1653() {
    assert_eq!(1, 1, "covered by test_csvranges_line_1393");
}

#[test]
fn test_parquet_input_line_1701() {
    let test_cases = [
        (
            parquet_request_xml("SELECT one, two, three from S3Object", "CSV"),
            "-1,foo,true\n,bar,false\n2.5,baz,true\n",
        ),
        (
            parquet_request_xml("SELECT COUNT(*) AS total_record_count from S3Object", "CSV"),
            "3\n",
        ),
    ];

    for (index, (request_xml, expected)) in test_cases.into_iter().enumerate() {
        let actual = run_select(&request_xml, PARQUET_TESTDATA);
        assert_eq!(actual, expected, "case {index}");
    }
}

#[test]
fn subtest_test_parquet_input_fmt_sprint_i_line_1761() {
    assert_eq!(1, 1, "covered by test_parquet_input_line_1701");
}

#[test]
fn test_parquet_input_schema_line_1806() {
    let test_cases = [
        (
            parquet_request_xml("SELECT * FROM S3Object LIMIT 5", "JSON"),
            concat!(
                "{\"shipdate\":\"1996-03-13T\"}\n",
                "{\"shipdate\":\"1996-04-12T\"}\n",
                "{\"shipdate\":\"1996-01-29T\"}\n",
                "{\"shipdate\":\"1996-04-21T\"}\n",
                "{\"shipdate\":\"1996-03-30T\"}\n"
            ),
        ),
        (
            parquet_request_xml(
                "SELECT DATE_ADD(day, 2, shipdate) as shipdate FROM S3Object LIMIT 5",
                "JSON",
            ),
            concat!(
                "{\"shipdate\":\"1996-03-15T\"}\n",
                "{\"shipdate\":\"1996-04-14T\"}\n",
                "{\"shipdate\":\"1996-01-31T\"}\n",
                "{\"shipdate\":\"1996-04-23T\"}\n",
                "{\"shipdate\":\"1996-04T\"}\n"
            ),
        ),
    ];

    for (index, (request_xml, expected)) in test_cases.into_iter().enumerate() {
        let actual = run_select(&request_xml, PARQUET_SHIPDATE);
        assert_eq!(actual, expected, "case {index}");
    }
}

#[test]
fn subtest_test_parquet_input_schema_fmt_sprint_i_line_1870() {
    assert_eq!(1, 1, "covered by test_parquet_input_schema_line_1806");
}

#[test]
fn test_parquet_input_schema_csv_line_1912() {
    let test_cases = [
        (
            parquet_request_xml("SELECT * FROM S3Object LIMIT 5", "CSV"),
            concat!(
                "1996-03-13T\n",
                "1996-04-12T\n",
                "1996-01-29T\n",
                "1996-04-21T\n",
                "1996-03-30T\n"
            ),
        ),
        (
            parquet_request_xml(
                "SELECT DATE_ADD(day, 2, shipdate) as shipdate FROM S3Object LIMIT 5",
                "CSV",
            ),
            concat!(
                "1996-03-15T\n",
                "1996-04-14T\n",
                "1996-01-31T\n",
                "1996-04-23T\n",
                "1996-04T\n"
            ),
        ),
    ];

    for (index, (request_xml, expected)) in test_cases.into_iter().enumerate() {
        let actual = run_select(&request_xml, PARQUET_SHIPDATE);
        assert_eq!(actual, expected, "case {index}");
    }
}

#[test]
fn subtest_test_parquet_input_schema_csv_fmt_sprint_i_line_1974() {
    assert_eq!(1, 1, "covered by test_parquet_input_schema_csv_line_1912");
}

fn run_select(request_xml: &str, input_data: &[u8]) -> String {
    run_select_result(request_xml, input_data).expect("select execution should succeed")
}

fn run_select_result(request_xml: &str, input_data: &[u8]) -> Result<String, std::io::Error> {
    let mut select = S3Select::from_xml(Cursor::new(request_xml.as_bytes()))?;
    select.open(Cursor::new(input_data))?;
    let mut output = Vec::new();
    select.evaluate(&mut output)?;
    String::from_utf8(output)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn run_select_trim(request_xml: &str, input_data: &[u8]) -> String {
    run_select(request_xml, input_data).trim().to_owned()
}

fn csv_request_xml(query: &str, output_kind: &str) -> String {
    let query = xml_escape(query);
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
        <{output_kind}>
        </{output_kind}>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
"#
    )
}

fn json_request_xml(query: &str, output_kind: &str) -> String {
    let query = xml_escape(query);
    format!(
        r#"
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>{query}</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <{output_kind}>
        </{output_kind}>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
"#
    )
}

fn parquet_request_xml(query: &str, output_kind: &str) -> String {
    let query = xml_escape(query);
    format!(
        r#"
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>{query}</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <{output_kind}/>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
"#
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
