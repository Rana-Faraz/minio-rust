use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::cmd::{
    BaseOptions, CheckPartsHandlerParams, CheckPartsResp, DeleteBulkReq, DeleteFileHandlerParams,
    DeleteOptions, DeleteVersionHandlerParams, DeleteVersionsErrsResp, DiskInfo, DiskInfoOptions,
    DiskMetrics, FileInfo, FileInfoVersions, FilesInfo, ListDirResult, LocalDiskIDs,
    MetadataHandlerParams, RawFileInfo, ReadAllHandlerParams, ReadMultipleReq, ReadMultipleResp,
    ReadPartsReq, ReadPartsResp, RenameDataHandlerParams, RenameDataInlineHandlerParams,
    RenameDataResp, RenameFileHandlerParams, RenameOptions, RenamePartHandlerParams,
    UpdateMetadataOpts, VolInfo, VolsInfo, WriteAllHandlerParams,
};

pub const SOURCE_FILE: &str = "cmd/storage-datatypes_gen_test.go";

trait CmdCodec: Default + Clone + PartialEq + Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

macro_rules! impl_cmd_codec {
    ($ty:ty) => {
        impl CmdCodec for $ty {
            fn marshal_msg(&self) -> Result<Vec<u8>, String> {
                <$ty>::marshal_msg(self)
            }
            fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
                <$ty>::unmarshal_msg(self, bytes)
            }
            fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String> {
                <$ty>::encode(self, writer)
            }
            fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
                <$ty>::decode(self, reader)
            }
            fn msgsize(&self) -> usize {
                <$ty>::msgsize(self)
            }
        }
    };
}

impl_cmd_codec!(BaseOptions);
impl_cmd_codec!(CheckPartsHandlerParams);
impl_cmd_codec!(CheckPartsResp);
impl_cmd_codec!(DeleteBulkReq);
impl_cmd_codec!(DeleteFileHandlerParams);
impl_cmd_codec!(DeleteOptions);
impl_cmd_codec!(DeleteVersionHandlerParams);
impl_cmd_codec!(DeleteVersionsErrsResp);
impl_cmd_codec!(DiskInfo);
impl_cmd_codec!(DiskInfoOptions);
impl_cmd_codec!(DiskMetrics);
impl_cmd_codec!(FileInfo);
impl_cmd_codec!(FileInfoVersions);
impl_cmd_codec!(FilesInfo);
impl_cmd_codec!(ListDirResult);
impl_cmd_codec!(LocalDiskIDs);
impl_cmd_codec!(MetadataHandlerParams);
impl_cmd_codec!(RawFileInfo);
impl_cmd_codec!(ReadAllHandlerParams);
impl_cmd_codec!(ReadMultipleReq);
impl_cmd_codec!(ReadMultipleResp);
impl_cmd_codec!(ReadPartsReq);
impl_cmd_codec!(ReadPartsResp);
impl_cmd_codec!(RenameDataHandlerParams);
impl_cmd_codec!(RenameDataInlineHandlerParams);
impl_cmd_codec!(RenameDataResp);
impl_cmd_codec!(RenameFileHandlerParams);
impl_cmd_codec!(RenameOptions);
impl_cmd_codec!(RenamePartHandlerParams);
impl_cmd_codec!(UpdateMetadataOpts);
impl_cmd_codec!(VolInfo);
impl_cmd_codec!(VolsInfo);
impl_cmd_codec!(WriteAllHandlerParams);

fn assert_roundtrip<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = T::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

fn assert_encode_decode<T: CmdCodec>(value: T) {
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode");
    assert!(value.msgsize() >= buf.get_ref().len());
    buf.set_position(0);
    let mut decoded = T::default();
    decoded.decode(&mut buf).expect("decode");
    assert_eq!(decoded, value);
}

fn exercise_benchmark_smoke<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    assert!(value.msgsize() >= bytes.len());
    for _ in 0..50 {
        let mut decoded = T::default();
        decoded.unmarshal_msg(&bytes).expect("decode");
    }
}

macro_rules! codec_tests {
    ($module:ident, $ty:ty) => {
        mod $module {
            use super::*;

            #[test]
            fn roundtrip() {
                assert_roundtrip(<$ty>::default());
            }

            #[test]
            fn encode_decode() {
                assert_encode_decode(<$ty>::default());
            }

            #[test]
            fn benchmark_smoke() {
                exercise_benchmark_smoke(<$ty>::default());
            }
        }
    };
}

codec_tests!(base_options, BaseOptions);
codec_tests!(check_parts_handler_params, CheckPartsHandlerParams);
codec_tests!(check_parts_resp, CheckPartsResp);
codec_tests!(delete_bulk_req, DeleteBulkReq);
codec_tests!(delete_file_handler_params, DeleteFileHandlerParams);
codec_tests!(delete_options, DeleteOptions);
codec_tests!(delete_version_handler_params, DeleteVersionHandlerParams);
codec_tests!(delete_versions_errs_resp, DeleteVersionsErrsResp);
codec_tests!(disk_info, DiskInfo);
codec_tests!(disk_info_options, DiskInfoOptions);
codec_tests!(disk_metrics, DiskMetrics);
codec_tests!(file_info, FileInfo);
codec_tests!(file_info_versions, FileInfoVersions);
codec_tests!(files_info, FilesInfo);
codec_tests!(list_dir_result, ListDirResult);
codec_tests!(local_disk_ids, LocalDiskIDs);
codec_tests!(metadata_handler_params, MetadataHandlerParams);
codec_tests!(raw_file_info, RawFileInfo);
codec_tests!(read_all_handler_params, ReadAllHandlerParams);
codec_tests!(read_multiple_req, ReadMultipleReq);
codec_tests!(read_multiple_resp, ReadMultipleResp);
codec_tests!(read_parts_req, ReadPartsReq);
codec_tests!(read_parts_resp, ReadPartsResp);
codec_tests!(rename_data_handler_params, RenameDataHandlerParams);
codec_tests!(
    rename_data_inline_handler_params,
    RenameDataInlineHandlerParams
);
codec_tests!(rename_data_resp, RenameDataResp);
codec_tests!(rename_file_handler_params, RenameFileHandlerParams);
codec_tests!(rename_options, RenameOptions);
codec_tests!(rename_part_handler_params, RenamePartHandlerParams);
codec_tests!(update_metadata_opts, UpdateMetadataOpts);
codec_tests!(vol_info, VolInfo);
codec_tests!(vols_info, VolsInfo);
codec_tests!(write_all_handler_params, WriteAllHandlerParams);
