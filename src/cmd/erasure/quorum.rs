use crate::cmd::*;

pub fn object_quorum_from_meta(
    parts: &[FileInfo],
    errs: &[Option<String>],
    default_parity_blocks: i32,
) -> Result<(i32, i32), String> {
    if parts.is_empty() {
        return Err(cmd_err(ERR_FILE_NOT_FOUND));
    }

    let available_parts = parts
        .iter()
        .enumerate()
        .filter(|(index, _)| errs.get(*index).is_none_or(|err| err.is_none()))
        .map(|(_, part)| part)
        .collect::<Vec<_>>();
    if available_parts.is_empty() {
        return Err(cmd_err(ERR_FILE_NOT_FOUND));
    }

    let parity = available_parts
        .iter()
        .find_map(|part| (part.erasure.parity_blocks > 0).then_some(part.erasure.parity_blocks))
        .unwrap_or(default_parity_blocks);
    let total_disks = parts.len() as i32;
    if parity < 0 || parity >= total_disks {
        return Err(cmd_err(ERR_INVALID_ARGUMENT));
    }

    let quorum = (total_disks - parity).max(1);
    Ok((quorum, quorum))
}
