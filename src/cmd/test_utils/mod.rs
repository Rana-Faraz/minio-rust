use std::error::Error;

use crate::cmd::{to_api_error_code, ApiErrorCode};

pub fn to_object_err<E>(err: Option<E>) -> Option<E> {
    err
}

pub fn to_storage_err<E>(err: Option<E>) -> Option<E> {
    err
}

pub fn to_api_error(err: Option<&(dyn Error + 'static)>) -> ApiErrorCode {
    to_api_error_code(err)
}
