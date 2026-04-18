use super::*;

impl ObjectApiHandlers {
    pub(crate) fn authorize(
        &self,
        auth: &RequestAuth,
        resource: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), HandlerResponse> {
        match auth.kind {
            RequestAuthKind::Anonymous => Err(api_error_response(
                403,
                "AccessDenied",
                "Access Denied.",
                resource,
                bucket,
                key,
            )),
            _ if auth.access_key != self.credentials.access_key => Err(api_error_response(
                403,
                "InvalidAccessKeyId",
                "The Access Key Id you provided does not exist in our records.",
                resource,
                bucket,
                key,
            )),
            _ if auth.prevalidated => Ok(()),
            _ if auth.secret_key != self.credentials.secret_key => Err(api_error_response(
                403,
                "SignatureDoesNotMatch",
                "The request signature we calculated does not match the signature you provided.",
                resource,
                bucket,
                key,
            )),
            _ => Ok(()),
        }
    }
}
