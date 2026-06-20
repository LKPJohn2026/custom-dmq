//! Optional bearer-token authentication on protocol handshake.

use std::io;

pub fn auth_required() -> bool {
    std::env::var("DMQ_AUTH_TOKEN").is_ok()
}

pub fn expected_token() -> Option<String> {
    std::env::var("DMQ_AUTH_TOKEN").ok()
}

pub fn validate_token(token: &[u8]) -> io::Result<()> {
    let Some(expected) = expected_token() else {
        return Ok(());
    };
    if token == expected.as_bytes() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid auth token",
        ))
    }
}

pub fn principal_from_token(token: &[u8]) -> String {
    if token.is_empty() {
        "anonymous".to_string()
    } else {
        String::from_utf8_lossy(token).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_matching_token() {
        std::env::set_var("DMQ_AUTH_TOKEN", "secret");
        assert!(validate_token(b"secret").is_ok());
        assert!(validate_token(b"wrong").is_err());
        std::env::remove_var("DMQ_AUTH_TOKEN");
    }
}
