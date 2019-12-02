#[cfg(not(feature = "tls"))]
pub const DATABASE_URL: &str = "tcp://localhost:9000?compression=lz4";

#[cfg(feature = "tls")]
pub const DATABASE_URL: &str = "tcp://localhost:9440?secure=true&skip_verify=true";
