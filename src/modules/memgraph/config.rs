use anyhow::{Result, anyhow};
use serde_json::Value;

fn default_uri() -> String {
    "bolt://127.0.0.1:7690".to_string()
}

fn get_bool(obj: &serde_json::Map<String, Value>, key: &str, default: bool) -> Result<bool> {
    match obj.get(key) {
        Some(Value::Bool(v)) => Ok(*v),
        Some(_) => Err(anyhow!("memgraph.{key} must be a boolean")),
        None => Ok(default),
    }
}

fn get_string(
    obj: &serde_json::Map<String, Value>,
    key: &str,
    default: Option<String>,
) -> Result<Option<String>> {
    match obj.get(key) {
        Some(Value::String(v)) => {
            let trimmed = v.trim().to_string();
            if trimmed.is_empty() { Ok(None) } else { Ok(Some(trimmed)) }
        }
        Some(Value::Null) => Ok(None),
        Some(_) => Err(anyhow!("memgraph.{key} must be a string or null")),
        None => Ok(default),
    }
}

fn get_u64(obj: &serde_json::Map<String, Value>, key: &str, default: u64) -> Result<u64> {
    match obj.get(key) {
        Some(Value::Number(v)) => {
            v.as_u64().ok_or_else(|| anyhow!("memgraph.{key} must be an unsigned integer"))
        }
        Some(_) => Err(anyhow!("memgraph.{key} must be an unsigned integer")),
        None => Ok(default),
    }
}

#[derive(Clone, Debug)]
pub struct MemgraphConfig {
    pub enabled: bool,
    pub uri: String,
    pub host: String,
    pub port: u16,
    pub ssl: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connect_timeout_secs: u64,
    pub request_timeout_secs: u64,
}

impl Default for MemgraphConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            uri: default_uri(),
            host: "127.0.0.1".to_string(),
            port: 7690,
            ssl: false,
            username: None,
            password: None,
            connect_timeout_secs: 5,
            request_timeout_secs: 30,
        }
    }
}

impl MemgraphConfig {
    pub const SPEC: &'static str = concat!(
        "{ \"enable\": <bool=true>, \"uri\": \"bolt://127.0.0.1:7690\", ",
        "\"username\": <string|null>, \"password\": <string|null>, ",
        "\"connect_timeout_secs\": <number=5>, \"request_timeout_secs\": <number=30> }"
    );

    pub fn from_json(value: &Value) -> Result<Self> {
        let obj = value.as_object().ok_or_else(|| {
            anyhow!("memgraph module config must be an object; expected {}", Self::SPEC)
        })?;

        let uri = get_string(obj, "uri", Some(default_uri()))?.unwrap_or_else(default_uri);
        let (host, port, ssl) = parse_bolt_uri(&uri)?;

        Ok(Self {
            enabled: get_bool(obj, "enable", true)?,
            uri,
            host,
            port,
            ssl,
            username: get_string(obj, "username", None)?,
            password: get_string(obj, "password", None)?,
            connect_timeout_secs: get_u64(obj, "connect_timeout_secs", 5)?,
            request_timeout_secs: get_u64(obj, "request_timeout_secs", 30)?,
        })
    }
}

pub fn memgraph_enabled_from_config(value: Option<&Value>) -> bool {
    let Some(value) = value else {
        return false;
    };
    MemgraphConfig::from_json(value).map(|cfg| cfg.enabled).unwrap_or(false)
}

fn parse_bolt_uri(uri: &str) -> Result<(String, u16, bool)> {
    let uri = uri.trim();
    if uri.is_empty() {
        return Err(anyhow!("memgraph.uri must be non-empty"));
    }

    let (rest, ssl) = if let Some(rest) = uri.strip_prefix("bolt+s://") {
        (rest, true)
    } else if let Some(rest) = uri.strip_prefix("memgraph+s://") {
        (rest, true)
    } else if let Some(rest) = uri.strip_prefix("bolt://") {
        (rest, false)
    } else if let Some(rest) = uri.strip_prefix("memgraph://") {
        (rest, false)
    } else {
        (uri, false)
    };

    let rest = rest.trim_end_matches('/');
    if rest.is_empty() || rest.contains('/') {
        return Err(anyhow!("memgraph.uri must be a Bolt host[:port], not a path"));
    }

    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
            let port =
                port.parse::<u16>().map_err(|_| anyhow!("memgraph.uri port must fit in u16"))?;
            (host, port)
        }
        _ => (rest, 7687),
    };

    if host.is_empty() {
        return Err(anyhow!("memgraph.uri host must be non-empty"));
    }

    Ok((host.trim_matches(['[', ']']).to_string(), port, ssl))
}
