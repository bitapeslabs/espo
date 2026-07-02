use super::config::MemgraphConfig;
use anyhow::{Context, Result, anyhow};
use rsmgclient::{ConnectParams, Connection, QueryParam, SSLMode};
use serde_json::{Map, Number, Value};
use std::collections::HashMap;

#[derive(Clone)]
pub(crate) struct MemgraphHost {
    host: String,
    port: u16,
    ssl: bool,
    username: Option<String>,
    password: Option<String>,
}

impl MemgraphHost {
    pub(crate) fn new(config: &MemgraphConfig) -> Result<Self> {
        let host = Self {
            host: config.host.clone(),
            port: config.port,
            ssl: config.ssl,
            username: config.username.clone(),
            password: config.password.clone(),
        };
        host.execute("RETURN 1", Value::Object(Map::new()))
            .with_context(|| format!("connect to Memgraph at {}", config.uri))?;
        Ok(host)
    }

    pub(crate) fn execute(&self, statement: &'static str, parameters: Value) -> Result<()> {
        let params = json_object_to_query_params(parameters)?;
        let mut conn = self.connect()?;
        conn.execute(statement, Some(&params))
            .with_context(|| format!("Memgraph query failed: {statement}"))?;
        conn.fetchall().context("Memgraph query result fetch failed")?;
        conn.commit().context("Memgraph commit failed")?;
        Ok(())
    }

    pub(crate) fn execute_many_without_params(&self, statements: Vec<&'static str>) -> Result<()> {
        for statement in statements {
            self.execute(statement, Value::Object(Map::new()))?;
        }
        Ok(())
    }

    fn connect(&self) -> Result<Connection> {
        let connect_params = ConnectParams {
            host: Some(self.host.clone()),
            port: self.port,
            username: self.username.clone(),
            password: self.password.clone(),
            sslmode: if self.ssl { SSLMode::Require } else { SSLMode::Disable },
            autocommit: true,
            ..Default::default()
        };
        Connection::connect(&connect_params).context("Memgraph Bolt connection failed")
    }
}

fn json_object_to_query_params(value: Value) -> Result<HashMap<String, QueryParam>> {
    let Value::Object(map) = value else {
        return Err(anyhow!("Memgraph query parameters must be a JSON object"));
    };

    let mut out = HashMap::with_capacity(map.len());
    for (key, value) in map {
        out.insert(key, json_to_query_param(value)?);
    }
    Ok(out)
}

fn json_to_query_param(value: Value) -> Result<QueryParam> {
    Ok(match value {
        Value::Null => QueryParam::Null,
        Value::Bool(value) => QueryParam::Bool(value),
        Value::Number(value) => number_to_query_param(value)?,
        Value::String(value) => QueryParam::String(value),
        Value::Array(values) => QueryParam::List(
            values
                .into_iter()
                .map(json_to_query_param)
                .collect::<Result<Vec<QueryParam>>>()?,
        ),
        Value::Object(values) => QueryParam::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, json_to_query_param(value)?)))
                .collect::<Result<HashMap<String, QueryParam>>>()?,
        ),
    })
}

fn number_to_query_param(value: Number) -> Result<QueryParam> {
    if let Some(value) = value.as_i64() {
        return Ok(QueryParam::Int(value));
    }
    if let Some(value) = value.as_u64() {
        let value = i64::try_from(value)
            .map_err(|_| anyhow!("Memgraph integer parameter exceeds i64: {value}"))?;
        return Ok(QueryParam::Int(value));
    }
    value
        .as_f64()
        .map(QueryParam::Float)
        .ok_or_else(|| anyhow!("Memgraph numeric parameter was not finite"))
}
