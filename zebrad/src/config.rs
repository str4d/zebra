//! Zebrad Config
//!
//! See instructions in `commands.rs` to specify the path to your
//! application's configuration file and/or command-line options
//! for specifying it.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use zebra_network::Config as NetworkSection;
use zebra_state::Config as StateSection;

/// Configuration for `zebrad`.
///
/// The `zebrad` config is a TOML-encoded version of this structure. The meaning
/// of each field is described in the documentation, although it may be necessary
/// to click through to the sub-structures for each section.
#[derive(Clone, Default, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZebradConfig {
    /// Metrics configuration
    pub metrics: MetricsSection,

    /// Networking configuration
    pub network: NetworkSection,

    /// State configuration
    pub state: StateSection,

    /// Tracing configuration
    pub tracing: TracingSection,
}

/// Tracing configuration section.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct TracingSection {
    /// The filter used for tracing events.
    pub filter: Option<String>,

    /// The endpoint address used for tracing.
    pub endpoint_addr: SocketAddr,
}

impl Default for TracingSection {
    fn default() -> Self {
        Self::populated()
    }
}

impl TracingSection {
    pub fn populated() -> Self {
        Self {
            filter: Some("info".to_owned()),
            endpoint_addr: "0.0.0.0:3000".parse().unwrap(),
        }
    }
}

/// Metrics configuration section.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsSection {
    /// The endpoint address used for metrics.
    pub endpoint_addr: SocketAddr,
}

impl Default for MetricsSection {
    fn default() -> Self {
        Self {
            endpoint_addr: "0.0.0.0:9999".parse().unwrap(),
        }
    }
}

#[cfg(test)]
mod test {
    use color_eyre::eyre::Result;

    #[test]
    fn test_toml_ser() -> Result<()> {
        let default_config = super::ZebradConfig::default();
        println!("Default config: {:?}", default_config);

        println!("Toml:\n{}", toml::Value::try_from(&default_config)?);

        Ok(())
    }
}
