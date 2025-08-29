use super::calculator::{DataHistory, ProcessedData};
use super::config::Config;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::json;

#[derive(Debug, Clone, PartialEq)]
pub enum MQTTHealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}
#[derive(Debug, Clone)]
pub struct MQTTState {
    pub status: MQTTHealthStatus,
    pub last_successful_publish: Option<std::time::Instant>,
    pub failed_publish_count: u32,
    pub last_error: Option<String>,
}

impl Default for MQTTState {
    fn default() -> Self {
        Self {
            status: MQTTHealthStatus::Unknown,
            last_successful_publish: None,
            failed_publish_count: 0,
            last_error: None,
        }
    }
}
