use crate::calculator::{DataHistory, MqttPayload, ProcessedData, SensorValue};
use crate::config::MqttConfig;
use color_eyre::eyre::Error;
use color_eyre::{Report, Result};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

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

#[derive(Debug, Clone)]
pub struct SolarMqttClient {
    pub client: AsyncClient,
    device_id: String,
    state: Arc<Mutex<MQTTState>>,
    config: MqttConfig,
}

impl SolarMqttClient {
    pub async fn new(mqtt_config: &MqttConfig, device_id: String) -> Result<Self> {
        let client_id = format!("{}_{}", mqtt_config.client_id_prefix, device_id);
        let mut mqttoptions = MqttOptions::new(client_id, &mqtt_config.broker_url, 1883);
        mqttoptions.set_keep_alive(Duration::from_secs(mqtt_config.keep_alive_secs));

        if !mqtt_config.username.is_empty() {
            mqttoptions.set_credentials(&mqtt_config.username, &mqtt_config.password);
        }

        // Set Last Will and Testament
        mqttoptions.set_last_will(rumqttc::LastWill::new(
            &mqtt_config.last_will_topic,
            mqtt_config.last_will_payload.clone(),
            mqtt_config.to_qos(),
            true,
        ));

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

        let state = Arc::new(Mutex::new(MQTTState::default()));
        let state_for_eventloop = state.clone();
        let config_for_eventloop = mqtt_config.clone();
        let device_id_for_eventloop = device_id.clone();

        tokio::spawn(async move {
            let mut consecutive_errors = 0u32;

            loop {
                match eventloop.poll().await {
                    Ok(notification) => {
                        consecutive_errors = 0;

                        match notification {
                            Event::Incoming(Packet::ConnAck(_)) => {
                                info!("MQTT connected successfully");
                                let mut state_guard = state_for_eventloop.lock().await;
                                state_guard.status = MQTTHealthStatus::Healthy;
                                state_guard.last_error = None;
                                drop(state_guard);
                            }
                            Event::Incoming(Packet::PubAck(_)) => {
                                debug!("Received publish ACK");
                                let mut state_guard = state_for_eventloop.lock().await;
                                state_guard.last_successful_publish =
                                    Some(std::time::Instant::now());
                                if state_guard.failed_publish_count > 0 {
                                    state_guard.failed_publish_count = 0;
                                    state_guard.status = MQTTHealthStatus::Healthy;
                                }
                                drop(state_guard);
                            }
                            Event::Incoming(Packet::Disconnect) => {
                                warn!("MQTT disconnected");
                                let mut state_guard = state_for_eventloop.lock().await;
                                state_guard.status = MQTTHealthStatus::Unhealthy;
                                state_guard.last_error = Some("MQTT Disconnected".to_string());
                                drop(state_guard);
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        error!(error = %e, consecutive_errors, "MQTT connection error");

                        let mut state_guard = state_for_eventloop.lock().await;
                        state_guard.status = if consecutive_errors >= 3 {
                            MQTTHealthStatus::Unhealthy
                        } else {
                            MQTTHealthStatus::Degraded
                        };
                        state_guard.last_error = Some(format!("Connection error: {}", e));
                        drop(state_guard);

                        let delay = std::cmp::min(consecutive_errors * 2, 30);
                        tokio::time::sleep(Duration::from_secs(delay as u64)).await;
                    }
                }
            }
        });

        let mqtt_client = Self {
            client,
            device_id,
            state,
            config: mqtt_config.clone(),
        };

        Ok(mqtt_client)
    }

    pub async fn is_healthy(&self) -> bool {
        let state_guard = self.state.lock().await;
        matches!(
            state_guard.status,
            MQTTHealthStatus::Healthy | MQTTHealthStatus::Degraded
        )
    }

    pub async fn get_health_status(&self) -> MQTTHealthStatus {
        let state_guard = self.state.lock().await;
        state_guard.status.clone()
    }

    pub async fn get_health_state(&self) -> MQTTState {
        let state_guard = self.state.lock().await;
        state_guard.clone()
    }

    pub async fn publish_current_data(&self, data: &ProcessedData) -> Result<()> {
        let topic = self.config.get_state_topic(&self.device_id, "power");

        match self
            .client
            .publish(
                &topic,
                self.config.to_qos(),
                false,
                data.to_state_json().to_string(),
            )
            .await
        {
            Ok(_) => {
                debug!("Published power data successfully");
                Ok(())
            }
            Err(e) => {
                let mut state_guard = self.state.lock().await;
                state_guard.failed_publish_count += 1;
                state_guard.last_error = Some(e.to_string());

                state_guard.status = if state_guard.failed_publish_count > 3 {
                    MQTTHealthStatus::Unhealthy
                } else {
                    MQTTHealthStatus::Degraded
                };

                error!(
                    error = %e,
                    failed_count = state_guard.failed_publish_count,
                    "Failed to publish power data"
                );
                drop(state_guard);
                Err(Report::new(e))
            }
        }
    }

    pub async fn publish_history_data(&self, data: &DataHistory) {
        let topic = self.config.get_state_topic(&self.device_id, "energy");

        match self
            .client
            .publish(
                &topic,
                self.config.to_qos(),
                false,
                data.to_state_json().to_string(),
            )
            .await
        {
            Ok(_) => {
                debug!("Published energy history successfully");
            }
            Err(e) => {
                // Update state für History publish errors
                let mut state_guard = self.state.lock().await;
                state_guard.last_error = Some(format!("History publish error: {}", e));

                error!(error = %e, "Failed to publish energy history");
                drop(state_guard);
            }
        }
    }

    pub async fn publish_state_data(&self, data: &ProcessedData) {
        let topic = self.config.get_state_topic(&self.device_id, "state");

        // JSON nur mit den State-Feldern für Text-Sensoren
        let state_json = json!({
            "battery_state": data.battery_status.battery_state.state_string(),
            "supply_state": data.supply_state.state_string(),
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        match self
            .client
            .publish(&topic, self.config.to_qos(), false, state_json.to_string())
            .await
        {
            Ok(_) => {
                debug!("Published state data successfully");
            }
            Err(e) => {
                // Update state für State publish errors
                let mut state_guard = self.state.lock().await;
                state_guard.last_error = Some(format!("State publish error: {}", e));

                error!(error = %e, "Failed to publish state data");
                drop(state_guard);
            }
        }
    }

    pub async fn setup_discovery(&self) -> Result<()> {
        info!("Setting up Home Assistant MQTT Discovery");

        info!("Solar Energy Monitor starting - sending discovery messages");

        self.create_sensor_config(
            "pv_production",
            "PV Production",
            "power",
            "W",
            "measurement",
            "{{ value_json.pv_production }}",
        )
        .await?;

        self.create_sensor_config(
            "consumption",
            "Power Consumption",
            "power",
            "W",
            "measurement",
            "{{ value_json.consumption }}",
        )
        .await?;

        self.create_sensor_config(
            "supply_power",
            "Grid Power",
            "power",
            "W",
            "measurement",
            "{{ value_json.supply_power }}",
        )
        .await?;

        self.create_sensor_config(
            "battery_power",
            "Battery Power",
            "power",
            "W",
            "measurement",
            "{{ value_json.battery_power }}",
        )
        .await?;

        self.create_sensor_config(
            "battery_percent",
            "Battery Charge Level",
            "battery",
            "%",
            "measurement",
            "{{ value_json.battery_percent }}",
        )
        .await?;

        self.create_sensor_config(
            "battery_energy_wh",
            "Battery Energy Stored",
            "energy_storage",
            "Wh",
            "measurement",
            "{{ value_json.battery_energy_wh }}",
        )
        .await?;

        self.create_energy_sensor_config(
            "grid_buy",
            "Grid Energy Consumed",
            "{{ value_json.grid_buy }}",
        )
        .await?;

        self.create_energy_sensor_config(
            "grid_sell",
            "Grid Energy Fed-in",
            "{{ value_json.grid_sell }}",
        )
        .await?;

        self.create_energy_sensor_config(
            "production_energy",
            "Energy Produced",
            "{{ value_json.production_energy }}",
        )
        .await?;

        self.create_energy_sensor_config(
            "consumption_energy",
            "Energy Consumed",
            "{{ value_json.consumption_energy }}",
        )
        .await?;

        self.create_energy_sensor_config(
            "battery_loaded",
            "Battery Energy Loaded",
            "{{ value_json.battery_loaded }}",
        )
        .await?;

        self.create_energy_sensor_config(
            "battery_discharge",
            "Battery Energy Discharged",
            "{{ value_json.battery_discharge }}",
        )
        .await?;

        self.create_number_sensor_config(
            "battery_cycles",
            "Battery Cycles",
            "{{ value_json.battery_cycles }}",
        )
        .await?;

        self.create_text_sensor_config(
            "battery_state",
            "Battery Status",
            "{{ value_json.battery_state }}",
        )
        .await?;

        self.create_text_sensor_config(
            "supply_state",
            "Grid Status",
            "{{ value_json.supply_state }}",
        )
        .await?;

        info!("Home Assistant Discovery setup completed");
        Ok(())
    }

    async fn create_sensor_config(
        &self,
        sensor_id: &str,
        name: &str,
        device_class: &str,
        unit: &str,
        state_class: &str,
        value_template: &str,
    ) -> Result<()> {
        let discovery_topic = self
            .config
            .get_discovery_topic("sensor", &self.device_id, sensor_id);
        let state_topic = self.config.get_state_topic(&self.device_id, "power");
        let availability_topic = self.config.get_availability_topic(&self.device_id);

        let config = json!({
            "name": name,
            "unique_id": format!("{}_{}", self.device_id, sensor_id),
            "state_topic": state_topic,
            "value_template": value_template,
            "device_class": device_class,
            "unit_of_measurement": unit,
            "state_class": state_class,
            "device": {
                "identifiers": [&self.device_id],
                "name": "Solar Energy Monitor",
                "model": "PV API v0.1.0",
                "manufacturer": "Custom",
                "serial_number": &self.device_id,
                "hw_version": "1.0",
                "sw_version": env!("CARGO_PKG_VERSION")
            },
            "origin": {
                "name": "PV API Solar Monitor",
                "sw": env!("CARGO_PKG_VERSION"),
                "url": "https://github.com/your-repo/pv_api"
            },
            "availability": {
                "topic": availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline"
            }
        });

        self.client
            .publish(
                &discovery_topic,
                self.config.to_qos(),
                true, // retain
                config.to_string(),
            )
            .await?;

        debug!("Created sensor config for {}", sensor_id);
        Ok(())
    }

    async fn create_energy_sensor_config(
        &self,
        sensor_id: &str,
        name: &str,
        value_template: &str,
    ) -> Result<()> {
        let discovery_topic = self
            .config
            .get_discovery_topic("sensor", &self.device_id, sensor_id);
        let state_topic = self.config.get_state_topic(&self.device_id, "energy");
        let availability_topic = self.config.get_availability_topic(&self.device_id);

        let config = json!({
            "name": name,
            "unique_id": format!("{}_{}", self.device_id, sensor_id),
            "state_topic": state_topic,
            "value_template": value_template,
            "device_class": "energy",
            "unit_of_measurement": "kWh",
            "state_class": "total_increasing",
            "device": {
                "identifiers": [&self.device_id],
                "name": "Solar Energy Monitor",
                "model": "PV API v0.1.0",
                "manufacturer": "Custom",
                "serial_number": &self.device_id,
                "hw_version": "1.0",
                "sw_version": env!("CARGO_PKG_VERSION")
            },
            "origin": {
                "name": "PV API Solar Monitor",
                "sw": env!("CARGO_PKG_VERSION"),
                "url": "https://github.com/your-repo/pv_api"
            },
            "availability": {
                "topic": availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline"
            }
        });

        self.client
            .publish(
                &discovery_topic,
                self.config.to_qos(),
                true,
                config.to_string(),
            )
            .await?;

        debug!("Created energy sensor config for {}", sensor_id);
        Ok(())
    }

    async fn create_text_sensor_config(
        &self,
        sensor_id: &str,
        name: &str,
        value_template: &str,
    ) -> Result<()> {
        let discovery_topic = self
            .config
            .get_discovery_topic("sensor", &self.device_id, sensor_id);
        let state_topic = self.config.get_state_topic(&self.device_id, "state");
        let availability_topic = self.config.get_availability_topic(&self.device_id);

        let config = json!({
            "name": name,
            "unique_id": format!("{}_{}", self.device_id, sensor_id),
            "state_topic": state_topic,
            "value_template": value_template,
            "device": {
                "identifiers": [&self.device_id],
                "name": "Solar Energy Monitor",
                "model": "PV API v0.1.0",
                "manufacturer": "Custom",
                "serial_number": &self.device_id,
                "hw_version": "1.0",
                "sw_version": env!("CARGO_PKG_VERSION")
            },
            "origin": {
                "name": "PV API Solar Monitor",
                "sw": env!("CARGO_PKG_VERSION"),
                "url": "https://github.com/your-repo/pv_api"
            },
            "availability": {
                "topic": availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline"
            }
        });

        self.client
            .publish(
                &discovery_topic,
                self.config.to_qos(),
                true,
                config.to_string(),
            )
            .await?;

        debug!("Created text sensor config for {}", sensor_id);
        Ok(())
    }

    async fn create_number_sensor_config(
        &self,
        sensor_id: &str,
        name: &str,
        value_template: &str,
    ) -> Result<()> {
        let discovery_topic = self
            .config
            .get_discovery_topic("sensor", &self.device_id, sensor_id);
        let state_topic = self.config.get_state_topic(&self.device_id, "energy");
        let availability_topic = self.config.get_availability_topic(&self.device_id);

        let config = json!({
            "name": name,
            "unique_id": format!("{}_{}", self.device_id, sensor_id),
            "state_topic": state_topic,
            "value_template": value_template,
            "state_class": "total",
            "device": {
                "identifiers": [&self.device_id],
                "name": "Solar Energy Monitor",
                "model": "PV API v0.1.0",
                "manufacturer": "Custom",
                "serial_number": &self.device_id,
                "hw_version": "1.0",
                "sw_version": env!("CARGO_PKG_VERSION")
            },
            "origin": {
                "name": "PV API Solar Monitor",
                "sw": env!("CARGO_PKG_VERSION"),
                "url": "https://github.com/your-repo/pv_api"
            },
            "availability": {
                "topic": availability_topic,
                "payload_available": "online",
                "payload_not_available": "offline"
            }
        });

        self.client
            .publish(
                &discovery_topic,
                self.config.to_qos(),
                true,
                config.to_string(),
            )
            .await?;

        debug!("Created number sensor config for {}", sensor_id);
        Ok(())
    }

    pub async fn publish_availability(&self, available: bool) {
        let topic = self.config.get_availability_topic(&self.device_id);
        let payload = if available { "online" } else { "offline" };

        if let Err(e) = self
            .client
            .publish(
                &topic,
                self.config.to_qos(),
                true, // retain
                payload.to_string(),
            )
            .await
        {
            error!(error = %e, "Failed to publish availability status");
        } else {
            debug!("Published availability: {}", payload);
        }
    }
    pub async fn publish_birth_message(&self) {
        if let Err(e) = self
            .client
            .publish(
                &self.config.birth_topic,
                self.config.to_qos(),
                true,
                self.config.birth_payload.clone(),
            )
            .await
        {
            error!(error = %e, "Failed to publish birth message");
        } else {
            info!(
                "Published birth message to {}: {}",
                self.config.birth_topic, self.config.birth_payload
            );
        }
    }

    pub async fn subscribe_to_hass_status(&self) -> Result<()> {
        self.client
            .subscribe(&self.config.birth_topic, self.config.to_qos())
            .await?;
        info!(
            "Subscribed to Home Assistant status topic: {}",
            self.config.birth_topic
        );
        Ok(())
    }
}
