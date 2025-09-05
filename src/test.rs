use std::time::Duration;

use crate::config;

use super::calculator::{
    BatteryState, BatteryStatus, DataHistory, MqttPayload, ProcessedData, SupplyState,
};
use super::collector::CONSUMPTION_POWER_PATH;
use super::collector::{RawEnergyData, RawPVData, RawPVMessage, send_request};
use super::config::{BatteryConfig, Config};
use super::db::{PostgresDatabase, PvEnergyRecord, PvPowerRecord, SqliteCache};
use super::mqtt::*;
use serde_json::Value;
use tracing::{debug, info};
use tracing_test::traced_test;

// Bestehende Tests...
#[traced_test]
#[tokio::test]
async fn process_data() {
    let config = Config::new();
    let raw = RawPVData::fill_raw(&config.pv_baseaddress).await.unwrap();
    let processed = ProcessedData::process_raw(raw.clone(), &config.battery_config);
    let history = DataHistory::process_raw(raw, &config.battery_config);
    debug!("HistoryData is: {:?}", history);
    debug!("Processed Data is: {:?}", processed);
}

#[traced_test]
#[tokio::test]
async fn single_request() {
    let config = Config::new();
    let url = format!("{:0}/{:1}", config.pv_baseaddress, CONSUMPTION_POWER_PATH);
    info!("Combined URL:{}", url);
    let response = send_request(&url).await.unwrap();
    info!("Received: {}", response.value);
    assert_ne!(0, response.value);
}

#[traced_test]
#[tokio::test]
async fn fill_test() {
    let config: Config = Config::new();
    let raw_data = RawPVData::fill_raw(config.pv_baseaddress.as_str())
        .await
        .unwrap();
    info!("The complete pv data: {:?}", raw_data);
    assert_ne!(0, raw_data.power_data.consumption_power);
}

#[traced_test]
#[test]
fn test_processed_data_to_state_json() {
    // Test Daten erstellen
    let processed_data = ProcessedData {
        supply_state: SupplyState::Surplus(800),
        battery_status: BatteryStatus {
            battery_state: BatteryState::Loading(600),
            battery_percent: 75,
            battery_energy: 6.5,
        },
        full_production: 2500,
        consumption: 1100,
    };

    // JSON generieren
    let json = processed_data.to_state_json();
    info!("Generated JSON: {}", json);

    // JSON Struktur prüfen
    assert!(json.is_object(), "JSON sollte ein Object sein");

    // Alle erwarteten Felder prüfen (angepasst an echte Implementierung)
    let expected_fields = [
        "pv_production",
        "supply_power", // nicht "grid_power"
        "battery_power",
        "consumption",
        "battery_state",
        "supply_state", // nicht "grid_state"
        "timestamp",
    ];

    for field in expected_fields {
        assert!(json.get(field).is_some(), "Feld '{}' fehlt im JSON", field);
    }

    // Werte validieren
    assert_eq!(
        json["pv_production"], 2500,
        "PV Produktion sollte 2500W sein"
    );
    assert_eq!(json["consumption"], 1100, "Verbrauch sollte 1100W sein");
    assert_eq!(
        json["supply_power"], -800,
        "Supply Power sollte -800W sein (surplus)"
    );
    assert_eq!(
        json["battery_power"], -600,
        "Battery Power sollte -600W sein (loading)"
    );
    assert_eq!(
        json["battery_state"], "charging",
        "Battery State sollte 'charging' sein"
    );
    assert_eq!(
        json["supply_state"], "surplus",
        "Supply State sollte 'surplus' sein"
    );

    // Timestamp Format prüfen (sollte RFC3339 String sein)
    assert!(
        json["timestamp"].is_string(),
        "Timestamp sollte ein String sein"
    );
    let timestamp_str = json["timestamp"].as_str().unwrap();
    assert!(
        timestamp_str.contains("T"),
        "Timestamp sollte RFC3339 Format haben"
    );
    assert!(
        timestamp_str.contains("Z") || timestamp_str.contains("+"),
        "Timestamp sollte Timezone enthalten"
    );

    debug!("✅ ProcessedData JSON Test erfolgreich");
}

#[traced_test]
#[test]
fn test_history_data_to_state_json() {
    // Test History Daten erstellen (realistische Werte in Wh)
    let history_data = DataHistory {
        grid_buy: 12500,           // 12.5 kWh in Wh
        grid_sell: 18750,          // 18.75 kWh in Wh
        production_energy: 25600,  // 25.6 kWh in Wh
        consumption_energy: 19200, // 19.2 kWh in Wh
        battery_loaded: 3200,      // 3.2 kWh in Wh
        battery_discharge: 2950,   // 2.95 kWh in Wh
        battery_cycles: 142,
    };

    // JSON generieren
    let json = history_data.to_state_json();
    info!("Generated History JSON: {}", json);

    // JSON Struktur prüfen
    assert!(json.is_object(), "JSON sollte ein Object sein");

    // Alle erwarteten Felder prüfen
    let expected_fields = [
        "grid_buy",
        "grid_sell",
        "production_energy",
        "consumption_energy",
        "battery_loaded",
        "battery_discharge",
        "battery_cycles",
        "timestamp",
    ];

    for field in expected_fields {
        assert!(json.get(field).is_some(), "Feld '{}' fehlt im JSON", field);
    }

    // Werte validieren (Umrechnung Wh -> kWh prüfen)
    assert_eq!(json["grid_buy"], 12.5, "Grid Buy sollte 12.5 kWh sein");
    assert_eq!(json["grid_sell"], 18.75, "Grid Sell sollte 18.75 kWh sein");
    assert_eq!(
        json["production_energy"], 25.6,
        "Production sollte 25.6 kWh sein"
    );
    assert_eq!(
        json["consumption_energy"], 19.2,
        "Consumption sollte 19.2 kWh sein"
    );
    assert_eq!(
        json["battery_loaded"], 3.2,
        "Battery Loaded sollte 3.2 kWh sein"
    );
    assert_eq!(
        json["battery_discharge"], 2.95,
        "Battery Discharge sollte 2.95 kWh sein"
    );
    assert_eq!(
        json["battery_cycles"], 142,
        "Battery Cycles sollte 142 sein"
    );

    // Timestamp Format prüfen
    assert!(
        json["timestamp"].is_string(),
        "Timestamp sollte ein String sein"
    );

    debug!("✅ DataHistory JSON Test erfolgreich");
}

#[traced_test]
#[test]
fn test_different_battery_states() {
    // Test verschiedene Battery States
    let test_cases = [
        (BatteryState::Loading(500), -500, "charging"),
        (BatteryState::Discharging(300), 300, "discharging"),
        (BatteryState::Full, 0, "full"),
        (BatteryState::Empty, 0, "empty"),
    ];

    for (battery_state, expected_power, expected_state) in test_cases {
        let processed_data = ProcessedData {
            supply_state: SupplyState::Demand(200),
            battery_status: BatteryStatus {
                battery_state: battery_state.clone(),
                battery_percent: 50,
                battery_energy: 5.0,
            },
            full_production: 1000,
            consumption: 800,
        };

        let json = processed_data.to_state_json();

        assert_eq!(
            json["battery_power"], expected_power,
            "Battery Power für {:?} sollte {} sein",
            battery_state, expected_power
        );
        assert_eq!(
            json["battery_state"], expected_state,
            "Battery State für {:?} sollte '{}' sein",
            battery_state, expected_state
        );

        info!("✅ Battery State {:?} korrekt getestet", battery_state);
    }
}

#[traced_test]
#[test]
fn test_different_supply_states() {
    // Test verschiedene Supply States
    let test_cases = [
        (SupplyState::Surplus(1200), -1200, "surplus"),
        (SupplyState::Demand(400), 400, "demand"),
        (SupplyState::Offline, 0, "offline"),
    ];

    for (supply_state, expected_power, expected_state) in test_cases {
        let processed_data = ProcessedData {
            supply_state: supply_state.clone(),
            battery_status: BatteryStatus {
                battery_state: BatteryState::Full,
                battery_percent: 100,
                battery_energy: 10.0,
            },
            full_production: 2000,
            consumption: 800,
        };

        let json = processed_data.to_state_json();

        assert_eq!(
            json["supply_power"], expected_power,
            "Supply Power für {:?} sollte {} sein",
            supply_state, expected_power
        );
        assert_eq!(
            json["supply_state"], expected_state,
            "Supply State für {:?} sollte '{}' sein",
            supply_state, expected_state
        );

        info!("✅ Supply State {:?} korrekt getestet", supply_state);
    }
}

#[traced_test]
#[tokio::test]
async fn test_real_data_json_generation() {
    // Test mit echten Daten aus dem System
    let config = Config::new();

    // Echte Daten abrufen
    let raw = RawPVData::fill_raw(&config.pv_baseaddress).await.unwrap();
    let processed = ProcessedData::process_raw(raw.clone(), &config.battery_config);
    let history = DataHistory::process_raw(raw, &config.battery_config);

    // JSON generieren
    let processed_json = processed.to_state_json();
    let history_json = history.to_state_json();

    info!("Real ProcessedData JSON: {}", processed_json);
    info!("Real DataHistory JSON: {}", history_json);

    // Grundlegende Validierung
    assert!(
        processed_json.is_object(),
        "Real ProcessedData JSON sollte valid sein"
    );
    assert!(
        history_json.is_object(),
        "Real DataHistory JSON sollte valid sein"
    );

    // Prüfen dass Werte numerisch und nicht null sind
    assert!(
        processed_json["pv_production"].is_number(),
        "PV Production sollte numerisch sein"
    );
    assert!(
        processed_json["consumption"].is_number(),
        "Consumption sollte numerisch sein"
    );
    assert!(
        processed_json["battery_state"].is_string(),
        "Battery State sollte String sein"
    );
    assert!(
        processed_json["supply_state"].is_string(),
        "Supply State sollte String sein"
    );

    debug!("✅ Real Data JSON Test erfolgreich");
}

#[traced_test]
#[tokio::test(flavor = "multi_thread")]
async fn test_mqtt() {
    let config = Config::new();
    let mqtt_url = config.mqtt_config.broker_url.clone();
    let mqtt_user = config.mqtt_config.username.clone();
    let mqtt_pw = config.mqtt_config.password.clone();
    let device_id = "test_id".to_string();
    let mqtt = SolarMqttClient::new(&config.mqtt_config, device_id)
        .await
        .unwrap();
    let mut status = mqtt.get_health_status().await;
    for i in 1..5 {
        let res = mqtt
            .client
            .publish(
                "test",
                rumqttc::QoS::AtLeastOnce,
                false,
                "Test-Msg".to_string(),
            )
            .await;

        status = mqtt.get_health_status().await;

        debug!("Healthstatus is : {:?}", status);
        std::thread::sleep(Duration::from_millis(1000));
    }
    assert_eq!(status, MQTTHealthStatus::Healthy);
}
#[traced_test]
#[tokio::test(flavor = "multi_thread")]
async fn test_discovery_mqtt() {
    let config = Config::new();
    let mqtt_url = config.mqtt_config.broker_url.clone();
    let mqtt_user = config.mqtt_config.username.clone();
    let mqtt_pw = config.mqtt_config.password.clone();
    let device_id = "test_id".to_string();
    let mqtt = SolarMqttClient::new(&config.mqtt_config, device_id)
        .await
        .unwrap();

    std::thread::sleep(Duration::from_secs(2));

    mqtt.setup_discovery().await;

    std::thread::sleep(Duration::from_secs(5));

    let status = mqtt.get_health_status().await;

    assert_eq!(status, MQTTHealthStatus::Healthy);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filled_mqtt() {
    let config = Config::new();
    let mqtt_url = config.mqtt_config.broker_url.clone();
    let mqtt_user = config.mqtt_config.username.clone();
    let mqtt_pw = config.mqtt_config.password.clone();
    let device_id = "test_id".to_string();
    let mqtt = SolarMqttClient::new(&config.mqtt_config, device_id)
        .await
        .unwrap();

    let raw = RawPVData::fill_raw(config.pv_baseaddress.as_str())
        .await
        .unwrap();

    let calc = ProcessedData::process_raw(raw.clone(), &config.battery_config);
    let history = DataHistory::process_raw(raw, &config.battery_config);

    std::thread::sleep(Duration::from_secs(2));

    mqtt.publish_availability(true).await;

    mqtt.publish_current_data(&calc).await;
    mqtt.publish_history_data(&history).await;
    mqtt.publish_state_data(&calc).await;

    std::thread::sleep(Duration::from_secs(5));

    let status = mqtt.get_health_status().await;

    assert_eq!(status, MQTTHealthStatus::Healthy);
}
#[traced_test]
#[tokio::test(flavor = "multi_thread")]
async fn test_db_filled_() {
    let config = Config::new();
    let raw = RawPVData::fill_raw(config.pv_baseaddress.as_str())
        .await
        .unwrap();

    let calc = ProcessedData::process_raw(raw.clone(), &config.battery_config);
    let history = DataHistory::process_raw(raw, &config.battery_config);

    let pgdb = PostgresDatabase::new(config.database_config.clone())
        .await
        .unwrap();

    let sqldb = SqliteCache::new(config::SqliteCacheConfig::new())
        .await
        .unwrap();

    sqldb.store_power_data(&calc).await.unwrap();
    sqldb.store_energy_data(&history).await.unwrap();

    sqldb.sync_to_postgres(&pgdb).await.unwrap();

    sqldb.archive_complete_cache().await.unwrap();
}
