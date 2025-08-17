use crate::config::Config;
use color_eyre::Result;
use reqwest;
use serde::{Deserialize, Serialize};
use std::any::type_name_of_val;
use std::{any::Any, collections::HashMap};
use tracing::{debug, info};
use tracing_test::traced_test;

const DC_POWER_PATH: &str = "_sum/ProductionDcActualPower";
const PRODUCTION_POWER_PATH: &str = "_sum/ProductionActivePower";
const PRODUCTION_ENERGY_PATH: &str = "_sum/ProductionActiveEnergy";
const GRID_POWER_PATH: &str = "_sum/GridActivePower";
const GRID_BUY_PATH: &str = "_sum/GridBuyActiveEnergy";
const GRID_SELL_PATH: &str = "_sum/GridSellActiveEnergy";
const BATTERY_STATE_PATH: &str = "_sum/EssSoc";
const BATTERY_POWER_PATH: &str = "_sum/EssActivePower";
const BATTERY_LOADING_PATH: &str = "_sum/EssActiveChargeEnergy";
const BATTERY_DISCHARGE_PATH: &str = "_sum/EssActiveDischargeEnergy";
const CONSUMPTION_POWER_PATH: &str = "_sum/ConsumptionActivePower";
const CONSUMPTION_ENERGY_PATH: &str = "_sum/ConsumptionActiveEnergy";

const PATH_ARR: [&str; 12] = [
    DC_POWER_PATH,
    PRODUCTION_POWER_PATH,
    PRODUCTION_ENERGY_PATH,
    GRID_POWER_PATH,
    GRID_BUY_PATH,
    GRID_SELL_PATH,
    BATTERY_STATE_PATH,
    BATTERY_POWER_PATH,
    BATTERY_LOADING_PATH,
    BATTERY_DISCHARGE_PATH,
    CONSUMPTION_POWER_PATH,
    CONSUMPTION_ENERGY_PATH,
];
#[derive(Deserialize)]
struct RawPVMessage {
    address: String,
    #[serde(rename = "type")]
    type_field: String,
    #[serde(rename = "accessMode")]
    access_mode: String,
    text: String,
    unit: String,
    value: i32,
}

#[derive(Default, Debug)]
struct RawPVData {
    dc_power: u16,
    production_power: u16,
    production_energy: i32,
    grid_power: i32,
    grid_buy: i32,
    grid_sell: i32,
    battery_state: u8,
    battery_loading: i32,
    battery_discharge: i32,
    battery_power: i32,
    consumption_power: u16,
    consumption_energy: i32,
}

impl RawPVData {
    pub async fn fill_raw(base_path: &str) -> Result<Self> {
        let mut raw_pv_data = RawPVData::default();
        for path in PATH_ARR {
            let url = format!("{:0}/{:1}", base_path, path);
            if let Ok(response) = send_request(url.as_str()).await {
                match response.address.as_str() {
                    DC_POWER_PATH => raw_pv_data.dc_power = response.value as u16,
                    PRODUCTION_POWER_PATH => raw_pv_data.production_power = response.value as u16,
                    PRODUCTION_ENERGY_PATH => raw_pv_data.production_energy = response.value,
                    GRID_POWER_PATH => raw_pv_data.grid_power = response.value,
                    GRID_BUY_PATH => raw_pv_data.grid_buy = response.value,
                    GRID_SELL_PATH => raw_pv_data.grid_sell = response.value,
                    BATTERY_STATE_PATH => raw_pv_data.battery_state = response.value as u8,
                    BATTERY_POWER_PATH => raw_pv_data.battery_power = response.value,
                    BATTERY_LOADING_PATH => raw_pv_data.battery_loading = response.value,
                    BATTERY_DISCHARGE_PATH => raw_pv_data.battery_discharge = response.value,
                    CONSUMPTION_POWER_PATH => raw_pv_data.consumption_power = response.value as u16,
                    CONSUMPTION_ENERGY_PATH => raw_pv_data.consumption_energy = response.value,
                    _ => panic!(
                        "Received invalid Data that somehow got parsed: {}",
                        response.address
                    ),
                }
            }
        }
        Ok(raw_pv_data)
    }
}

pub async fn send_request(path: &str) -> Result<RawPVMessage> {
    let response = reqwest::get(path).await?.text().await?;

    let response = serde_json::from_str(&response)?;

    Ok(response)
}

#[traced_test]
#[tokio::test]
async fn single_request() {
    let config = Config::new();

    let url = format!("{:0}/{:1}", config.pv_baseaddress, CONSUMPTION_POWER_PATH);

    info!("Combined URL:{}", url);

    let response = send_request(&url).await.unwrap();
    info!("Received: {}", response.value);
    let value_type = type_name_of_val(&response.value);

    assert!(value_type.contains("i32"));
}

#[traced_test]
#[tokio::test]
async fn fill_test() {
    let config: Config = Config::new();

    let raw_data = RawPVData::fill_raw(config.pv_baseaddress.as_str())
        .await
        .unwrap();

    info!("The complete pv data: {:?}", raw_data);

    assert_ne!(0, raw_data.consumption_power);
}
