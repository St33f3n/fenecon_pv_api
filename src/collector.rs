use color_eyre::Result;
use color_eyre::eyre::eyre;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

const DC_POWER_PATH: &str = "_sum/ProductionDcActualPower";
const PRODUCTION_POWER_PATH: &str = "_sum/ProductionActivePower";
const PRODUCTION_ENERGY_PATH: &str = "_sum/ProductionActiveEnergy";
const GRID_POWER_PATH: &str = "_sum/GridActivePower";
const GRID_BUY_PATH: &str = "_sum/GridBuyActiveEnergy";
const GRID_SELL_PATH: &str = "_sum/GridSellActiveEnergy";
const BATTERY_STATE_PATH: &str = "_sum/EssSoc";
const BATTERY_POWER_PATH: &str = "_sum/EssActivePower";
const BATTERY_LOADING_PATH: &str = "_sum/EssDcChargeEnergy";
const BATTERY_DISCHARGE_PATH: &str = "_sum/EssDcDischargeEnergy";
pub const CONSUMPTION_POWER_PATH: &str = "_sum/ConsumptionActivePower";
const CONSUMPTION_ENERGY_PATH: &str = "_sum/ConsumptionActiveEnergy";

const PATH_POWER_ARR: [&str; 6] = [
    DC_POWER_PATH,
    PRODUCTION_POWER_PATH,
    GRID_POWER_PATH,
    BATTERY_STATE_PATH,
    BATTERY_POWER_PATH,
    CONSUMPTION_POWER_PATH,
];

const PATH_ENERGY_ARR: [&str; 6] = [
    GRID_BUY_PATH,
    GRID_SELL_PATH,
    PRODUCTION_ENERGY_PATH,
    CONSUMPTION_ENERGY_PATH,
    BATTERY_LOADING_PATH,
    BATTERY_DISCHARGE_PATH,
];

#[derive(Deserialize, Clone, Debug)]
pub struct RawPVMessage {
    pub address: String,
    #[serde(rename = "type")]
    pub type_field: String,
    #[serde(rename = "accessMode")]
    pub access_mode: String,
    pub text: String,
    pub unit: String,
    pub value: i64,
}

#[derive(Default, Debug, Clone)]
pub struct RawPVData {
    pub energy_data: RawEnergyData,
    pub power_data: RawPowerData,
}

#[derive(Default, PartialEq, Debug, Clone)]
pub struct RawPowerData {
    pub dc_power: u16,
    pub production_power: u16,
    pub grid_power: i32,
    pub battery_state: u8,
    pub battery_power: i32,
    pub consumption_power: u16,
}
#[derive(Default, Debug, PartialEq, Clone)]
pub struct RawEnergyData {
    pub grid_buy: u64,
    pub grid_sell: u64,
    pub battery_loading: u64,
    pub battery_discharge: u64,
    pub production_energy: u64,
    pub consumption_energy: u64,
}

impl RawPowerData {
    pub async fn get_data(base_path: &str) -> Result<Self> {
        let mut raw_power_data = RawPowerData::default();
        for path in PATH_POWER_ARR {
            let url = format!("{:0}/{:1}", base_path, path);
            match send_request(url.as_str()).await {
                Ok(response) => match response.address.as_str() {
                    DC_POWER_PATH => raw_power_data.dc_power = response.value as u16,
                    PRODUCTION_POWER_PATH => {
                        raw_power_data.production_power = response.value as u16
                    }
                    GRID_POWER_PATH => raw_power_data.grid_power = response.value as i32,
                    BATTERY_STATE_PATH => raw_power_data.battery_state = response.value as u8,
                    BATTERY_POWER_PATH => raw_power_data.battery_power = response.value as i32,
                    CONSUMPTION_POWER_PATH => {
                        raw_power_data.consumption_power = response.value as u16
                    }
                    _ => panic!("Should not be possible"),
                },
                Err(e) => {
                    error!("No working HTTP-Request could be resieved: {e}");
                    return Err(e);
                }
            }
        }
        if raw_power_data == RawPowerData::default() {
            return Err(eyre!(
                "No real data could be generated the http Request seams to be not working correctly"
            ));
        }
        raw_power_data.battery_power -= raw_power_data.dc_power as i32;

        Ok(raw_power_data)
    }
}

impl RawEnergyData {
    pub async fn get_data(base_path: &str) -> Result<Self> {
        let mut raw_energy_data = RawEnergyData::default();
        for path in PATH_ENERGY_ARR {
            let url = format!("{:0}/{:1}", base_path, path);
            match send_request(url.as_str()).await {
                Ok(response) => match response.address.as_str() {
                    PRODUCTION_ENERGY_PATH => {
                        raw_energy_data.production_energy = response.value as u64
                    }
                    GRID_BUY_PATH => raw_energy_data.grid_buy = response.value as u64,
                    GRID_SELL_PATH => raw_energy_data.grid_sell = response.value as u64,
                    BATTERY_LOADING_PATH => raw_energy_data.battery_loading = response.value as u64,
                    BATTERY_DISCHARGE_PATH => {
                        raw_energy_data.battery_discharge = response.value as u64
                    }
                    CONSUMPTION_ENERGY_PATH => {
                        raw_energy_data.consumption_energy = response.value as u64
                    }
                    _ => panic!("Should not be possible"),
                },

                Err(e) => {
                    error!("No working HTTP-Request could be resieved: {e}");
                    return Err(e);
                }
            }
        }
        if raw_energy_data == RawEnergyData::default() {
            return Err(eyre!(
                "No real data could be generated the http Request seams to be not working correctly"
            ));
        }

        Ok(raw_energy_data)
    }
}

impl RawPVData {
    pub async fn fill_raw(base_path: &str) -> Result<Self> {
        let (energy_res, power_res) = tokio::join!(
            RawEnergyData::get_data(base_path),
            RawPowerData::get_data(base_path)
        );

        if energy_res.is_ok() && power_res.is_ok() {
            let raw_pv_data = RawPVData {
                energy_data: energy_res.unwrap(),
                power_data: power_res.unwrap(),
            };
            Ok(raw_pv_data)
        } else {
            Err(eyre!("Request Failed"))
        }
    }
}

pub async fn send_request(path: &str) -> Result<RawPVMessage> {
    let response = reqwest::get(path).await?.text().await?;
    debug!("{response}");
    let response = serde_json::from_str(&response)?;

    Ok(response)
}
