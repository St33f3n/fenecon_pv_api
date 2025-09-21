use crate::collector::RawPVData;
use crate::config;
use serde_json::json;
use std::cmp::Ordering;
use std::fmt;
use tracing::warn;

#[derive(Debug, Clone, Default)]
pub struct ProcessedData {
    pub supply_state: SupplyState,
    pub battery_status: BatteryStatus,
    pub full_production: u16,
    pub consumption: u16,
}
#[derive(Debug, Clone)]
pub struct DataHistory {
    pub grid_buy: u64,
    pub grid_sell: u64,
    pub production_energy: u64,
    pub consumption_energy: u64,
    pub battery_loaded: u64,
    pub battery_discharge: u64,
    pub battery_cycles: u16,
}
#[derive(Debug, Default, Clone)]
pub struct BatteryStatus {
    pub battery_state: BatteryState,
    pub battery_percent: u8,
    pub battery_energy: f32,
}
#[derive(Debug, Clone, Default)]
pub enum BatteryState {
    Loading(u32),
    Discharging(u32),
    Full,
    #[default]
    Empty,
}
#[derive(Debug, Default, Clone)]
pub enum SupplyState {
    Surplus(u32),
    Demand(u32),
    #[default]
    Offline,
}

pub trait SensorValue {
    fn power_value(&self) -> i32;
    fn state_string(&self) -> String;
}

impl fmt::Display for BatteryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.state_string())
    }
}

impl fmt::Display for SupplyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.state_string())
    }
}

impl SensorValue for BatteryState {
    /// Batterie Power: negativ = laden, positiv = entladen
    fn power_value(&self) -> i32 {
        match self {
            BatteryState::Loading(power) => -(*power as i32), // negativ = laden
            BatteryState::Discharging(power) => *power as i32, // positiv = entladen
            BatteryState::Full | BatteryState::Empty => 0,
        }
    }

    fn state_string(&self) -> String {
        match self {
            BatteryState::Loading(_) => "charging".to_string(),
            BatteryState::Discharging(_) => "discharging".to_string(),
            BatteryState::Full => "full".to_string(),
            BatteryState::Empty => "empty".to_string(),
        }
    }
}

impl SensorValue for SupplyState {
    /// Grid Power: negativ = einspeisung (surplus), positiv = bezug (demand)
    fn power_value(&self) -> i32 {
        match self {
            SupplyState::Surplus(power) => -(*power as i32),
            SupplyState::Demand(power) => *power as i32,
            SupplyState::Offline => 0,
        }
    }

    fn state_string(&self) -> String {
        match self {
            SupplyState::Surplus(_) => "surplus".to_string(),
            SupplyState::Demand(_) => "demand".to_string(),
            SupplyState::Offline => "offline".to_string(),
        }
    }
}

pub trait MqttPayload {
    fn to_state_json(&self) -> serde_json::Value;
}

impl MqttPayload for ProcessedData {
    fn to_state_json(&self) -> serde_json::Value {
        json!({
            "pv_production": self.full_production,
            "supply_power": self.supply_state.power_value(),
            "battery_power": self.battery_status.battery_state.power_value(),
            "consumption": self.consumption,
            "battery_state": self.battery_status.battery_state.state_string(),
            "supply_state": self.supply_state.state_string(),
            "timestamp": chrono::Utc::now().to_rfc3339()
        })
    }
}

impl MqttPayload for DataHistory {
    fn to_state_json(&self) -> serde_json::Value {
        json!({
            "grid_buy": self.grid_buy as f64 / 1000.0,
            "grid_sell": self.grid_sell as f64 / 1000.0,
            "production_energy": self.production_energy as f64 / 1000.0,
            "consumption_energy": self.consumption_energy as f64 / 1000.0,
            "battery_loaded": self.battery_loaded as f64 / 1000.0,
            "battery_discharge": self.battery_discharge as f64 / 1000.0,
            "battery_cycles": self.battery_cycles,
            "timestamp": chrono::Utc::now().to_rfc3339()
        })
    }
}

impl ProcessedData {
    pub fn process_raw(raw_data: RawPVData, config: &config::BatteryConfig) -> Self {
        let grid_power = raw_data.power_data.grid_power;
        let battery_power = raw_data.power_data.battery_power;
        let battery_percent = raw_data.power_data.battery_state;
        let battery_threshold: u8 = config.empty_threshold;
        let max_battery_cap = config.max_battery_energy;

        let supply_state = match grid_power.cmp(&0) {
            Ordering::Less => SupplyState::Surplus(grid_power.abs().try_into().unwrap()),
            Ordering::Greater => SupplyState::Demand(grid_power as u32),
            Ordering::Equal => SupplyState::Offline,
        };

        let battery_state = match battery_power {
            100.. => BatteryState::Discharging(battery_power.try_into().unwrap()),
            ..-100 => BatteryState::Loading(battery_power.abs().try_into().unwrap()),
            -100..100 => {
                if battery_percent <= battery_threshold {
                    BatteryState::Empty
                } else {
                    BatteryState::Full
                }
            }
        };

        let percent = battery_percent as f32 / 100.0;

        //debug!("The battery is charged to {percent}");

        let battery_energy: f32 = max_battery_cap as f32 * percent;

        let battery_status = BatteryStatus {
            battery_state,
            battery_percent,
            battery_energy,
        };

        ProcessedData {
            supply_state,
            battery_status,
            full_production: raw_data.power_data.production_power,
            consumption: raw_data.power_data.consumption_power,
        }
    }
}

impl DataHistory {
    pub fn process_raw(raw_data: RawPVData, config: &config::BatteryConfig) -> Self {
        let battery_cycles = (raw_data.energy_data.battery_discharge as f32
            / (config.max_battery_energy as f32 * config.empty_threshold as f32))
            as u16;

        let grid_buy = raw_data.energy_data.grid_buy;
        let grid_sell = raw_data.energy_data.grid_sell;
        let production_energy = raw_data.energy_data.production_energy;
        let consumption_energy = raw_data.energy_data.consumption_energy;
        let battery_loaded = raw_data.energy_data.battery_loading;
        let battery_discharge = raw_data.energy_data.battery_discharge;
        warn!("Batter loaded: {battery_loaded}");
        DataHistory {
            grid_buy,
            grid_sell,
            production_energy,
            consumption_energy,
            battery_loaded,
            battery_discharge,
            battery_cycles,
        }
    }
}
