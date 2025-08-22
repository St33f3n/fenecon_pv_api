use std::env;

pub struct Config {
    pub database_url: String,
    pub database_pw: String,
    pub database_user: String,
    pub pv_baseaddress: String,
    pub mqtt_url: String,
    pub mqtt_user: String,
    pub mqtt_pw: String,
    pub battery_config: BatteryConfig,
}

#[derive(Default, Debug)]
pub struct BatteryConfig {
    pub max_battery_energy: u16,
    pub empty_threshold: u8,
}
impl BatteryConfig {
    pub fn new() -> Self {
        let max_battery_energy_str = env::var("MAX_BATTERY_ENERGY").unwrap_or("10000".to_string());
        let empty_threshold_str = env::var("EMPTY_THRESHOLD").unwrap_or("10".to_string());

        let max_battery_energy: u16 = max_battery_energy_str.parse().unwrap();
        let empty_threshold: u8 = empty_threshold_str.parse().unwrap();

        BatteryConfig {
            max_battery_energy,
            empty_threshold,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        let database_url = env::var("DATABASE_URL").unwrap_or_default();
        let database_pw = env::var("DATABASE_PW").unwrap_or_default();
        let database_user = env::var("DATABASE_USER").unwrap_or_default();
        let pv_baseaddress = env::var("PV_BASEADDRESS").unwrap_or_default();
        let mqtt_url = env::var("MQTT_URL").unwrap_or_default();
        let mqtt_user = env::var("MQTT_USER").unwrap_or_default();
        let mqtt_pw = env::var("MQTT_PW").unwrap_or_default();

        let battery_config = BatteryConfig::new();

        Config {
            database_url,
            database_pw,
            database_user,
            pv_baseaddress,
            mqtt_url,
            mqtt_user,
            mqtt_pw,
            battery_config,
        }
    }
    pub fn to_vector(&self) -> Vec<String> {
        vec![
            self.database_url.clone(),
            self.database_pw.clone(),
            self.database_user.clone(),
            self.pv_baseaddress.clone(),
            self.mqtt_url.clone(),
            self.mqtt_user.clone(),
            self.mqtt_pw.clone(),
        ]
    }
}

#[test]

fn test_pw_env() {
    let config = Config::new();
    let config_vec = config.to_vector();
    println!("{:?}", config_vec);

    let test = config_vec.into_iter().filter(|x| x.is_empty()).count();
    assert_eq!(0, test);
}
#[test]
fn test_battery_env() {
    let config = Config::new();
    println!("{:?}", config.battery_config);
    assert!(config.battery_config.max_battery_energy > 10000);
    assert!(config.battery_config.empty_threshold >= 10)
}
