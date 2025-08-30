use std::env;
#[derive(Default, Debug, Clone)]
pub struct Config {
    pub pv_baseaddress: String,
    pub mqtt_config: MqttConfig,
    pub battery_config: BatteryConfig,
    pub database_config: DatabaseConfig,
    pub sqlite_cache_config: SqliteCacheConfig,
}

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub broker_url: String,
    pub username: String,
    pub password: String,
    pub discovery_prefix: String,
    pub birth_topic: String,
    pub birth_payload: String,
    pub last_will_topic: String,
    pub last_will_payload: String,
    pub client_id_prefix: String,
    pub keep_alive_secs: u64,
    pub qos_level: u8,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            broker_url: "localhost".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            discovery_prefix: "hass".to_string(),
            birth_topic: "hass/status".to_string(),
            birth_payload: "online".to_string(),
            last_will_topic: "hass/status".to_string(),
            last_will_payload: "offline".to_string(),
            client_id_prefix: "solar_monitor".to_string(),
            keep_alive_secs: 60,
            qos_level: 1, // AtLeastOnce
        }
    }
}

impl MqttConfig {
    pub fn new() -> Self {
        let broker_url = env::var("MQTT_URL").unwrap_or("localhost".to_string());
        let username = env::var("MQTT_USER").unwrap_or_default();
        let password = env::var("MQTT_PW").unwrap_or_default();
        let discovery_prefix = env::var("MQTT_DISCOVERY_PREFIX").unwrap_or("hass".to_string());
        let birth_topic = env::var("MQTT_BIRTH_TOPIC").unwrap_or("hass/status".to_string());
        let birth_payload = env::var("MQTT_BIRTH_PAYLOAD").unwrap_or("online".to_string());
        let last_will_topic = env::var("MQTT_LAST_WILL_TOPIC").unwrap_or("hass/status".to_string());
        let last_will_payload = env::var("MQTT_LAST_WILL_PAYLOAD").unwrap_or("offline".to_string());
        let client_id_prefix =
            env::var("MQTT_CLIENT_ID_PREFIX").unwrap_or("solar_monitor".to_string());

        let keep_alive_secs = env::var("MQTT_KEEP_ALIVE_SECS")
            .unwrap_or("60".to_string())
            .parse()
            .unwrap_or(60);

        let qos_level = env::var("MQTT_QOS_LEVEL")
            .unwrap_or("1".to_string())
            .parse()
            .unwrap_or(1);

        Self {
            broker_url,
            username,
            password,
            discovery_prefix,
            birth_topic,
            birth_payload,
            last_will_topic,
            last_will_payload,
            client_id_prefix,
            keep_alive_secs,
            qos_level,
        }
    }

    pub fn get_discovery_topic(&self, component: &str, device_id: &str, object_id: &str) -> String {
        format!(
            "{}/{}/{}/{}/config",
            self.discovery_prefix, component, device_id, object_id
        )
    }

    pub fn get_state_topic(&self, device_id: &str, topic_type: &str) -> String {
        format!("solar/{}/{}", device_id, topic_type)
    }

    pub fn get_availability_topic(&self, device_id: &str) -> String {
        format!("solar/{}/availability", device_id)
    }

    pub fn to_qos(&self) -> rumqttc::QoS {
        match self.qos_level {
            0 => rumqttc::QoS::AtMostOnce,
            1 => rumqttc::QoS::AtLeastOnce,
            2 => rumqttc::QoS::ExactlyOnce,
            _ => rumqttc::QoS::AtLeastOnce,
        }
    }
}

#[derive(Default, Clone, Debug)]
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
        let pv_baseaddress = env::var("PV_BASEADDRESS").unwrap_or_default();
        let mqtt_config = MqttConfig::new();
        let battery_config = BatteryConfig::new();
        let database_config = DatabaseConfig::new();
        let sqlite_cache_config = SqliteCacheConfig::new();

        Config {
            pv_baseaddress,
            mqtt_config,
            battery_config,
            database_config,
            sqlite_cache_config,
        }
    }

    pub fn to_vector(&self) -> Vec<String> {
        vec![
            self.database_config.database_url.clone(),
            self.database_config.database_pw.clone(),
            self.database_config.database_user.clone(),
            self.pv_baseaddress.clone(),
            self.mqtt_config.broker_url.clone(),
            self.mqtt_config.username.clone(),
            self.mqtt_config.password.clone(),
        ]
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub database_url: String,
    pub database_pw: String,
    pub database_user: String,
    pub max_connections: u32,
    pub health_check_timeout_secs: u64,
    pub max_failures_before_degraded: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://user:password@localhost/pv_data".to_string()),
            database_pw: env::var("DATABASE_PW").unwrap_or_default(),
            database_user: env::var("DATABASE_USER").unwrap_or_default(),
            max_connections: 10,
            health_check_timeout_secs: 10,
            max_failures_before_degraded: 3,
        }
    }
}

impl DatabaseConfig {
    pub fn new() -> Self {
        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
            format!(
                "postgresql://{}:{}@localhost/pv_data",
                env::var("DATABASE_USER").unwrap_or_else(|_| "postgres".to_string()),
                env::var("DATABASE_PW").unwrap_or_else(|_| "password".to_string())
            )
        });

        Self {
            database_url,
            database_pw: env::var("DATABASE_PW").unwrap_or_default(),
            database_user: env::var("DATABASE_USER").unwrap_or_default(),
            max_connections: env::var("DB_MAX_CONNECTIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
            health_check_timeout_secs: env::var("DB_HEALTH_CHECK_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
            max_failures_before_degraded: env::var("DB_MAX_FAILURES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqliteCacheConfig {
    pub cache_db_path: String,
    pub archive_db_path: String,
    pub sync_batch_size: i64,
    pub max_cache_size_mb: u64,
    pub cleanup_threshold_days: i64,
}

impl Default for SqliteCacheConfig {
    fn default() -> Self {
        Self {
            cache_db_path: env::var("SQLITE_CACHE_PATH")
                .unwrap_or_else(|_| "data/cache.db".to_string()),
            archive_db_path: env::var("SQLITE_ARCHIVE_PATH")
                .unwrap_or_else(|_| "data/archive.db".to_string()),
            sync_batch_size: env::var("CACHE_SYNC_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            max_cache_size_mb: env::var("MAX_CACHE_SIZE_MB")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
            cleanup_threshold_days: env::var("CACHE_CLEANUP_DAYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(7),
        }
    }
}

impl SqliteCacheConfig {
    pub fn new() -> Self {
        Self::default()
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

#[test]
fn test_mqtt_config() {
    let config = Config::new();

    // Test default values
    assert_eq!(config.mqtt_config.discovery_prefix, "hass");
    assert_eq!(config.mqtt_config.birth_topic, "hass/status");
    assert_eq!(config.mqtt_config.birth_payload, "online");
    assert_eq!(config.mqtt_config.last_will_topic, "hass/status");
    assert_eq!(config.mqtt_config.last_will_payload, "offline");

    // Test topic generation
    let discovery_topic =
        config
            .mqtt_config
            .get_discovery_topic("sensor", "solar_001", "pv_production");
    assert_eq!(
        discovery_topic,
        "hass/sensor/solar_001/pv_production/config"
    );

    let state_topic = config.mqtt_config.get_state_topic("solar_001", "power");
    assert_eq!(state_topic, "solar/solar_001/power");

    let availability_topic = config.mqtt_config.get_availability_topic("solar_001");
    assert_eq!(availability_topic, "solar/solar_001/availability");

    println!("✅ MQTT Config test passed");
}
