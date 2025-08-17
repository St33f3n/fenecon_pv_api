use std::env;

pub struct Config {
    pub database_url: String,
    pub database_pw: String,
    pub database_user: String,
    pub pv_baseaddress: String,
    pub mqtt_url: String,
    pub mqtt_user: String,
    pub mqtt_pw: String,
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

        Config {
            database_url,
            database_pw,
            database_user,
            pv_baseaddress,
            mqtt_url,
            mqtt_user,
            mqtt_pw,
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

fn test_env() {
    let config = Config::new();
    let config_vec = config.to_vector();
    println!("{:?}", config_vec);

    let test = config_vec.into_iter().filter(|x| x.is_empty()).count();
    assert_eq!(0, test);
}
