use crate::calculator::{DataHistory, ProcessedData, SensorValue};
use crate::config::{DatabaseConfig, SqliteCacheConfig};
use chrono::{DateTime, Utc};
use color_eyre::eyre::{Result, WrapErr, eyre};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqlitePoolOptions, SqliteRow};
use sqlx::{
    Decode, Encode, PgPool, Row, Sqlite, SqlitePool, Transaction, Type, postgres::PgTypeInfo,
    sqlite::SqliteTypeInfo,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{debug, error, field, info, instrument, warn};

// =============================================================================
// UNIFIED DATA TYPES - Used by both PostgreSQL and SQLite
// =============================================================================

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct PvPowerRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[sqlx(try_from = "String", rename = "created_at")]
    pub timestamp: UtcDateTime,
    pub pv_production: i32,
    pub supply_power: i32,
    pub battery_power: i32,
    pub consumption: i32,
    pub battery_state: String,
    pub supply_state: String,
    pub battery_percent: i32,
    pub battery_energy_wh: i32,
    #[sqlx(try_from = "String", rename = "created_at")]
    pub created_at: UtcDateTime,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct PvEnergyRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[sqlx(try_from = "String", rename = "created_at")]
    pub timestamp: UtcDateTime,
    pub grid_buy_wh: u64,
    pub grid_sell_wh: u64,
    pub production_energy_wh: u64,
    pub consumption_energy_wh: u64,
    pub battery_loaded_wh: u64,
    pub battery_discharge_wh: u64,
    pub battery_cycles: u32,
    #[sqlx(try_from = "String", rename = "created_at")]
    pub created_at: UtcDateTime,
}
#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
pub struct UtcDateTime(pub DateTime<Utc>);

impl UtcDateTime {
    fn as_chrono(&self) -> DateTime<Utc> {
        self.0
    }
}

impl TryFrom<String> for UtcDateTime {
    type Error = chrono::ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        DateTime::parse_from_rfc3339(&value).map(|dt| UtcDateTime(dt.with_timezone(&Utc)))
    }
}

impl Type<sqlx::Sqlite> for UtcDateTime {
    fn type_info() -> SqliteTypeInfo {
        <String as Type<sqlx::Sqlite>>::type_info()
    }
}

impl Type<sqlx::Postgres> for UtcDateTime {
    fn type_info() -> PgTypeInfo {
        <DateTime<Utc> as Type<sqlx::Postgres>>::type_info()
    }
}

impl<'q> Encode<'q, sqlx::Sqlite> for UtcDateTime {
    fn encode_by_ref(
        &self,
        args: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <String as Encode<sqlx::Sqlite>>::encode_by_ref(&self.0.to_rfc3339(), args)
    }
}

impl<'q> Encode<'q, sqlx::Postgres> for UtcDateTime {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <DateTime<Utc> as Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}
impl<'r> Decode<'r, sqlx::Sqlite> for UtcDateTime {
    fn decode(
        value: sqlx::sqlite::SqliteValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let s = <String as Decode<sqlx::Sqlite>>::decode(value)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| UtcDateTime(dt.with_timezone(&Utc)))
            .map_err(Into::into)
    }
}

impl<'r> Decode<'r, sqlx::Postgres> for UtcDateTime {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let dt = <DateTime<Utc> as Decode<sqlx::Postgres>>::decode(value)?;
        Ok(UtcDateTime(dt))
    }
}

// Conversion implementations for proper serialization
impl From<&ProcessedData> for PvPowerRecord {
    fn from(data: &ProcessedData) -> Self {
        let timestamp = UtcDateTime(Utc::now());
        Self {
            id: None,
            timestamp,
            pv_production: data.full_production as i32,
            supply_power: data.supply_state.power_value(),
            battery_power: data.battery_status.battery_state.power_value(),
            consumption: data.consumption as i32,
            battery_state: data.battery_status.battery_state.state_string(),
            supply_state: data.supply_state.state_string(),
            battery_percent: data.battery_status.battery_percent as i32,
            battery_energy_wh: data.battery_status.battery_energy as i32,
            created_at: timestamp,
        }
    }
}

impl From<&DataHistory> for PvEnergyRecord {
    fn from(data: &DataHistory) -> Self {
        let timestamp = UtcDateTime(Utc::now());
        Self {
            id: None,
            timestamp,
            grid_buy_wh: data.grid_buy,
            grid_sell_wh: data.grid_sell,
            production_energy_wh: data.production_energy,
            consumption_energy_wh: data.consumption_energy,
            battery_loaded_wh: data.battery_loaded,
            battery_discharge_wh: data.battery_discharge,
            battery_cycles: data.battery_cycles as u32,
            created_at: timestamp,
        }
    }
}

// =============================================================================
// POSTGRESQL MODULE - Production Database
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum PostgresHealth {
    Healthy,
    Degraded,
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct PostgresState {
    pub health: PostgresHealth,
    pub last_success: Option<DateTime<Utc>>,
    pub last_failure: Option<DateTime<Utc>>,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
}

impl Default for PostgresState {
    fn default() -> Self {
        Self {
            health: PostgresHealth::Disconnected,
            last_success: None,
            last_failure: None,
            consecutive_failures: 0,
            last_error: None,
        }
    }
}

pub struct PostgresDatabase {
    pool: Option<PgPool>,
    state: Arc<Mutex<PostgresState>>,
    config: DatabaseConfig,
}

impl PostgresDatabase {
    pub async fn new(config: DatabaseConfig) -> Result<Self> {
        info!("Initializing PostgreSQL database connection");

        let pool = match Self::create_pool(&config).await {
            Ok(pool) => {
                Self::init_schema(&pool)
                    .await
                    .wrap_err("Failed to initialize PostgreSQL schema")?;
                info!("PostgreSQL connection established");
                Some(pool)
            }
            Err(e) => {
                warn!(error = %e, "PostgreSQL connection failed");
                None
            }
        };

        let initial_health = if pool.is_some() {
            PostgresHealth::Healthy
        } else {
            PostgresHealth::Disconnected
        };

        let mut state = PostgresState::default();
        state.health = initial_health;

        Ok(Self {
            pool,
            state: Arc::new(Mutex::new(state)),
            config,
        })
    }

    async fn create_pool(config: &DatabaseConfig) -> Result<PgPool> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .min_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&config.database_url)
            .await?;

        debug!("PostgreSQL pool created");
        Ok(pool)
    }

    async fn init_schema(pool: &PgPool) -> Result<()> {
        sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS pv_power_data (
            id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL UNIQUE,
            pv_production INTEGER NOT NULL,
            supply_power INTEGER NOT NULL,
            battery_power INTEGER NOT NULL,
            consumption INTEGER NOT NULL,
            battery_state VARCHAR(20) NOT NULL,
            supply_state VARCHAR(20) NOT NULL,
            battery_percent INTEGER NOT NULL CHECK (battery_percent >= 0 AND battery_percent <= 100),
            battery_energy_wh INTEGER NOT NULL CHECK (battery_energy_wh >= 0),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_pv_power_timestamp ON pv_power_data(timestamp DESC)",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS pv_energy_data (
            id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL UNIQUE,
            grid_buy_wh BIGINT NOT NULL CHECK (grid_buy_wh >= 0),
            grid_sell_wh BIGINT NOT NULL CHECK (grid_sell_wh >= 0),
            production_energy_wh BIGINT NOT NULL CHECK (production_energy_wh >= 0),
            consumption_energy_wh BIGINT NOT NULL CHECK (consumption_energy_wh >= 0),
            battery_loaded_wh BIGINT NOT NULL CHECK (battery_loaded_wh >= 0),
            battery_discharge_wh BIGINT NOT NULL CHECK (battery_discharge_wh >= 0),
            battery_cycles INTEGER NOT NULL CHECK (battery_cycles >= 0),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_pv_energy_timestamp ON pv_energy_data(timestamp DESC)",
        )
        .execute(pool)
        .await?;

        info!("PostgreSQL schema initialized");
        Ok(())
    }
    pub async fn store_power_data(&self, data: &ProcessedData) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| eyre!("PostgreSQL not connected"))?;

        let record = PvPowerRecord::from(data);
        let processing_start = Instant::now();

        match sqlx::query!(
            r#"
            INSERT INTO pv_power_data (
                timestamp, pv_production, supply_power, battery_power, consumption,
                battery_state, supply_state, battery_percent, battery_energy_wh
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (timestamp) DO UPDATE SET
                pv_production = EXCLUDED.pv_production,
                supply_power = EXCLUDED.supply_power,
                battery_power = EXCLUDED.battery_power,
                consumption = EXCLUDED.consumption,
                battery_state = EXCLUDED.battery_state,
                supply_state = EXCLUDED.supply_state,
                battery_percent = EXCLUDED.battery_percent,
                battery_energy_wh = EXCLUDED.battery_energy_wh
            "#,
            record.timestamp.as_chrono(),
            record.pv_production,
            record.supply_power,
            record.battery_power,
            record.consumption,
            record.battery_state,
            record.supply_state,
            record.battery_percent,
            record.battery_energy_wh
        )
        .execute(pool)
        .await
        {
            Ok(_) => {
                self.update_success().await;
                let duration = processing_start.elapsed();
                debug!(
                    processing_time_ms = duration.as_millis(),
                    "Power data stored in PostgreSQL"
                );
                Ok(())
            }
            Err(e) => {
                self.update_failure(&e.to_string()).await;
                Err(e.into())
            }
        }
    }

    pub async fn store_energy_data(&self, data: &DataHistory) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| eyre!("PostgreSQL not connected"))?;

        let record = PvEnergyRecord::from(data);
        let processing_start = Instant::now();

        match sqlx::query!(
            r#"
            INSERT INTO pv_energy_data (
                timestamp, grid_buy_wh, grid_sell_wh, production_energy_wh, 
                consumption_energy_wh, battery_loaded_wh, battery_discharge_wh, battery_cycles
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (timestamp) DO UPDATE SET
                grid_buy_wh = EXCLUDED.grid_buy_wh,
                grid_sell_wh = EXCLUDED.grid_sell_wh,
                production_energy_wh = EXCLUDED.production_energy_wh,
                consumption_energy_wh = EXCLUDED.consumption_energy_wh,
                battery_loaded_wh = EXCLUDED.battery_loaded_wh,
                battery_discharge_wh = EXCLUDED.battery_discharge_wh,
                battery_cycles = EXCLUDED.battery_cycles
            "#,
            record.timestamp.as_chrono(),
            record.grid_buy_wh as i64,
            record.grid_sell_wh as i64,
            record.production_energy_wh as i64,
            record.consumption_energy_wh as i64,
            record.battery_loaded_wh as i64,
            record.battery_discharge_wh as i64,
            record.battery_cycles as i32
        )
        .execute(pool)
        .await
        {
            Ok(_) => {
                self.update_success().await;
                let duration = processing_start.elapsed();
                debug!(
                    processing_time_ms = duration.as_millis(),
                    "Energy data stored in PostgreSQL"
                );
                Ok(())
            }
            Err(e) => {
                self.update_failure(&e.to_string()).await;
                Err(e.into())
            }
        }
    }

    pub async fn health_check(&self) -> Result<PostgresHealth> {
        let pool = match &self.pool {
            Some(pool) => pool,
            None => {
                let health = PostgresHealth::Disconnected;
                self.set_health(health.clone()).await;
                return Ok(health);
            }
        };

        match sqlx::query("SELECT 1").fetch_one(pool).await {
            Ok(_) => {
                self.update_success().await;
                Ok(PostgresHealth::Healthy)
            }
            Err(e) => {
                self.update_failure(&e.to_string()).await;
                let current_health = self.get_health().await;
                Ok(current_health)
            }
        }
    }

    pub async fn get_health(&self) -> PostgresHealth {
        let state = self.state.lock().await;
        state.health.clone()
    }

    pub async fn get_state(&self) -> PostgresState {
        let state = self.state.lock().await;
        state.clone()
    }

    async fn update_success(&self) {
        let mut state = self.state.lock().await;
        state.last_success = Some(Utc::now());
        state.consecutive_failures = 0;
        state.last_error = None;
        state.health = PostgresHealth::Healthy;
    }

    async fn update_failure(&self, error: &str) {
        let mut state = self.state.lock().await;
        state.last_failure = Some(Utc::now());
        state.consecutive_failures += 1;
        state.last_error = Some(error.to_string());

        state.health = if state.consecutive_failures >= 3 {
            PostgresHealth::Disconnected
        } else {
            PostgresHealth::Degraded
        };
    }

    async fn set_health(&self, health: PostgresHealth) {
        let mut state = self.state.lock().await;
        state.health = health;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncResult {
    pub records_synced: u64,
    pub duration_ms: u64,
    pub success: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheStats {
    pub power_records_cached: u64,
    pub energy_records_cached: u64,
    pub power_records_archived: u64,
    pub energy_records_archived: u64,
}

pub struct SqliteCache {
    cache_pool: SqlitePool,
    config: SqliteCacheConfig,
}

impl SqliteCache {
    pub async fn new(config: SqliteCacheConfig) -> Result<Self> {
        info!("Initializing SQLite cache system");

        let cache_pool = Self::create_pool(&config.cache_db_path).await?;

        Self::init_cache_schema(&cache_pool).await?;
        Self::init_archive_schema(&cache_pool).await?;

        info!("SQLite cache system initialized successfully");
        Ok(Self { cache_pool, config })
    }

    async fn create_pool(path: &str) -> Result<SqlitePool> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&format!("sqlite://{}", path))
            .await
            .wrap_err_with(|| format!("Failed to create SQLite pool for {}", path))?;

        debug!(path = %path, "SQLite pool created");
        Ok(pool)
    }

    async fn init_cache_schema(pool: &SqlitePool) -> Result<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS pv_power_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL UNIQUE,
                pv_production INTEGER NOT NULL,
                supply_power INTEGER NOT NULL,
                battery_power INTEGER NOT NULL,
                consumption INTEGER NOT NULL,
                battery_state TEXT NOT NULL,
                supply_state TEXT NOT NULL,
                battery_percent INTEGER NOT NULL CHECK (battery_percent >= 0 AND battery_percent <= 100),
                battery_energy_wh INTEGER NOT NULL CHECK (battery_energy_wh >= 0),
                created_at TEXT DEFAULT (datetime('now', 'utc'))
            );
            
            CREATE INDEX IF NOT EXISTS idx_cache_power_timestamp ON pv_power_cache(timestamp DESC);
            
            CREATE TABLE IF NOT EXISTS pv_energy_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL UNIQUE,
                grid_buy_wh INTEGER NOT NULL CHECK (grid_buy_wh >= 0),
                grid_sell_wh INTEGER NOT NULL CHECK (grid_sell_wh >= 0),
                production_energy_wh INTEGER NOT NULL CHECK (production_energy_wh >= 0),
                consumption_energy_wh INTEGER NOT NULL CHECK (consumption_energy_wh >= 0),
                battery_loaded_wh INTEGER NOT NULL CHECK (battery_loaded_wh >= 0),
                battery_discharge_wh INTEGER NOT NULL CHECK (battery_discharge_wh >= 0),
                battery_cycles INTEGER NOT NULL CHECK (battery_cycles >= 0),
                created_at TEXT DEFAULT (datetime('now', 'utc'))
            );
            
            CREATE INDEX IF NOT EXISTS idx_cache_energy_timestamp ON pv_energy_cache(timestamp DESC);
        "#)
        .execute(pool)
        .await
        .wrap_err("Failed to initialize cache schema")?;

        debug!("Cache schema initialized");
        Ok(())
    }

    async fn init_archive_schema(pool: &SqlitePool) -> Result<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS pv_power_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                pv_production INTEGER NOT NULL,
                supply_power INTEGER NOT NULL,
                battery_power INTEGER NOT NULL,
                consumption INTEGER NOT NULL,
                battery_state TEXT NOT NULL,
                supply_state TEXT NOT NULL,
                battery_percent INTEGER NOT NULL,
                battery_energy_wh INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                archived_at TEXT DEFAULT (datetime('now', 'utc'))
            );
            
            CREATE INDEX IF NOT EXISTS idx_archive_power_timestamp ON pv_power_archive(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_archive_power_archived_at ON pv_power_archive(archived_at DESC);
            
            CREATE TABLE IF NOT EXISTS pv_energy_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                grid_buy_wh INTEGER NOT NULL,
                grid_sell_wh INTEGER NOT NULL,
                production_energy_wh INTEGER NOT NULL,
                consumption_energy_wh INTEGER NOT NULL,
                battery_loaded_wh INTEGER NOT NULL,
                battery_discharge_wh INTEGER NOT NULL,
                battery_cycles INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                archived_at TEXT DEFAULT (datetime('now', 'utc'))
            );
            
            CREATE INDEX IF NOT EXISTS idx_archive_energy_timestamp ON pv_energy_archive(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_archive_energy_archived_at ON pv_energy_archive(archived_at DESC);
        "#)
        .execute(pool)
        .await
        .wrap_err("Failed to initialize archive schema")?;

        debug!("Archive schema initialized");
        Ok(())
    }

    #[instrument(skip(self, data), fields(timestamp = %data.battery_status.battery_percent))]
    pub async fn store_power_data(&self, data: &ProcessedData) -> Result<()> {
        let record = PvPowerRecord::from(data);

        let query = r#"
            INSERT OR REPLACE INTO pv_power_cache (
                timestamp, pv_production, supply_power, battery_power, consumption,
                battery_state, supply_state, battery_percent, battery_energy_wh
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#;

        sqlx::query(query)
            .bind(record.timestamp.as_chrono().to_rfc3339())
            .bind(record.pv_production)
            .bind(record.supply_power)
            .bind(record.battery_power)
            .bind(record.consumption)
            .bind(record.battery_state)
            .bind(record.supply_state)
            .bind(record.battery_percent)
            .bind(record.battery_energy_wh)
            .execute(&self.cache_pool)
            .await
            .wrap_err("Failed to store power data in cache")?;

        debug!("Power data stored in cache");
        Ok(())
    }

    #[instrument(skip(self, data), fields(grid_buy = data.grid_buy, grid_sell = data.grid_sell))]
    pub async fn store_energy_data(&self, data: &DataHistory) -> Result<()> {
        let record = PvEnergyRecord::from(data);

        let query = r#"
            INSERT OR REPLACE INTO pv_energy_cache (
                timestamp, grid_buy_wh, grid_sell_wh, production_energy_wh,
                consumption_energy_wh, battery_loaded_wh, battery_discharge_wh, battery_cycles
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#;

        sqlx::query(query)
            .bind(record.timestamp.as_chrono().to_rfc3339())
            .bind(record.grid_buy_wh as i64)
            .bind(record.grid_sell_wh as i64)
            .bind(record.production_energy_wh as i64)
            .bind(record.consumption_energy_wh as i64)
            .bind(record.battery_loaded_wh as i64)
            .bind(record.battery_discharge_wh as i64)
            .bind(record.battery_cycles as i32)
            .execute(&self.cache_pool)
            .await
            .wrap_err("Failed to store energy data in cache")?;

        debug!("Energy data stored in cache");
        Ok(())
    }

    #[instrument(skip(self, postgres_db), fields(sync_batch_size = self.config.sync_batch_size))]
    pub async fn sync_to_postgres(&self, postgres_db: &PostgresDatabase) -> Result<SyncResult> {
        info!("Starting cache synchronization to PostgreSQL");
        let start_time = Instant::now();

        let mut total_synced = 0u64;

        // Sync power data
        let power_synced = self.sync_power_data_batch(postgres_db).await?;
        total_synced += power_synced;

        // Sync energy data
        let energy_synced = self.sync_energy_data_batch(postgres_db).await?;
        total_synced += energy_synced;

        let duration = start_time.elapsed();

        info!(
            synced_records = total_synced,
            duration_ms = duration.as_millis(),
            "Cache synchronization completed"
        );

        Ok(SyncResult {
            records_synced: total_synced,
            duration_ms: duration.as_millis() as u64,
            success: true,
        })
    }

    async fn sync_power_data_batch(&self, postgres_db: &PostgresDatabase) -> Result<u64> {
        let cached_records: Vec<PvPowerRecord> = sqlx::query_as(
            r#"
            SELECT 
                id, timestamp, pv_production, supply_power, battery_power, consumption,
                battery_state, supply_state, battery_percent, battery_energy_wh, 
                timestamp as created_at
            FROM pv_power_cache 
            ORDER BY timestamp ASC 
            LIMIT ?
            "#,
        )
        .bind(self.config.sync_batch_size)
        .fetch_all(&self.cache_pool)
        .await?;
        for record in &cached_records {
            // record ist bereits PvPowerRecord
            self.store_serialized_power_data(postgres_db, record)
                .await?;
        }

        Ok(cached_records.len() as u64)
    }

    async fn sync_energy_data_batch(&self, postgres_db: &PostgresDatabase) -> Result<u64> {
        let cached_records: Vec<PvEnergyRecord> = sqlx::query_as(
            r#"
       SELECT 
           id, timestamp, grid_buy_wh, grid_sell_wh, production_energy_wh, 
           consumption_energy_wh, battery_loaded_wh, battery_discharge_wh, 
           battery_cycles, timestamp as created_at
       FROM pv_energy_cache 
       ORDER BY timestamp ASC 
       LIMIT ?
       "#,
        )
        .bind(self.config.sync_batch_size)
        .fetch_all(&self.cache_pool)
        .await?;
        for record in &cached_records {
            self.store_serialized_energy_data(postgres_db, record)
                .await?;
        }

        Ok(cached_records.len() as u64)
    }

    // Direct serialized storage for sync operations (unchanged)
    async fn store_serialized_power_data(
        &self,
        postgres_db: &PostgresDatabase,
        record: &PvPowerRecord,
    ) -> Result<()> {
        let pool = postgres_db
            .pool
            .as_ref()
            .ok_or_else(|| eyre!("PostgreSQL not connected"))?;

        sqlx::query!(
            r#"
            INSERT INTO pv_power_data (
                timestamp, pv_production, supply_power, battery_power, consumption,
                battery_state, supply_state, battery_percent, battery_energy_wh
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (timestamp) DO NOTHING
            "#,
            record.timestamp.as_chrono(),
            record.pv_production,
            record.supply_power,
            record.battery_power,
            record.consumption,
            record.battery_state,
            record.supply_state,
            record.battery_percent,
            record.battery_energy_wh
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn store_serialized_energy_data(
        &self,
        postgres_db: &PostgresDatabase,
        record: &PvEnergyRecord,
    ) -> Result<()> {
        let pool = postgres_db
            .pool
            .as_ref()
            .ok_or_else(|| eyre!("PostgreSQL not connected"))?;

        sqlx::query!(
            r#"
            INSERT INTO pv_energy_data (
                timestamp, grid_buy_wh, grid_sell_wh, production_energy_wh, 
                consumption_energy_wh, battery_loaded_wh, battery_discharge_wh, battery_cycles
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (timestamp) DO NOTHING
            "#,
            record.timestamp.as_chrono(),
            record.grid_buy_wh as i64,
            record.grid_sell_wh as i64,
            record.production_energy_wh as i64,
            record.consumption_energy_wh as i64,
            record.battery_loaded_wh as i64,
            record.battery_discharge_wh as i64,
            record.battery_cycles as i32
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    // Archiviert alle Power Records aus dem Cache und leert den Cache
    pub async fn archive_all_power_records(&self) -> Result<u64> {
        debug!("Starting power records archive operation");

        let mut cache_tx = self.cache_pool.begin().await?;

        // Get count before archiving
        let record_count: i64 = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pv_power_cache")
            .fetch_one(&self.cache_pool)
            .await?;

        if record_count == 0 {
            debug!("No power records to archive");
            return Ok(0);
        }

        // Archive all records from cache
        let archived_rows = sqlx::query(
            r#"
        INSERT INTO pv_power_archive 
        SELECT *, datetime('now', 'utc') as archived_at 
        FROM pv_power_cache
        "#,
        )
        .execute(&mut *cache_tx)
        .await?
        .rows_affected();

        // Clear the cache completely
        sqlx::query("DELETE FROM pv_power_cache")
            .execute(&mut *cache_tx)
            .await?;

        cache_tx.commit().await?;

        debug!(
            archived_count = archived_rows,
            "Power cache archived and cleared"
        );
        Ok(archived_rows)
    }

    // Archiviert alle Energy Records aus dem Cache und leert den Cache
    pub async fn archive_all_energy_records(&self) -> Result<u64> {
        debug!("Starting energy records archive operation");

        let mut cache_tx = self.cache_pool.begin().await?;

        // Get count before archiving
        let record_count: i64 =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pv_energy_cache")
                .fetch_one(&self.cache_pool)
                .await?;

        if record_count == 0 {
            debug!("No energy records to archive");
            return Ok(0);
        }

        // Archive all records from cache
        let archived_rows = sqlx::query(
            r#"
        INSERT INTO pv_energy_archive 
        SELECT *, datetime('now', 'utc') as archived_at 
        FROM pv_energy_cache
        "#,
        )
        .execute(&mut *cache_tx)
        .await?
        .rows_affected();

        // Clear the cache completely
        sqlx::query("DELETE FROM pv_energy_cache")
            .execute(&mut *cache_tx)
            .await?;

        cache_tx.commit().await?;

        debug!(
            archived_count = archived_rows,
            "Energy cache archived and cleared"
        );
        Ok(archived_rows)
    }

    // Combined function um beide Caches auf einmal zu archivieren
    pub async fn archive_complete_cache(&self) -> Result<(u64, u64)> {
        debug!("Starting complete cache archive operation");

        let power_archived = self.archive_all_power_records().await?;
        let energy_archived = self.archive_all_energy_records().await?;

        debug!(
            power_archived = power_archived,
            energy_archived = energy_archived,
            total_archived = power_archived + energy_archived,
            "Complete cache archive finished"
        );

        Ok((power_archived, energy_archived))
    }

    #[instrument(skip(self))]
    pub async fn get_cache_stats(&self) -> Result<CacheStats> {
        // Count power cache records
        let power_cached = sqlx::query("SELECT COUNT(*) as count FROM pv_power_cache")
            .fetch_one(&self.cache_pool)
            .await
            .wrap_err("Failed to count power cache records")?
            .try_get::<i64, _>("count")
            .wrap_err("Failed to get power cache count")? as u64;

        // Count energy cache records
        let energy_cached = sqlx::query("SELECT COUNT(*) as count FROM pv_energy_cache")
            .fetch_one(&self.cache_pool)
            .await
            .wrap_err("Failed to count energy cache records")?
            .try_get::<i64, _>("count")
            .wrap_err("Failed to get energy cache count")? as u64;

        // Count power archive records
        let power_archived = sqlx::query("SELECT COUNT(*) as count FROM pv_power_archive")
            .fetch_one(&self.cache_pool)
            .await
            .wrap_err("Failed to count power archive records")?
            .try_get::<i64, _>("count")
            .wrap_err("Failed to get power archive count")? as u64;

        // Count energy archive records
        let energy_archived = sqlx::query("SELECT COUNT(*) as count FROM pv_energy_archive")
            .fetch_one(&self.cache_pool)
            .await
            .wrap_err("Failed to count energy archive records")?
            .try_get::<i64, _>("count")
            .wrap_err("Failed to get energy archive count")? as u64;

        Ok(CacheStats {
            power_records_cached: power_cached,
            energy_records_cached: energy_cached,
            power_records_archived: power_archived,
            energy_records_archived: energy_archived,
        })
    }
}
#[tokio::test]
async fn test_record_conversion() {
    let mut processed_data = ProcessedData::default();
    processed_data.full_production = 2500;
    processed_data.consumption = 1100;
    processed_data.battery_status.battery_percent = 75;
    processed_data.battery_status.battery_energy = 6500.0;

    let power_record = PvPowerRecord::from(&processed_data);

    assert_eq!(power_record.pv_production, 2500);
    assert_eq!(power_record.consumption, 1100);
    assert_eq!(power_record.battery_percent, 75);
    assert_eq!(power_record.battery_energy_wh, 6500); // 6.5kWh = 6500Wh
}

#[tokio::test]
async fn test_sqlite_cache_creation() {
    let config = SqliteCacheConfig {
        max_cache_size_mb: 100,
        cache_db_path: "data/test_power_cache.db".to_string(),
        sync_batch_size: 100,
        cleanup_threshold_days: 150,
    };

    let cache = SqliteCache::new(config).await;
    assert!(cache.is_ok());
}

#[tokio::test]
async fn test_cache_power_data_storage() {
    let config = SqliteCacheConfig {
        max_cache_size_mb: 100,
        cache_db_path: "data/test_power_cache.db".to_string(),
        sync_batch_size: 100,
        cleanup_threshold_days: 150,
    };

    let cache = SqliteCache::new(config).await.unwrap();

    let mut processed_data = ProcessedData::default();
    processed_data.full_production = 2500;
    processed_data.consumption = 1100;
    processed_data.battery_status.battery_percent = 75;
    processed_data.battery_status.battery_energy = 6500.0;

    let result = cache.store_power_data(&processed_data).await;
    assert!(result.is_ok());

    let stats = cache.get_cache_stats().await.unwrap();
    assert_eq!(stats.power_records_cached, 1);
}
