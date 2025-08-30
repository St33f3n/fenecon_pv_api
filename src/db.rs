use crate::calculator::{DataHistory, ProcessedData, SensorValue};
use crate::config::{DatabaseConfig, SqliteCacheConfig};
use chrono::{DateTime, Utc};
use color_eyre::eyre::{Result, WrapErr, eyre};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{PgPool, SqlitePool};
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
    pub timestamp: DateTime<Utc>,
    pub pv_production: i32,
    pub supply_power: i32,
    pub battery_power: i32,
    pub consumption: i32,
    pub battery_state: String,
    pub supply_state: String,
    pub battery_percent: i32,
    pub battery_energy_wh: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct PvEnergyRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    pub timestamp: DateTime<Utc>,
    pub grid_buy_wh: u64,
    pub grid_sell_wh: u64,
    pub production_energy_wh: u64,
    pub consumption_energy_wh: u64,
    pub battery_loaded_wh: u64,
    pub battery_discharge_wh: u64,
    pub battery_cycles: u32,
    pub created_at: DateTime<Utc>,
}

// Conversion implementations for proper serialization
impl From<&ProcessedData> for PvPowerRecord {
    fn from(data: &ProcessedData) -> Self {
        let timestamp = Utc::now();
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
            battery_energy_wh: (data.battery_status.battery_energy * 1000.0) as i32,
            created_at: timestamp,
        }
    }
}

impl From<&DataHistory> for PvEnergyRecord {
    fn from(data: &DataHistory) -> Self {
        let timestamp = Utc::now();
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
            );
            
            CREATE INDEX IF NOT EXISTS idx_pv_power_timestamp ON pv_power_data(timestamp DESC);
            
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
            );
            
            CREATE INDEX IF NOT EXISTS idx_pv_energy_timestamp ON pv_energy_data(timestamp DESC);
        "#)
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
            record.timestamp,
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
            record.timestamp,
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

// =============================================================================
// SQLITE CACHE MODULE - Local Cache & Archive
// =============================================================================

pub struct SqliteCache {
    cache_pool: SqlitePool,
    archive_pool: SqlitePool,
    config: SqliteCacheConfig,
}

impl SqliteCache {
    pub async fn new(config: SqliteCacheConfig) -> Result<Self> {
        info!("Initializing SQLite cache system");

        // Ensure directories exist
        for path in [&config.cache_db_path, &config.archive_db_path] {
            if let Some(parent) = std::path::Path::new(path).parent() {
                std::fs::create_dir_all(parent)
                    .wrap_err_with(|| format!("Failed to create directory for {}", path))?;
            }
        }

        let cache_pool = Self::create_pool(&config.cache_db_path).await?;
        let archive_pool = Self::create_pool(&config.archive_db_path).await?;

        Self::init_cache_schema(&cache_pool).await?;
        Self::init_archive_schema(&archive_pool).await?;

        info!("SQLite cache system initialized");
        Ok(Self {
            cache_pool,
            archive_pool,
            config,
        })
    }

    async fn create_pool(path: &str) -> Result<SqlitePool> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&format!("sqlite://{}", path))
            .await?;

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
        .await?;

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
        .await?;

        debug!("Archive schema initialized");
        Ok(())
    }

    pub async fn store_power_data(&self, data: &ProcessedData) -> Result<()> {
        let record = PvPowerRecord::from(data);

        sqlx::query!(
            r#"
            INSERT OR REPLACE INTO pv_power_cache (
                timestamp, pv_production, supply_power, battery_power, consumption,
                battery_state, supply_state, battery_percent, battery_energy_wh
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            record.timestamp.to_rfc3339(),
            record.pv_production,
            record.supply_power,
            record.battery_power,
            record.consumption,
            record.battery_state,
            record.supply_state,
            record.battery_percent,
            record.battery_energy_wh
        )
        .execute(&self.cache_pool)
        .await?;

        debug!("Power data stored in cache");
        Ok(())
    }

    pub async fn store_energy_data(&self, data: &DataHistory) -> Result<()> {
        let record = PvEnergyRecord::from(data);

        sqlx::query!(
            r#"
            INSERT OR REPLACE INTO pv_energy_cache (
                timestamp, grid_buy_wh, grid_sell_wh, production_energy_wh,
                consumption_energy_wh, battery_loaded_wh, battery_discharge_wh, battery_cycles
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            record.timestamp.to_rfc3339(),
            record.grid_buy_wh as i64,
            record.grid_sell_wh as i64,
            record.production_energy_wh as i64,
            record.consumption_energy_wh as i64,
            record.battery_loaded_wh as i64,
            record.battery_discharge_wh as i64,
            record.battery_cycles as i32
        )
        .execute(&self.cache_pool)
        .await?;

        debug!("Energy data stored in cache");
        Ok(())
    }

    // Sync function to be triggered by state machine
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
        let cached_records = sqlx::query_as!(
            PvPowerRecord,
            r#"
            SELECT 
                id, timestamp, pv_production, supply_power, battery_power, consumption,
                battery_state, supply_state, battery_percent, battery_energy_wh, created_at
            FROM pv_power_cache 
            ORDER BY timestamp ASC 
            LIMIT ?
            "#,
            self.config.sync_batch_size
        )
        .fetch_all(&self.cache_pool)
        .await?;

        if cached_records.is_empty() {
            return Ok(0);
        }

        let mut synced_count = 0u64;
        let mut timestamps_to_archive = Vec::new();

        for record in cached_records {
            // Use serialized record directly with PostgreSQL
            if self
                .store_serialized_power_data(postgres_db, &record)
                .await
                .is_ok()
            {
                synced_count += 1;
                timestamps_to_archive.push(record.timestamp.to_rfc3339());
            }
        }

        // Archive successfully synced records
        if !timestamps_to_archive.is_empty() {
            self.archive_power_records(&timestamps_to_archive).await?;
        }

        Ok(synced_count)
    }

    async fn sync_energy_data_batch(&self, postgres_db: &PostgresDatabase) -> Result<u64> {
        let cached_records = sqlx::query_as!(
            PvEnergyRecord,
            r#"
            SELECT 
                id, timestamp, grid_buy_wh, grid_sell_wh, production_energy_wh, 
                consumption_energy_wh, battery_loaded_wh, battery_discharge_wh, 
                battery_cycles, created_at
            FROM pv_energy_cache 
            ORDER BY timestamp ASC 
            LIMIT ?
            "#,
            self.config.sync_batch_size
        )
        .fetch_all(&self.cache_pool)
        .await?;

        if cached_records.is_empty() {
            return Ok(0);
        }

        let mut synced_count = 0u64;
        let mut timestamps_to_archive = Vec::new();

        for record in cached_records {
            // Use serialized record directly with PostgreSQL
            if self
                .store_serialized_energy_data(postgres_db, &record)
                .await
                .is_ok()
            {
                synced_count += 1;
                timestamps_to_archive.push(record.timestamp.to_rfc3339());
            }
        }

        // Archive successfully synced records
        if !timestamps_to_archive.is_empty() {
            self.archive_energy_records(&timestamps_to_archive).await?;
        }

        Ok(synced_count)
    }

    // Direct serialized storage for sync operations
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
            record.timestamp,
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
            record.timestamp,
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

    // Archive function to be triggered by state machine
    async fn archive_power_records(&self, timestamps: &[String]) -> Result<()> {
        let mut archive_tx = self.archive_pool.begin().await?;
        let mut cache_tx = self.cache_pool.begin().await?;

        for timestamp in timestamps {
            // Move record from cache to archive
            sqlx::query!(
                r#"
                INSERT INTO pv_power_archive 
                SELECT *, datetime('now', 'utc') as archived_at 
                FROM pv_power_cache 
                WHERE timestamp = ?
                "#,
                timestamp
            )
            .execute(&mut *archive_tx)
            .await?;

            // Delete from cache
            sqlx::query!("DELETE FROM pv_power_cache WHERE timestamp = ?", timestamp)
                .execute(&mut *cache_tx)
                .await?;
        }

        archive_tx.commit().await?;
        cache_tx.commit().await?;

        debug!(archived_count = timestamps.len(), "Power records archived");
        Ok(())
    }

    async fn archive_energy_records(&self, timestamps: &[String]) -> Result<()> {
        let mut archive_tx = self.archive_pool.begin().await?;
        let mut cache_tx = self.cache_pool.begin().await?;

        for timestamp in timestamps {
            // Move record from cache to archive
            sqlx::query!(
                r#"
                INSERT INTO pv_energy_archive 
                SELECT *, datetime('now', 'utc') as archived_at 
                FROM pv_energy_cache 
                WHERE timestamp = ?
                "#,
                timestamp
            )
            .execute(&mut *archive_tx)
            .await?;

            // Delete from cache
            sqlx::query!("DELETE FROM pv_energy_cache WHERE timestamp = ?", timestamp)
                .execute(&mut *cache_tx)
                .await?;
        }

        archive_tx.commit().await?;
        cache_tx.commit().await?;

        debug!(archived_count = timestamps.len(), "Energy records archived");
        Ok(())
    }

    pub async fn get_cache_stats(&self) -> Result<CacheStats> {
        let power_cached = sqlx::query_scalar!("SELECT COUNT(*) FROM pv_power_cache")
            .fetch_one(&self.cache_pool)
            .await?
            .unwrap_or(0) as u64;

        let energy_cached = sqlx::query_scalar!("SELECT COUNT(*) FROM pv_energy_cache")
            .fetch_one(&self.cache_pool)
            .await?
            .unwrap_or(0) as u64;

        let power_archived = sqlx::query_scalar!("SELECT COUNT(*) FROM pv_power_archive")
            .fetch_one(&self.archive_pool)
            .await?
            .unwrap_or(0) as u64;

        let energy_archived = sqlx::query_scalar!("SELECT COUNT(*) FROM pv_energy_archive")
            .fetch_one(&self.archive_pool)
            .await?
            .unwrap_or(0) as u64;

        Ok(CacheStats {
            power_records_cached: power_cached,
            energy_records_cached: energy_cached,
            power_records_archived: power_archived,
            energy_records_archived: energy_archived,
        })
    }

    // Manual cleanup function - triggered by state machine when needed
    pub async fn cleanup_old_cache(&self, days_old: i64) -> Result<u64> {
        let cutoff_date = (Utc::now() - chrono::Duration::days(days_old)).to_rfc3339();

        let power_deleted = sqlx::query!(
            "DELETE FROM pv_power_cache WHERE timestamp < ?",
            cutoff_date
        )
        .execute(&self.cache_pool)
        .await?
        .rows_affected();

        let energy_deleted = sqlx::query!(
            "DELETE FROM pv_energy_cache WHERE timestamp < ?",
            cutoff_date
        )
        .execute(&self.cache_pool)
        .await?
        .rows_affected();

        let total_deleted = power_deleted + energy_deleted;
        info!(
            power_deleted = power_deleted,
            energy_deleted = energy_deleted,
            total_deleted = total_deleted,
            cutoff_days = days_old,
            "Cleaned up old cache data"
        );

        Ok(total_deleted)
    }
}

// =============================================================================
// SUPPORTING TYPES AND STRUCTS
// =============================================================================

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

// =============================================================================
// DATABASE MANAGER - Coordinates both systems based on state machine input
// =============================================================================

pub struct DatabaseManager {
    pub postgres: PostgresDatabase,
    pub cache: SqliteCache,
}

impl DatabaseManager {
    pub async fn new(db_config: DatabaseConfig, cache_config: SqliteCacheConfig) -> Result<Self> {
        info!("Initializing database manager");

        let postgres = PostgresDatabase::new(db_config).await?;
        let cache = SqliteCache::new(cache_config).await?;

        info!("Database manager initialized successfully");
        Ok(Self { postgres, cache })
    }

    // Store data based on state machine decision
    pub async fn store_power_data(&self, data: &ProcessedData, use_postgres: bool) -> Result<()> {
        if use_postgres {
            self.postgres.store_power_data(data).await
        } else {
            self.cache.store_power_data(data).await
        }
    }

    pub async fn store_energy_data(&self, data: &DataHistory, use_postgres: bool) -> Result<()> {
        if use_postgres {
            self.postgres.store_energy_data(data).await
        } else {
            self.cache.store_energy_data(data).await
        }
    }

    // State machine triggered operations
    pub async fn sync_cache_to_postgres(&self) -> Result<SyncResult> {
        self.cache.sync_to_postgres(&self.postgres).await
    }

    pub async fn get_postgres_health(&self) -> PostgresHealth {
        self.postgres.get_health().await
    }

    pub async fn check_postgres_health(&self) -> Result<PostgresHealth> {
        self.postgres.health_check().await
    }

    pub async fn get_postgres_state(&self) -> PostgresState {
        self.postgres.get_state().await
    }

    pub async fn get_cache_stats(&self) -> Result<CacheStats> {
        self.cache.get_cache_stats().await
    }

    pub async fn cleanup_old_cache(&self, days_old: i64) -> Result<u64> {
        self.cache.cleanup_old_cache(days_old).await
    }
}

// =============================================================================
// TESTS MODULE
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calculator::{BatteryState, BatteryStatus, SupplyState};

    fn create_test_processed_data() -> ProcessedData {
        ProcessedData {
            supply_state: SupplyState::Surplus(800),
            battery_status: BatteryStatus {
                battery_state: BatteryState::Loading(600),
                battery_percent: 75,
                battery_energy: 6.5,
            },
            full_production: 2500,
            consumption: 1100,
        }
    }

    fn create_test_energy_data() -> crate::calculator::DataHistory {
        crate::calculator::DataHistory {
            grid_buy: 12500,           // Wh
            grid_sell: 18750,          // Wh
            production_energy: 25600,  // Wh
            consumption_energy: 19200, // Wh
            battery_loaded: 3200,      // Wh
            battery_discharge: 2950,   // Wh
            battery_cycles: 142,
        }
    }

    #[tokio::test]
    async fn test_record_conversion() {
        let processed_data = create_test_processed_data();
        let power_record = PvPowerRecord::from(&processed_data);

        assert_eq!(power_record.pv_production, 2500);
        assert_eq!(power_record.consumption, 1100);
        assert_eq!(power_record.battery_percent, 75);
        assert_eq!(power_record.battery_energy_wh, 6500); // 6.5kWh = 6500Wh

        let energy_data = create_test_energy_data();
        let energy_record = PvEnergyRecord::from(&energy_data);

        assert_eq!(energy_record.grid_buy_wh, 12500);
        assert_eq!(energy_record.battery_cycles, 142);
    }

    #[tokio::test]
    async fn test_sqlite_cache_creation() {
        let config = SqliteCacheConfig {
            max_cache_size_mb: 100,
            cache_db_path: "test_power_cache.db".to_string(),
            archive_db_path: "test_power_archive.db".to_string(),
            sync_batch_size: 100,
            cleanup_threshold_days: 150,
        };

        let cache = SqliteCache::new(config).await;
        assert!(cache.is_ok());

        // Cleanup
        std::fs::remove_file("test_cache.db").ok();
        std::fs::remove_file("test_archive.db").ok();
    }

    #[tokio::test]
    async fn test_cache_power_data_storage() {
        let config = SqliteCacheConfig {
            max_cache_size_mb: 100,
            cache_db_path: "test_power_cache.db".to_string(),
            archive_db_path: "test_power_archive.db".to_string(),
            sync_batch_size: 100,
            cleanup_threshold_days: 150,
        };

        let cache = SqliteCache::new(config).await.unwrap();
        let test_data = create_test_processed_data();

        let result = cache.store_power_data(&test_data).await;
        assert!(result.is_ok());

        let stats = cache.get_cache_stats().await.unwrap();
        assert_eq!(stats.power_records_cached, 1);

        // Cleanup
        std::fs::remove_file("test_power_cache.db").ok();
        std::fs::remove_file("test_power_archive.db").ok();
    }

    #[tokio::test]
    async fn test_energy_data_types() {
        let config = SqliteCacheConfig {
            max_cache_size_mb: 100,
            cache_db_path: "test_power_cache.db".to_string(),
            archive_db_path: "test_power_archive.db".to_string(),
            sync_batch_size: 100,
            cleanup_threshold_days: 150,
        };

        let cache = SqliteCache::new(config).await.unwrap();
        let test_data = create_test_energy_data();

        let result = cache.store_energy_data(&test_data).await;
        assert!(result.is_ok());

        let stats = cache.get_cache_stats().await.unwrap();
        assert_eq!(stats.energy_records_cached, 1);

        // Cleanup
        std::fs::remove_file("test_energy_cache.db").ok();
        std::fs::remove_file("test_energy_archive.db").ok();
    }
}
