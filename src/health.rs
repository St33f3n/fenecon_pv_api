use crate::calculator::{DataHistory, ProcessedData};
use crate::collector::RawPVData;
use crate::config::Config;
use crate::db::{PostgresDatabase, SqliteCache};
use crate::mqtt::{MQTTHealthStatus, SolarMqttClient};
use color_eyre::eyre::{Result, WrapErr, eyre};
use statum::{machine, state};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

// =============================================================================
// STATE DEFINITIONS
// =============================================================================

#[state]
#[derive(Clone, Debug)]
pub enum HealthState {
    Healthy,
    DegradedNoDB,
    DegradedNoMqtt,
    CacheOnly,
    Shutdown,
}

// =============================================================================
// RESULT TYPE
// =============================================================================

#[derive(Debug)]
pub enum CoordinatorResult {
    Continue,
    TransitionTo(HealthStateTransition),
    Shutdown,
}

#[derive(Debug)]
pub enum HealthStateTransition {
    ToHealthy,
    ToDegradedNoDB(ProcessedData, DataHistory),
    ToDegradedNoMqtt,
    ToCacheOnly(ProcessedData, DataHistory),
    ToShutdown,
}

// =============================================================================
// COORDINATOR STATE MACHINE
// =============================================================================

#[machine]
#[derive(Clone, Debug)]
pub struct Coordinator<S: HealthState> {
    mqtt_client: SolarMqttClient,
    pgdb: PostgresDatabase,
    cache: SqliteCache,
    config: Config,
    last_recovery_attempt: Instant,
}

// =============================================================================
// STATE IMPLEMENTATIONS
// =============================================================================

impl Coordinator<Healthy> {
    pub async fn start() -> Result<Self> {
        let config = Config::new();
        let client = SolarMqttClient::new(&config.mqtt_config, "pv_api".to_string()).await?;
        let db = PostgresDatabase::new(config.database_config.clone()).await?;
        let cache = SqliteCache::new(config.sqlite_cache_config.clone()).await?;
        client.setup_discovery().await?;

        client.publish_availability(true).await;
        Ok(Coordinator::new(client, db, cache, config, Instant::now()))
    }

    pub async fn run_cycle(&mut self) -> Result<CoordinatorResult> {
        info!("Running standard cycle in Healthy state");

        let raw_data = collect_raw_data_with_retry(&self.config.pv_baseaddress).await?;
        let processed_data =
            ProcessedData::process_raw(raw_data.clone(), &self.config.battery_config);
        let data_history = DataHistory::process_raw(raw_data, &self.config.battery_config);

        let db_result = self.pgdb.store_power_data(&processed_data).await;
        let energy_result = self.pgdb.store_energy_data(&data_history).await;
        let mqtt_result = self.mqtt_client.publish_current_data(&processed_data).await;

        self.mqtt_client.publish_state_data(&processed_data).await;
        self.mqtt_client.publish_history_data(&data_history).await;
        // Determine transition based on what failed - pass data to transitions
        match (
            db_result.is_ok() && energy_result.is_ok(),
            mqtt_result.is_ok(),
        ) {
            (true, true) => {
                debug!("All operations successful, staying healthy");
                Ok(CoordinatorResult::Continue)
            }
            (true, false) => {
                warn!("MQTT failed, transitioning to DegradedNoMqtt");
                Ok(CoordinatorResult::TransitionTo(
                    HealthStateTransition::ToDegradedNoMqtt,
                ))
            }
            (false, true) => {
                warn!("Database failed, transitioning to DegradedNoDB with data backup");
                Ok(CoordinatorResult::TransitionTo(
                    HealthStateTransition::ToDegradedNoDB(processed_data, data_history),
                ))
            }
            (false, false) => {
                error!("Both DB and MQTT failed, transitioning to CacheOnly with data backup");
                Ok(CoordinatorResult::TransitionTo(
                    HealthStateTransition::ToCacheOnly(processed_data, data_history),
                ))
            }
        }
    }

    pub async fn to_degraded_no_db(
        self,
        power_data: ProcessedData,
        energy_data: DataHistory,
    ) -> Coordinator<DegradedNoDB> {
        info!("Transitioning from Healthy to DegradedNoDB - saving data to cache");

        let power_res = self.cache.store_power_data(&power_data).await;
        let energy_res = self.cache.store_energy_data(&energy_data).await;

        if power_res.is_err() || energy_res.is_err() {
            error!("CRITICAL: Cache storage failed during transition - forcing shutdown");
            self.to_shutdown();
            unreachable!();
        } else {
            info!("Data successfully saved to cache during DB failure");
            self.transition()
        }
    }

    pub fn to_degraded_no_mqtt(self) -> Coordinator<DegradedNoMqtt> {
        info!("Transitioning from Healthy to DegradedNoMqtt");
        self.transition()
    }

    pub async fn to_cache_only(
        self,
        power_data: ProcessedData,
        energy_data: DataHistory,
    ) -> Coordinator<CacheOnly> {
        info!("Transitioning from Healthy to CacheOnly - saving data to cache");

        let power_res = self.cache.store_power_data(&power_data).await;
        let energy_res = self.cache.store_energy_data(&energy_data).await;

        if power_res.is_err() || energy_res.is_err() {
            error!("CRITICAL: Cache storage failed during transition - forcing shutdown");
            self.to_shutdown();
            unreachable!();
        } else {
            info!("Data successfully saved to cache during service failures");
            self.transition()
        }
    }

    pub fn to_shutdown(self) -> Coordinator<Shutdown> {
        info!("Transitioning from Healthy to Shutdown");
        self.transition()
    }
}

impl Coordinator<DegradedNoDB> {
    pub async fn run_cycle(&mut self) -> Result<CoordinatorResult> {
        if self.should_attempt_recovery() {
            debug!("Attempting database recovery in DegradedNoDB");
            match self.pgdb.health_check().await {
                Ok(crate::db::PostgresHealth::Healthy) => {
                    info!("Database recovered! Transitioning to Healthy and syncing cache");
                    // Trigger cache sync during transition
                    return Ok(CoordinatorResult::TransitionTo(
                        HealthStateTransition::ToHealthy,
                    ));
                }
                _ => {
                    debug!("Database still not available");
                    self.last_recovery_attempt = Instant::now();
                }
            }
        }

        // Normal degraded cycle: collect -> process -> store cache + MQTT
        info!("Running degraded cycle (no DB) - using cache + MQTT");

        let raw_data = collect_raw_data_with_retry(&self.config.pv_baseaddress).await?;

        let processed_data =
            ProcessedData::process_raw(raw_data.clone(), &self.config.battery_config);
        let data_history = DataHistory::process_raw(raw_data, &self.config.battery_config);

        if let Err(e) = self.cache.store_power_data(&processed_data).await {
            error!("Cache storage failed: {}", e);
            return Ok(CoordinatorResult::TransitionTo(
                HealthStateTransition::ToShutdown,
            ));
        }

        if let Err(e) = self.cache.store_energy_data(&data_history).await {
            error!("Cache energy storage failed: {}", e);
            return Ok(CoordinatorResult::TransitionTo(
                HealthStateTransition::ToShutdown,
            ));
        }

        if let Err(e) = self.mqtt_client.publish_current_data(&processed_data).await {
            warn!(
                "MQTT failed in DegradedNoDB: {}, transitioning to CacheOnly",
                e
            );
            return Ok(CoordinatorResult::TransitionTo(
                HealthStateTransition::ToCacheOnly(processed_data, data_history),
            ));
        }

        self.mqtt_client.publish_state_data(&processed_data).await;
        self.mqtt_client.publish_history_data(&data_history).await;
        debug!("DegradedNoDB cycle completed successfully");
        Ok(CoordinatorResult::Continue)
    }

    pub async fn to_healthy(self) -> Coordinator<Healthy> {
        info!("Transitioning from DegradedNoDB to Healthy - starting cache sync");

        // Sync cache to postgres during transition
        if let Err(e) = self.cache.sync_to_postgres(&self.pgdb).await {
            warn!("Cache sync failed during transition: {}", e);
        } else {
            info!("Cache sync completed successfully");
        }

        self.transition()
    }

    pub fn to_cache_only(self) -> Coordinator<CacheOnly> {
        info!("Transitioning from DegradedNoDB to CacheOnly");
        self.transition()
    }

    pub fn to_shutdown(self) -> Coordinator<Shutdown> {
        info!("Transitioning from DegradedNoDB to Shutdown");
        self.transition()
    }
}

impl Coordinator<DegradedNoMqtt> {
    pub async fn run_cycle(&mut self) -> Result<CoordinatorResult> {
        // First: Try to recover MQTT connection
        if self.should_attempt_recovery() {
            debug!("Attempting MQTT recovery in DegradedNoMqtt");
            match self.mqtt_client.get_health_status().await {
                MQTTHealthStatus::Healthy => {
                    info!("MQTT recovered! Transitioning to Healthy");
                    return Ok(CoordinatorResult::TransitionTo(
                        HealthStateTransition::ToHealthy,
                    ));
                }
                _ => {
                    debug!("MQTT still not available");
                    self.last_recovery_attempt = Instant::now();
                }
            }
        }

        info!("Running degraded cycle (no MQTT) - using DB only");

        let raw_data = collect_raw_data_with_retry(&self.config.pv_baseaddress).await?;

        let processed_data =
            ProcessedData::process_raw(raw_data.clone(), &self.config.battery_config);
        let data_history = DataHistory::process_raw(raw_data, &self.config.battery_config);

        // Store to DB
        let db_result = self.pgdb.store_power_data(&processed_data).await;
        let energy_result = self.pgdb.store_energy_data(&data_history).await;

        if db_result.is_err() || energy_result.is_err() {
            warn!("Database failed in DegradedNoMqtt, transitioning to CacheOnly");
            return Ok(CoordinatorResult::TransitionTo(
                HealthStateTransition::ToCacheOnly(processed_data, data_history),
            ));
        }

        debug!("DegradedNoMqtt cycle completed successfully");
        Ok(CoordinatorResult::Continue)
    }

    pub fn to_healthy(self) -> Coordinator<Healthy> {
        info!("Transitioning from DegradedNoMqtt to Healthy");
        self.transition()
    }

    pub fn to_cache_only(self) -> Coordinator<CacheOnly> {
        info!("Transitioning from DegradedNoMqtt to CacheOnly");
        self.transition()
    }

    pub fn to_shutdown(self) -> Coordinator<Shutdown> {
        info!("Transitioning from DegradedNoMqtt to Shutdown");
        self.transition()
    }
}

impl Coordinator<CacheOnly> {
    pub async fn run_cycle(&mut self) -> Result<CoordinatorResult> {
        if self.should_attempt_recovery() {
            debug!("Attempting service recovery in CacheOnly");

            let db_healthy = matches!(
                self.pgdb
                    .health_check()
                    .await
                    .unwrap_or(crate::db::PostgresHealth::Disconnected),
                crate::db::PostgresHealth::Healthy
            );

            let mqtt_healthy = matches!(
                self.mqtt_client.get_health_status().await,
                MQTTHealthStatus::Healthy
            );

            match (db_healthy, mqtt_healthy) {
                (true, true) => {
                    info!("Both services recovered! Transitioning to Healthy");
                    return Ok(CoordinatorResult::TransitionTo(
                        HealthStateTransition::ToHealthy,
                    ));
                }
                (true, false) => {
                    info!("Database recovered, transitioning to DegradedNoMqtt");
                    return Ok(CoordinatorResult::TransitionTo(
                        HealthStateTransition::ToDegradedNoMqtt,
                    ));
                }
                (false, true) => {
                    info!("MQTT recovered, transitioning to DegradedNoDB");
                    // Don't pass default data, just transition
                    return Ok(CoordinatorResult::TransitionTo(
                        HealthStateTransition::ToDegradedNoMqtt,
                    ));
                }
                (false, false) => {
                    debug!("No services recovered yet");
                    self.last_recovery_attempt = Instant::now();
                }
            }
        }

        // Normal cache-only cycle: try to collect -> store to cache only
        info!("Running cache-only cycle");

        if let Ok(raw_data) = RawPVData::fill_raw(&self.config.pv_baseaddress).await {
            let processed_data =
                ProcessedData::process_raw(raw_data.clone(), &self.config.battery_config);
            let data_history = DataHistory::process_raw(raw_data, &self.config.battery_config);

            if let Err(e) = self.cache.store_power_data(&processed_data).await {
                error!("Cache storage failed in CacheOnly: {}", e);
                return Ok(CoordinatorResult::TransitionTo(
                    HealthStateTransition::ToShutdown,
                ));
            }

            if let Err(e) = self.cache.store_energy_data(&data_history).await {
                error!("Cache energy storage failed in CacheOnly: {}", e);
                return Ok(CoordinatorResult::TransitionTo(
                    HealthStateTransition::ToShutdown,
                ));
            }

            debug!("Data stored to cache successfully");
        } else {
            warn!("Data collection failed in CacheOnly mode");
        }

        Ok(CoordinatorResult::Continue)
    }

    pub async fn to_healthy(self) -> Coordinator<Healthy> {
        info!("Transitioning from CacheOnly to Healthy - starting cache sync");

        // Sync cache to postgres during transition
        if let Err(e) = self.cache.sync_to_postgres(&self.pgdb).await {
            warn!("Cache sync failed during transition: {}", e);
        } else {
            info!("Cache sync completed successfully");
        }

        self.transition()
    }

    pub fn to_degraded_no_db(self) -> Coordinator<DegradedNoDB> {
        info!("Transitioning from CacheOnly to DegradedNoDB");
        self.transition()
    }

    pub fn to_degraded_no_mqtt(self) -> Coordinator<DegradedNoMqtt> {
        info!("Transitioning from CacheOnly to DegradedNoMqtt");
        self.transition()
    }

    pub fn to_shutdown(self) -> Coordinator<Shutdown> {
        info!("Transitioning from CacheOnly to Shutdown");
        self.transition()
    }
}

impl Coordinator<Shutdown> {
    pub async fn run_cycle(&mut self) -> Result<CoordinatorResult> {
        info!("Running cleanup in Shutdown state");
        self.cleanup().await?;
        Ok(CoordinatorResult::Shutdown)
    }

    pub async fn cleanup(&self) -> Result<()> {
        info!("Performing cleanup operations");

        // Publish offline status
        self.mqtt_client.publish_availability(false).await;

        // Sync any remaining cache data
        if let Err(e) = self.cache.sync_to_postgres(&self.pgdb).await {
            warn!("Failed to sync cache during shutdown: {}", e);
        }

        info!("Cleanup completed");
        Ok(())
    }
}

// =============================================================================
// SHARED METHODS (Available to all states)
// =============================================================================

impl<S: HealthState> Coordinator<S> {
    fn should_attempt_recovery(&self) -> bool {
        // Try recovery every 30 seconds
        self.last_recovery_attempt.elapsed() > Duration::from_secs(10)
    }

    pub async fn check_mqtt_health(&self) -> MQTTHealthStatus {
        self.mqtt_client.get_health_status().await
    }

    pub async fn check_postgres_health(&self) -> Result<bool> {
        match self.pgdb.health_check().await {
            Ok(crate::db::PostgresHealth::Healthy) => Ok(true),
            _ => Ok(false),
        }
    }
}

// =============================================================================
// ENUM FOR PATTERN MATCHING IN MAIN LOOP
// =============================================================================

#[derive(Debug)]
pub enum CoordinatorKind {
    Healthy(Coordinator<Healthy>),
    DegradedNoDB(Coordinator<DegradedNoDB>),
    DegradedNoMqtt(Coordinator<DegradedNoMqtt>),
    CacheOnly(Coordinator<CacheOnly>),
    Shutdown(Coordinator<Shutdown>),
}

impl CoordinatorKind {
    pub async fn run_cycle(&mut self) -> Result<CoordinatorResult> {
        match self {
            CoordinatorKind::Healthy(c) => c.run_cycle().await,
            CoordinatorKind::DegradedNoDB(c) => c.run_cycle().await,
            CoordinatorKind::DegradedNoMqtt(c) => c.run_cycle().await,
            CoordinatorKind::CacheOnly(c) => c.run_cycle().await,
            CoordinatorKind::Shutdown(c) => c.run_cycle().await,
        }
    }
}

// =============================================================================
// MAIN LOOP IMPLEMENTATION
// =============================================================================

pub async fn run_coordinator() -> Result<()> {
    info!("Starting coordinator main loop");

    let mut coordinator = CoordinatorKind::Healthy(Coordinator::start().await?);

    loop {
        coordinator = match coordinator.run_cycle().await? {
            CoordinatorResult::Continue => coordinator,

            CoordinatorResult::TransitionTo(transition) => {
                info!("Performing state transition: {:?}", transition);

                match transition {
                    HealthStateTransition::ToHealthy => match coordinator {
                        CoordinatorKind::DegradedNoDB(c) => {
                            CoordinatorKind::Healthy(c.to_healthy().await)
                        }
                        CoordinatorKind::DegradedNoMqtt(c) => {
                            CoordinatorKind::Healthy(c.to_healthy())
                        }
                        CoordinatorKind::CacheOnly(c) => {
                            CoordinatorKind::Healthy(c.to_healthy().await)
                        }
                        other => other,
                    },

                    HealthStateTransition::ToDegradedNoDB(power_data, energy_data) => {
                        match coordinator {
                            CoordinatorKind::Healthy(c) => CoordinatorKind::DegradedNoDB(
                                c.to_degraded_no_db(power_data, energy_data).await,
                            ),
                            CoordinatorKind::DegradedNoMqtt(c) => {
                                CoordinatorKind::DegradedNoDB(c.to_cache_only().to_degraded_no_db())
                            }
                            CoordinatorKind::CacheOnly(c) => {
                                CoordinatorKind::DegradedNoDB(c.to_degraded_no_db())
                            }
                            other => other,
                        }
                    }

                    HealthStateTransition::ToDegradedNoMqtt => match coordinator {
                        CoordinatorKind::Healthy(c) => {
                            CoordinatorKind::DegradedNoMqtt(c.to_degraded_no_mqtt())
                        }
                        CoordinatorKind::DegradedNoDB(c) => {
                            CoordinatorKind::DegradedNoMqtt(c.to_cache_only().to_degraded_no_mqtt())
                        }
                        CoordinatorKind::CacheOnly(c) => {
                            CoordinatorKind::DegradedNoMqtt(c.to_degraded_no_mqtt())
                        }
                        other => other,
                    },

                    HealthStateTransition::ToCacheOnly(power_data, energy_data) => {
                        match coordinator {
                            CoordinatorKind::Healthy(c) => CoordinatorKind::CacheOnly(
                                c.to_cache_only(power_data, energy_data).await,
                            ),
                            CoordinatorKind::DegradedNoDB(c) => {
                                CoordinatorKind::CacheOnly(c.to_cache_only())
                            }
                            CoordinatorKind::DegradedNoMqtt(c) => {
                                CoordinatorKind::CacheOnly(c.to_cache_only())
                            }
                            other => other,
                        }
                    }

                    HealthStateTransition::ToShutdown => match coordinator {
                        CoordinatorKind::Healthy(c) => CoordinatorKind::Shutdown(c.to_shutdown()),
                        CoordinatorKind::DegradedNoDB(c) => {
                            CoordinatorKind::Shutdown(c.to_shutdown())
                        }
                        CoordinatorKind::DegradedNoMqtt(c) => {
                            CoordinatorKind::Shutdown(c.to_shutdown())
                        }
                        CoordinatorKind::CacheOnly(c) => CoordinatorKind::Shutdown(c.to_shutdown()),
                        other => other,
                    },
                }
            }

            CoordinatorResult::Shutdown => {
                info!("Shutdown requested, exiting main loop");
                break;
            }
        };
        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    info!("Coordinator main loop completed");
    Ok(())
}

async fn collect_raw_data_with_retry(basepath: &str) -> Result<RawPVData> {
    const MAX_RETRIES: u8 = 3;
    const BASE_DELAY_MS: u64 = 100;

    for attempt in 0..MAX_RETRIES {
        match RawPVData::fill_raw(basepath).await {
            Ok(data) => return Ok(data),
            Err(e) => {
                if attempt == MAX_RETRIES - 1 {
                    error!(
                        "Data collection failed after {} attempts: {}",
                        MAX_RETRIES, e
                    );
                    return Err(e);
                }
                warn!(
                    "Data collection attempt {} failed: {}, retrying",
                    attempt + 1,
                    e
                );
                let delay = Duration::from_millis(BASE_DELAY_MS * 2_u64.pow(attempt as u32));
                tokio::time::sleep(delay).await;
            }
        }
    }
    unreachable!()
}
