use boosted_hex_update::FileTypeBoostedHexUpdate;
use bytes::BytesMut;
use cbrs_heartbeat_ingest::FileTypeCbrsHeartbeatIngestReport;
use cell_speedtest_ingest::FileTypeCellSpeedtestIngestReport;
use chrono::{DateTime, TimeZone, Utc};
use clap::ValueEnum;
use coverage_object::FileTypeCoverageObject;
use iot_reward_share::FileTypeIotRewardShare;
use mobile_reward_share::FileTypeMobileRewardShare;
use oracle_boosting::FileTypeOracleBoostingReport;
use radio_thresholds::{FileTypeInvalidatedRadioThreshold, FileTypeRadioThreshold};
use sqlx::{Pool, Postgres};
use validated_heartbeat::FileTypeValidatedHeartbeat;
use wifi_heartbeat_ingest_report::FileTypeWifiHeartbeatIngestReport;

mod boosted_hex_update;
mod cbrs_heartbeat_ingest;
mod cell_speedtest_ingest;
mod coverage_object;
mod iot_reward_share;
mod mobile_reward_share;
mod oracle_boosting;
mod radio_thresholds;
pub mod validated_heartbeat;
mod wifi_heartbeat_ingest_report;

#[derive(Debug, Clone, ValueEnum)]
pub enum SupportedFileTypes {
    BoostedHexUpdate,
    CbrsHeartbeatIngestReport,
    CellSpeedtestIngestReport,
    CoverageObject,
    IotRewardShare,
    MobileRewardShare,
    OracleBoostingReport,
    RadioThreshold,
    InvalidatedRadioThreshold,
    ValidatedHeartbeat,
    WifiHeartbeatIngestReport,
}

trait SupportedFileTypeTrait: Decode + ToPrefix + DbTable {}
impl<T> SupportedFileTypeTrait for T where T: Decode + ToPrefix + DbTable {}

impl SupportedFileTypes {
    fn inner(&self) -> Box<dyn SupportedFileTypeTrait> {
        match self {
            SupportedFileTypes::BoostedHexUpdate => Box::new(FileTypeBoostedHexUpdate {}),
            SupportedFileTypes::CbrsHeartbeatIngestReport => {
                Box::new(FileTypeCbrsHeartbeatIngestReport {})
            }
            SupportedFileTypes::CellSpeedtestIngestReport => {
                Box::new(FileTypeCellSpeedtestIngestReport {})
            }
            SupportedFileTypes::CoverageObject => Box::new(FileTypeCoverageObject {}),
            SupportedFileTypes::InvalidatedRadioThreshold => {
                Box::new(FileTypeInvalidatedRadioThreshold {})
            }
            SupportedFileTypes::IotRewardShare => Box::new(FileTypeIotRewardShare {}),
            SupportedFileTypes::MobileRewardShare => Box::new(FileTypeMobileRewardShare {}),
            SupportedFileTypes::OracleBoostingReport => Box::new(FileTypeOracleBoostingReport {}),
            SupportedFileTypes::RadioThreshold => Box::new(FileTypeRadioThreshold {}),
            SupportedFileTypes::ValidatedHeartbeat => Box::new(FileTypeValidatedHeartbeat {}),
            SupportedFileTypes::WifiHeartbeatIngestReport => {
                Box::new(FileTypeWifiHeartbeatIngestReport {})
            }
        }
    }

    pub fn prefix(&self) -> String {
        self.inner().to_prefix()
    }

    pub fn decode(&self, buf: BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        self.inner().decode(buf)
    }

    pub async fn create_table(&self, db: &Pool<Postgres>) -> anyhow::Result<()> {
        self.inner().create_table(db).await
    }
}

#[async_trait::async_trait]
pub trait Persist {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()>;
}

pub trait Decode {
    fn decode(&self, buf: BytesMut) -> anyhow::Result<Box<dyn Persist>>;
}

pub trait ToPrefix {
    fn to_prefix(&self) -> String;
}

#[async_trait::async_trait]
pub trait DbTable {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()>;
    // async fn drop_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()>;
}

pub fn to_datetime(timestamp: u64) -> DateTime<Utc> {
    Utc.timestamp_opt(timestamp as i64, 0).single().unwrap()
}

pub fn to_datetime_ms(timestamp: u64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp as i64).single().unwrap()
}

fn to_optional_datetime(timestamp: u64) -> Option<DateTime<Utc>> {
    if timestamp == 0 {
        None
    } else {
        Some(to_datetime(timestamp))
    }
}
