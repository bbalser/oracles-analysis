use boosted_hex_update::FileTypeBoostedHexUpdate;
use cbrs_heartbeat_ingest::FileTypeCbrsHeartbeatIngestReport;
use cell_speedtest_ingest::FileTypeCellSpeedtestIngestReport;
use chrono::{DateTime, TimeZone, Utc};
use clap::ValueEnum;
use coverage_object::FileTypeCoverageObject;
use file_store::BytesMutStream;
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
pub mod commands;
mod coverage_object;
mod iot_reward_share;
mod mobile_reward_share;
mod oracle_boosting;
mod radio_thresholds;
mod validated_heartbeat;
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

    pub async fn decode(&self, buf: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        self.inner().decode(buf).await
    }

    pub async fn create_table(&self, db: &Pool<Postgres>) -> anyhow::Result<()> {
        self.inner().create_table(db).await
    }
}

#[async_trait::async_trait]
pub trait Decode {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>>;
}

#[async_trait::async_trait]
pub trait Insertable {
    async fn insert(&self, db: &Pool<Postgres>) -> anyhow::Result<()>;
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

pub fn to_optional_datetime(timestamp: u64) -> Option<DateTime<Utc>> {
    if timestamp == 0 {
        None
    } else {
        Some(to_datetime(timestamp))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use apache_avro::{from_value, Reader};
    use chrono::NaiveDateTime;
    use serde::Deserialize;
    use uuid::Uuid;

    #[derive(Clone, Debug, Deserialize)]
    pub struct CdrRaw {
        pub id: String,
        pub cdr_type: String,
        pub record_type: i16,
        // The time the record was written by T-Mobile
        pub creation_time: String,
        // The time the record was harvested from the FTP
        pub created_at: String,
        pub imsi: String,
        pub msisdn: String,
        pub imei: String,
        pub total_volume: Option<f64>,
        pub uplink_volume: Option<f64>,
        pub downlink_volume: Option<f64>,
        pub call_time_min: Option<i64>,
        pub agw_sn: Option<String>,
        pub subscriber_id: Option<Uuid>,
        pub cbsd_id: Option<String>,
    }

    #[tokio::test]
    async fn brian() -> anyhow::Result<()> {
        let f = File::open("hm-wifirecords-20240424161927.dat")?;
        let reader = Reader::new(f)?;

        let value = reader.into_iter().next().unwrap()?;
        dbg!(&value);
        let cdr = from_value::<CdrRaw>(&value)?;
        dbg!(cdr);

        // for value in reader {
        //     dbg!(&value);
        //     let cdr = from_value::<Cdr>(&value.unwrap())?;
        //     dbg!(cdr);
        // }

        Ok(())
    }
}
