use boosted_hex_update::FileTypeBoostedHexUpdate;
use cbrs_heartbeat_ingest::FileTypeCbrsHeartbeatIngestReport;
use cell_speedtest_ingest::FileTypeCellSpeedtestIngestReport;
use chrono::{DateTime, TimeZone, Utc};
use clap::ValueEnum;
use coverage_object::FileTypeCoverageObject;
use data_transfer_session_ingest::FileTypeDataTransferSessionIngestReport;
use file_store::BytesMutStream;
use iot_reward_share::FileTypeIotRewardShare;
use mobile_reward_share::FileTypeMobileRewardShare;
use oracle_boosting::FileTypeOracleBoostingReport;
use radio_thresholds::{FileTypeInvalidatedRadioThreshold, FileTypeRadioThreshold};
use radio_usage_stats_ingest_report::FileTypeRadioUsageStatsIngestReport;
use seniority_update::FileTypeSeniorityUpdate;
use service_provider_bans::FileTypeServiceProviderBan;
use sqlx::{Pool, Postgres};
use valid_data_transfer_session::FileTypeValidDataTransferSession;
use validated_heartbeat::FileTypeValidatedHeartbeat;
use verified_data_transfer_ingest::FileTypeVerifiedDataTransferIngest;
use wifi_heartbeat_ingest_report::FileTypeWifiHeartbeatIngestReport;

mod boosted_hex_update;
mod cbrs_heartbeat_ingest;
mod cell_speedtest_ingest;
pub mod commands;
mod coverage_object;
mod data_transfer_session_ingest;
mod iot_reward_share;
mod mobile_reward_share;
mod oracle_boosting;
mod radio_thresholds;
mod radio_usage_stats_ingest_report;
mod seniority_update;
mod service_provider_bans;
mod valid_data_transfer_session;
mod validated_heartbeat;
mod verified_data_transfer_ingest;
mod wifi_heartbeat_ingest_report;

#[derive(Debug, Clone, ValueEnum)]
pub enum SupportedFileTypes {
    BoostedHexUpdate,
    CbrsHeartbeatIngestReport,
    CellSpeedtestIngestReport,
    CoverageObject,
    DataTransferSessionIngestReport,
    IotRewardShare,
    MobileRewardShare,
    OracleBoostingReport,
    RadioThreshold,
    RadioUsageStatsIngestReport,
    InvalidatedRadioThreshold,
    SeniorityUpdate,
    ServiceProviderBans,
    ValidatedHeartbeat,
    ValidDataTransferSession,
    VerifiedDataTransferIngest,
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
            SupportedFileTypes::DataTransferSessionIngestReport => {
                Box::new(FileTypeDataTransferSessionIngestReport)
            }
            SupportedFileTypes::InvalidatedRadioThreshold => {
                Box::new(FileTypeInvalidatedRadioThreshold {})
            }
            SupportedFileTypes::IotRewardShare => Box::new(FileTypeIotRewardShare {}),
            SupportedFileTypes::MobileRewardShare => Box::new(FileTypeMobileRewardShare {}),
            SupportedFileTypes::OracleBoostingReport => Box::new(FileTypeOracleBoostingReport {}),
            SupportedFileTypes::RadioThreshold => Box::new(FileTypeRadioThreshold {}),
            SupportedFileTypes::RadioUsageStatsIngestReport => {
                Box::new(FileTypeRadioUsageStatsIngestReport)
            }
            SupportedFileTypes::SeniorityUpdate => Box::new(FileTypeSeniorityUpdate),
            SupportedFileTypes::ServiceProviderBans => Box::new(FileTypeServiceProviderBan),
            SupportedFileTypes::ValidDataTransferSession => {
                Box::new(FileTypeValidDataTransferSession)
            }
            SupportedFileTypes::ValidatedHeartbeat => Box::new(FileTypeValidatedHeartbeat {}),
            SupportedFileTypes::VerifiedDataTransferIngest => {
                Box::new(FileTypeVerifiedDataTransferIngest)
            }
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
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()>;
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
    use std::str::FromStr;

    use geo::{Contains, Point};
    use geojson::GeoJson;
    use h3o::{CellIndex, LatLng};
    use sqlx::{postgres::PgPoolOptions, Row};
    use tokio::fs;

    #[tokio::test]
    async fn brian() -> anyhow::Result<()> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(
                "postgres://novareadonly:Bt2Emvd-KKP*LR4sz-DCNMru@localhost:3310/mobile_verifier",
            )
            .await?;

        let rows = sqlx::query(r#"
            SELECT *
            from wifi_heartbeats
            where hotspot_key = '1trSusf1ZNK9urSaTVpiTYYXZYwPusjP5cimH2oDceMAGJAM4PYm4Ucqt4N9okqeZqWEfaHk4si3niQCsxHNBDdgTVv4gq8tcjVe44h5nKaWxXZDXKzdAxUeLxycJUAdNwZDV8GHmMAgBAQvZF5mh6gdZwRztHjoTCPZZEL7vkcG47HixfrV2bPySsuXtre4ZD3QQXZt8a7v1hLR7FxhUu5ouDebFEcbMTXHPoBTHwTs2XJtS6hsfEwdVNxUN6zi6rFJR8sG7bRphh9pRHjEbhT4ESjWSu98CwGpcSf2u88Fr3A9cCpiiyCtjMJReBVkNMbWYGN4SCU2qeeid57bDCThfFCZ6KQMQyH6VNsGMpCw8J'
            "#)
        .fetch_all(&pool).await?;

        let hexes: Vec<CellIndex> = sqlx::query_scalar::<_, i64>(r#"
            SELECT h.hex
            FROM coverage_objects co 
                inner join hexes h on co.uuid = h.uuid
            where co.radio_key = '1trSusf1ZNK9urSaTVpiTYYXZYwPusjP5cimH2oDceMAGJAM4PYm4Ucqt4N9okqeZqWEfaHk4si3niQCsxHNBDdgTVv4gq8tcjVe44h5nKaWxXZDXKzdAxUeLxycJUAdNwZDV8GHmMAgBAQvZF5mh6gdZwRztHjoTCPZZEL7vkcG47HixfrV2bPySsuXtre4ZD3QQXZt8a7v1hLR7FxhUu5ouDebFEcbMTXHPoBTHwTs2XJtS6hsfEwdVNxUN6zi6rFJR8sG7bRphh9pRHjEbhT4ESjWSu98CwGpcSf2u88Fr3A9cCpiiyCtjMJReBVkNMbWYGN4SCU2qeeid57bDCThfFCZ6KQMQyH6VNsGMpCw8J'
            "#)
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|i| CellIndex::try_from(i as u64))
        .collect::<Result<_,_>>()?;

        for row in rows {
            let lat = row.get::<f64, &str>("lat");
            let lon = row.get::<f64, &str>("lon");

            let ll: LatLng = LatLng::new(lat, lon)?
                .to_cell(h3o::Resolution::Twelve)
                .into();

            let max_m: f64 = hexes.iter().fold(0.0, |curr_max, curr_cov| {
                let cov = LatLng::from(*curr_cov);
                curr_max.max(cov.distance_m(ll))
            });

            dbg!(max_m);
        }

        Ok(())
    }

    #[tokio::test]
    async fn alaska() -> anyhow::Result<()> {
        let geojson_str = fs::read_to_string("alaska.geojson").await?;
        let feature: geojson::Feature = GeoJson::from_str(&geojson_str)?.try_into()?;
        let alaska: geo::Geometry = feature.geometry.unwrap().try_into()?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("posgres://postgres:postgres@localhost/alaska")
            .await?;

        let rows = sqlx::query(
            r#"
                WITH latest_assertions AS (
                	SELECT DISTINCT ON (serialnumber) *
                	FROM hotspot_assertions ha 
                	ORDER BY serialnumber, time DESC
                )
                SELECT la.*
                FROM sp_boosted_rewards_bans bans
                	INNER JOIN wifi_hotspots wh ON wh.public_key = bans.radio_key
                	INNER JOIN latest_assertions la ON wh.serialnumber = la.serialnumber
                WHERE bans.invalidated_at IS NULL
                	AND bans.radio_type = 'wifi'
            "#,
        )
        .fetch_all(&pool)
        .await?;

        for row in rows {
            let serialnumber = row.get::<String, &str>("serialnumber");
            let lat = row.get::<f64, &str>("latitude");
            let lon = row.get::<f64, &str>("longitude");

            let p: Point = (lon, lat).into();

            let result = alaska.contains(&p);

            sqlx::query(
                r#"
                INSERT INTO alaska(serialnumber, in_alaska) values($1, $2)
            "#,
            )
            .bind(serialnumber)
            .bind(result)
            .execute(&pool)
            .await?;
        }

        Ok(())
    }
}
