use chrono::{DateTime, Utc};
use file_store::{
    traits::MsgDecode, wifi_heartbeat::WifiHeartbeatIngestReport, BytesMutStream, FileType,
};

use futures::TryStreamExt;
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeWifiHeartbeatIngestReport {}

#[async_trait::async_trait]
impl Decode for FileTypeWifiHeartbeatIngestReport {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                WifiHeartbeatIngestReport::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeWifiHeartbeatIngestReport {
    fn to_prefix(&self) -> String {
        FileType::WifiHeartbeatIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeWifiHeartbeatIngestReport {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_wifi_ingest_reports(
                    received_timestamp timestamptz not null,
                    hotspot_key text not null,
                    timestamp timestamptz not null,
                    lat numeric not null,
                    lon numeric not null,
                    location_validation_timestamp timestamptz,
                    operation_mode boolean not null,
                    coverage_object text
                )
            "#,
        )
        .execute(db)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }
}

#[async_trait::async_trait]
impl Insertable for Vec<WifiHeartbeatIngestReport> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 8) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            QueryBuilder::new("INSERT INTO mobile_wifi_ingest_reports(received_timestamp, hotspot_key, timestamp, lat, lon, location_validation_timestamp, operation_mode, coverage_object)")
            .push_values(chunk, |mut b, report| {

                let uuid = uuid::Uuid::from_slice(&report.report.coverage_object).expect("unable to create uuid");

                b.push_bind(report.received_timestamp)
                    .push_bind(report.report.pubkey.to_string())
                    .push_bind(report.report.timestamp)
                    .push_bind(report.report.lat)
                    .push_bind(report.report.lon)
                    .push_bind(report.report.location_validation_timestamp)
                    .push_bind(report.report.operation_mode)
                    .push_bind(uuid);
            })
            .build()
            .execute(db)
            .await?;
        }
        Ok(())
    }
}
