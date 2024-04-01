use file_store::{traits::MsgDecode, wifi_heartbeat::WifiHeartbeatIngestReport, FileType};

use sqlx::{Pool, Postgres};

use crate::{DbTable, Decode, Persist, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeWifiHeartbeatIngestReport {}

impl Decode for FileTypeWifiHeartbeatIngestReport {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(WifiHeartbeatIngestReport::decode(buf)?))
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
impl Persist for WifiHeartbeatIngestReport {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        let uuid = uuid::Uuid::from_slice(&self.report.coverage_object)?;
        sqlx::query(r#"
            INSERT INTO mobile_wifi_ingest_reports(received_timestamp, hotspot_key, timestamp, lat, lon, location_validation_timestamp, operation_mode, coverage_object)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            "#)
            .bind(self.received_timestamp)
            .bind(self.report.pubkey.to_string())
            .bind(self.report.timestamp)
            .bind(self.report.lat)
            .bind(self.report.lon)
            .bind(self.report.location_validation_timestamp)
            .bind(self.report.operation_mode)
            .bind(uuid)
            .execute(pool)
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }
}
