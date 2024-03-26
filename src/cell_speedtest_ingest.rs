use file_store::{speedtest::CellSpeedtestIngestReport, traits::MsgDecode, FileType};
use sqlx::{Pool, Postgres};

use crate::{DbTable, Decode, Persist, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeCellSpeedtestIngestReport {}

impl Decode for FileTypeCellSpeedtestIngestReport {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(CellSpeedtestIngestReport::decode(buf)?))
    }
}

impl ToPrefix for FileTypeCellSpeedtestIngestReport {
    fn to_prefix(&self) -> String {
        FileType::CellSpeedtestIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeCellSpeedtestIngestReport {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_speedtest_ingest_reports(
                  hotspot_key text not null,
                  serial text not null,
                  timestamp timestamptz not null,
                  received_timestamp timestamptz not null,
                  upload_speed bigint not null,
                  download_speed bigint not null,
                  latency integer not null
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
impl Persist for CellSpeedtestIngestReport {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                INSERT INTO mobile_speedtest_ingest_reports(hotspot_key, serial, timestamp,
                    received_timestamp, upload_speed, download_speed, latency)
                VALUES($1,$2,$3,$4,$5,$6,$7)
            "#,
        )
        .bind(self.report.pubkey.to_string())
        .bind(self.report.serial)
        .bind(self.report.timestamp)
        .bind(self.received_timestamp)
        .bind(self.report.upload_speed as i64)
        .bind(self.report.download_speed as i64)
        .bind(self.report.latency as i64)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }
}
