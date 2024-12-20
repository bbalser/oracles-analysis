use chrono::{DateTime, Utc};
use file_store::{
    speedtest::CellSpeedtestIngestReport, traits::MsgDecode, BytesMutStream, FileType,
};
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use crate::{DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeCellSpeedtestIngestReport {}

#[async_trait::async_trait]
impl Decode for FileTypeCellSpeedtestIngestReport {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                CellSpeedtestIngestReport::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
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
impl Insertable for Vec<CellSpeedtestIngestReport> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        for test in self {
            sqlx::query(
                r#"
                INSERT INTO mobile_speedtest_ingest_reports(hotspot_key, serial, timestamp,
                    received_timestamp, upload_speed, download_speed, latency)
                VALUES($1,$2,$3,$4,$5,$6,$7)
            "#,
            )
            .bind(test.report.pubkey.to_string())
            .bind(&test.report.serial)
            .bind(test.report.timestamp)
            .bind(test.received_timestamp)
            .bind(test.report.upload_speed as i64)
            .bind(test.report.download_speed as i64)
            .bind(test.report.latency as i64)
            .execute(db)
            .await?;
        }
        Ok(())
    }
}
