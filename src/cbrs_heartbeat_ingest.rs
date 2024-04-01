use std::str::FromStr;

use file_store::{heartbeat::CbrsHeartbeatIngestReport, traits::MsgDecode, FileType};
use helium_crypto::PublicKeyBinary;
use sqlx::{Pool, Postgres};

use crate::{DbTable, Decode, Persist, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeCbrsHeartbeatIngestReport {}

impl Decode for FileTypeCbrsHeartbeatIngestReport {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(CbrsHeartbeatIngestReport::decode(buf)?))
    }
}

impl ToPrefix for FileTypeCbrsHeartbeatIngestReport {
    fn to_prefix(&self) -> String {
        FileType::CbrsHeartbeatIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeCbrsHeartbeatIngestReport {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_cbrs_ingest_reports (
                    hotspot_key text not null,
                    hotspot_type text not null,
                    cell_id integer not null,
                    received_timestamp timestamptz not null,
                    timestamp timestamptz not null,
                    lat numeric not null,
                    lon numeric not null,
                    operation_mode boolean not null,
                    cbsd_category text not null,
                    cbsd_id text not null,
                    coverage_object bytea not null
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
impl Persist for CbrsHeartbeatIngestReport {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        sqlx::query(
                    r#"
                        INSERT INTO mobile_cbrs_ingest_reports(hotspot_key, hotspot_type, cell_id, received_timestamp, timestamp, lat, lon, operation_mode, cbsd_category, cbsd_id, coverage_object)
                        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    "#,
                )
                .bind(self.report.pubkey.to_string())
                .bind(self.report.hotspot_type)
                .bind(self.report.cell_id as i32)
                .bind(self.received_timestamp)
                .bind(self.report.timestamp)
                .bind(self.report.lat)
                .bind(self.report.lon)
                .bind(self.report.operation_mode)
                .bind(self.report.cbsd_category)
                .bind(self.report.cbsd_id)
                .bind(self.report.coverage_object)
                .execute(pool)
                .await
                .map(|_| ())
                .map_err(anyhow::Error::from)
    }
}
