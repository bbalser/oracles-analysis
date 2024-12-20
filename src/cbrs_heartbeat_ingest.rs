use chrono::{DateTime, Utc};
use file_store::{
    heartbeat::CbrsHeartbeatIngestReport, traits::MsgDecode, BytesMutStream, FileType,
};
use futures::TryStreamExt;
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeCbrsHeartbeatIngestReport {}

#[async_trait::async_trait]
impl Decode for FileTypeCbrsHeartbeatIngestReport {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                CbrsHeartbeatIngestReport::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
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
impl Insertable for Vec<CbrsHeartbeatIngestReport> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 11) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            let mut qb = QueryBuilder::new(
                r#"
                INSERT INTO mobile_cbrs_ingest_reports(hotspot_key, hotspot_type, cell_id, received_timestamp, timestamp, lat, lon, operation_mode, cbsd_category, cbsd_id, coverage_object)
                "#,
            );

            qb.push_values(chunk, |mut b, hb| {
                b.push_bind(hb.report.pubkey.to_string())
                    .push_bind(&hb.report.hotspot_type)
                    .push_bind(hb.report.cell_id as i32)
                    .push_bind(hb.received_timestamp)
                    .push_bind(hb.report.timestamp)
                    .push_bind(hb.report.lat)
                    .push_bind(hb.report.lon)
                    .push_bind(hb.report.operation_mode)
                    .push_bind(&hb.report.cbsd_category)
                    .push_bind(&hb.report.cbsd_id)
                    .push_bind(&hb.report.coverage_object);
            })
            .build()
            .execute(db)
            .await?;
        }
        Ok(())
    }
}
