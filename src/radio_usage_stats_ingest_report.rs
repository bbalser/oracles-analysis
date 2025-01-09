use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::{services::poc_mobile::RadioUsageStatsIngestReportV1, Message};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{to_datetime, to_datetime_ms, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeRadioUsageStatsIngestReport;

#[async_trait::async_trait]
impl Decode for FileTypeRadioUsageStatsIngestReport {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                RadioUsageStatsIngestReportV1::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeRadioUsageStatsIngestReport {
    fn to_prefix(&self) -> String {
        FileType::RadioUsageStatsIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeRadioUsageStatsIngestReport {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS radio_usage_stats_ingest(
                    received_timestamp timestamptz not null,
                    pubkey text not null,
                    cbsd_id text,
                    service_provider_user_count bigint not null,
                    disco_mapping_user_count bigint not null,
                    offload_user_count bigint not null,
                    service_provider_transfer_bytes bigint not null,
                    offload_transfer_bytes bigint not null,
                    epoch_start timestamptz not null,
                    epoch_end timestamptz not null,
                    generated_timestamp timestamptz not null
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
impl Insertable for Vec<RadioUsageStatsIngestReportV1> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 11) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            QueryBuilder::new("INSERT INTO radio_usage_stats_ingest(received_timestamp, pubkey, cbsd_id, service_provider_user_count, disco_mapping_user_count, offload_user_count, service_provider_transfer_bytes, offload_transfer_bytes, epoch_start, epoch_end, generated_timestamp)")
            .push_values(chunk, |mut b, report| {

                let req = report.clone().report.unwrap();

                b.push_bind(to_datetime_ms(report.received_timestamp))
                    .push_bind(PublicKeyBinary::from(req.hotspot_pubkey.clone()).to_string())
                    .push_bind(req.cbsd_id)
                    .push_bind(req.service_provider_user_count as i64)
                    .push_bind(req.disco_mapping_user_count as i64)
                    .push_bind(req.offload_user_count as i64)
                    .push_bind(req.service_provider_transfer_bytes as i64)
                    .push_bind(req.offload_transfer_bytes as i64)
                    .push_bind(to_datetime(req.epoch_start_timestamp))
                    .push_bind(to_datetime(req.epoch_end_timestamp))
                    .push_bind(to_datetime(req.timestamp));
                
            })
            .build()
            .execute(db)
            .await?;
        }

        Ok(())
    }
}
