use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::{services::poc_mobile::DataTransferSessionIngestReportV1, Message};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{to_datetime, to_datetime_ms, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeDataTransferSessionIngestReport;

#[async_trait::async_trait]
impl Decode for FileTypeDataTransferSessionIngestReport {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                DataTransferSessionIngestReportV1::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeDataTransferSessionIngestReport {
    fn to_prefix(&self) -> String {
        FileType::DataTransferSessionIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeDataTransferSessionIngestReport {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS data_transfer_session_ingest_reports(
                    pub_key text not null,
                    received_timestamp timestamptz not null,
                    timestamp timestamptz not null,
                    payer text not null,
                    upload_bytes bigint not null,
                    download_bytes bigint not null,
                    rewardable_bytes bigint not null,
                    reward_cancelled bool not null,
                    event_id text not null
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
impl Insertable for Vec<DataTransferSessionIngestReportV1> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 9) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            QueryBuilder::new("INSERT INTO data_transfer_session_ingest_reports(pub_key, received_timestamp, timestamp, payer, upload_bytes, download_bytes, rewardable_bytes, reward_cancelled, event_id)")
            .push_values(chunk, |mut b, report| {

                let req = report.clone().report.unwrap();
                let usage = req.data_transfer_usage.unwrap();

                b.push_bind(PublicKeyBinary::from(usage.pub_key.clone()).to_string())
                    .push_bind(to_datetime_ms(report.received_timestamp))
                    .push_bind(to_datetime(usage.timestamp))
                    .push_bind(PublicKeyBinary::from(usage.payer.clone()).to_string())
                    .push_bind(usage.upload_bytes as i64)
                    .push_bind(usage.download_bytes as i64)
                    .push_bind(req.rewardable_bytes as i64)
                    .push_bind(req.reward_cancelled)
                    .push_bind(usage.event_id);
            })
            .build()
            .execute(db)
            .await?;
        }
        Ok(())
    }
}
