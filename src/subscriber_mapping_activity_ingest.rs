use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_proto::{services::poc_mobile::SubscriberMappingActivityIngestReportV1, Message};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{determine_timestamp, DbTable, Decode, Insertable, ToPrefix};

#[derive(Debug, Clone)]
pub struct FileTypeSubscriberMappingActivityIngest;

#[async_trait::async_trait]
impl Decode for FileTypeSubscriberMappingActivityIngest {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                SubscriberMappingActivityIngestReportV1::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeSubscriberMappingActivityIngest {
    fn to_prefix(&self) -> String {
        FileType::SubscriberMappingActivityIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeSubscriberMappingActivityIngest {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS subscriber_mapping_activity_ingest (
                subscriber_id bytea not null,
                discovery_reward_shares bigint not null,
                verification_reward_shares bigint not null,
                timestamp timestamptz not null,
                received_timestamp timestamptz not null
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
impl Insertable for Vec<SubscriberMappingActivityIngestReportV1> {
    async fn insert(
        &self,
        pool: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 5) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            QueryBuilder::new("INSERT INTO subscriber_mapping_activity_ingest (subscriber_id, discovery_reward_shares, verification_reward_shares, timestamp, received_timestamp)")
            .push_values(chunk, |mut b, ingest| {

                let req = ingest.report.as_ref().unwrap().clone();

                b.push_bind(req.subscriber_id)
                    .push_bind(req.discovery_reward_shares as i64)
                    .push_bind(req.verification_reward_shares as i64)
                    .push_bind(determine_timestamp(req.timestamp))
                    .push_bind(determine_timestamp(ingest.received_timestamp));

                
            })
            .build()
            .execute(pool)
            .await?;
        }

        Ok(())
    }
}

