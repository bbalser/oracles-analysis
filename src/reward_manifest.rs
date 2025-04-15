use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_proto::{reward_manifest::RewardData, Message, RewardManifest};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{to_datetime, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeRewardManifest;

#[async_trait::async_trait]
impl Decode for FileTypeRewardManifest {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move { RewardManifest::decode(buf).map_err(anyhow::Error::from) })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeRewardManifest {
    fn to_prefix(&self) -> String {
        FileType::RewardManifest.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeRewardManifest {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS reward_manifests(
                    start_timestamp TIMESTAMPTZ NOT NULL,
                    end_timestamp TIMESTAMPTZ NOT NULL,
                    epoch BIGINT NOT NULL,
                    price BIGINT NOT NULL,
                    token TEXT NOT NULL
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
impl Insertable for Vec<RewardManifest> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 5) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            QueryBuilder::new(
                "INSERT INTO reward_manifests(start_timestamp, end_timestamp, epoch, price, token)",
            )
            .push_values(chunk, |mut b, report| {
                let token = match &report.reward_data {
                    Some(RewardData::MobileRewardData(mobile)) => mobile.token().as_str_name(),
                    Some(RewardData::IotRewardData(iot)) => iot.token().as_str_name(),
                    _ => panic!("Unknown reward data"),
                };

                b.push_bind(to_datetime(report.start_timestamp))
                    .push_bind(to_datetime(report.end_timestamp))
                    .push_bind(report.epoch as i64)
                    .push_bind(report.price as i64)
                    .push_bind(token);
            })
            .build()
            .execute(db)
            .await?;
        }
        Ok(())
    }
}
