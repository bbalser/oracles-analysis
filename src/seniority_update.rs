use core::panic;

use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_mobile::{seniority_update, SeniorityUpdate},
    Message,
};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{to_datetime_ms, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeSeniorityUpdate;

impl ToPrefix for FileTypeSeniorityUpdate {
    fn to_prefix(&self) -> String {
        FileType::SeniorityUpdate.to_string()
    }
}

#[async_trait::async_trait]
impl Decode for FileTypeSeniorityUpdate {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(
                |buf| async move { SeniorityUpdate::decode(buf).map_err(anyhow::Error::from) },
            )
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeSeniorityUpdate {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS seniority_updates (
                file_timestamp timestamptz not null,
                radio_type text not null,
                radio_key text not null,
                new_seniority_timestamp timestamptz not null,
                reason text not null
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
impl Insertable for Vec<SeniorityUpdate> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 4) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            QueryBuilder::new("INSERT INTO seniority_updates(file_timestamp,radio_type, radio_key, new_seniority_timestamp, reason)")
            .push_values(chunk, |mut b, report| {

                let (key_type, key_value) = match report.key_type.clone() {
                    Some(seniority_update::KeyType::HotspotKey(pubkey)) => ("wifi", PublicKeyBinary::from(pubkey).to_string()),
                    Some(seniority_update::KeyType::CbsdId(cbsd_id)) => ("cbrs", cbsd_id),
                    _ => panic!("invalid key type"),
                };

                b.push_bind(file_timestamp)
                .push_bind(key_type)
                .push_bind(key_value)
                .push_bind(to_datetime_ms(report.new_seniority_timestamp_ms))
                .push_bind(report.reason().as_str_name());

            })
            .build()
            .execute(db)
            .await?;
        }

        Ok(())
    }
}
