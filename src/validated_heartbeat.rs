use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKey;
use helium_proto::{services::poc_mobile::Heartbeat, Message};
use sqlx::{Pool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{determine_timestamp, DbTable, Decode, Insertable, ToPrefix};

#[derive(Debug, Clone)]
pub struct FileTypeValidatedHeartbeat {}

#[async_trait::async_trait]
impl Decode for FileTypeValidatedHeartbeat {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move { Heartbeat::decode(buf).map_err(anyhow::Error::from) })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeValidatedHeartbeat {
    fn to_prefix(&self) -> String {
        FileType::ValidatedHeartbeat.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeValidatedHeartbeat {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_validated_heartbeats (
                    hotspot_key text not null,
                    cbsd_id text,
                    reward_multiplier numeric not null,
                    cell_type text not null,
                    validity text not null,
                    location_validation_timestamp timestamptz,
                    distance_to_asserted bigint,
                    location_trust_score_multiplier int4,
                    timestamp timestamptz,
                    lat numeric,
                    lon numeric,
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
impl Insertable for Vec<Heartbeat> {
    async fn insert(
        &self,
        pool: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 12) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            let mut qb = QueryBuilder::new("INSERT INTO mobile_validated_heartbeats(hotspot_key, cbsd_id, reward_multiplier, cell_type, validity, location_validation_timestamp, distance_to_asserted, timestamp, location_trust_score_multiplier, lat, lon, coverage_object)");

            qb.push_values(chunk, |mut b, hb| {
                b.push_bind(PublicKey::try_from(hb.pub_key.clone()).unwrap().to_string())
                    .push_bind(&hb.cbsd_id)
                    .push_bind(0)
                    .push_bind(hb.cell_type().as_str_name())
                    .push_bind(hb.validity().as_str_name())
                    .push_bind(determine_timestamp(hb.location_validation_timestamp))
                    .push_bind(hb.distance_to_asserted as i64)
                    .push_bind(determine_timestamp(hb.timestamp))
                    .push_bind(hb.location_trust_score_multiplier as i64)
                    .push_bind(hb.lat)
                    .push_bind(hb.lon)
                    .push_bind(
                        Uuid::from_slice(hb.coverage_object.as_slice())
                            .map(|u| u.to_string())
                            .unwrap_or("invalid".to_string()),
                    );
            })
            .build()
            .execute(pool)
            .await?;
        }

        Ok(())
    }
}
