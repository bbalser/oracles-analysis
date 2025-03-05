use std::str::FromStr;

use chrono::{DateTime, Utc};
use file_store::{heartbeat::cli::ValidatedHeartbeat, traits::MsgDecode, BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::{PublicKey, PublicKeyBinary};
use sqlx::{Pool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{DbTable, Decode, Insertable, ToPrefix};

#[derive(Debug, Clone)]
pub struct FileTypeValidatedHeartbeat {}

#[async_trait::async_trait]
impl Decode for FileTypeValidatedHeartbeat {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(
                |buf| async move { ValidatedHeartbeat::decode(buf).map_err(anyhow::Error::from) },
            )
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
impl Insertable for Vec<ValidatedHeartbeat> {
    async fn insert(
        &self,
        pool: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 11) as usize;

        let pubkey = PublicKeyBinary::from_str("1trSusey7ycXX5i5sB2xLPEsdecG5quC6n18aKkG8V73s62CkArwF2Wqgr7WpbzSsnouyJJr5wSFw13zjos7Kq56ZgujDtkieMAxqLkRBBDGbFYyXjbEVKdquuvBdKwWZ5PaSZJyhYgrWSpPAg1gRa8x3mX5s425k4d2RDb7xf3n7sbuLpDfAwJJtuCUFeG2NHBE7Na52u4YsA6EvZtCq3jZXK1MfWLRFm9c4cXD1TwvfDVg1z2MzgWHuqG2MdEp1TWfU3Lgs6DTxKoUzYaBWiEX4e1onjYA4N4ERBwn7rGZEdrESC483T6KpALAdUpfYEh7XTUPwk6xmcNY2MzuW6yeRzxD4AN34rDEvYTFCwhpjs")?;

        let hbs: Vec<&ValidatedHeartbeat> = self.iter().filter(|hb| hb.pub_key == pubkey).collect();

        for chunk in hbs.chunks(NUM_IN_BATCH) {
            let mut qb = QueryBuilder::new("INSERT INTO mobile_validated_heartbeats(hotspot_key, cbsd_id, reward_multiplier, cell_type, validity, location_validation_timestamp, distance_to_asserted, timestamp, location_trust_score_multiplier, lat, lon, coverage_object)");

            qb.push_values(chunk, |mut b, hb| {
                b.push_bind(PublicKey::try_from(hb.pub_key.clone()).unwrap().to_string())
                    .push_bind(&hb.cbsd_id)
                    .push_bind(0)
                    .push_bind(hb.cell_type.as_str_name())
                    .push_bind(hb.validity.as_str_name())
                    .push_bind(hb.location_validation_timestamp)
                    .push_bind(hb.distance_to_asserted as i64)
                    .push_bind(hb.timestamp)
                    .push_bind(hb.location_trust_score_multiplier as i64)
                    .push_bind(hb.lat)
                    .push_bind(hb.lon)
                    .push_bind(
                        Uuid::from_slice(hb.coverage_object.as_slice())
                            .expect("inavlid uuid")
                            .to_string(),
                    );
            })
            .build()
            .execute(pool)
            .await?;
        }

        Ok(())
    }
}
