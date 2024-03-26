use file_store::{heartbeat::cli::ValidatedHeartbeat, traits::MsgDecode, FileType};
use helium_crypto::PublicKey;
use sqlx::{Pool, Postgres};

use crate::{DbTable, Decode, Persist, ToPrefix};

#[derive(Debug, Clone)]
pub struct FileTypeValidatedHeartbeat {}

impl Decode for FileTypeValidatedHeartbeat {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(ValidatedHeartbeat::decode(buf)?))
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
                    lon numeric
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
impl Persist for ValidatedHeartbeat {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        if self.cbsd_id.len() == 0 {
            sqlx::query(r#"
                INSERT INTO mobile_validated_heartbeats(hotspot_key, cbsd_id, reward_multiplier, cell_type, validity, location_validation_timestamp, distance_to_asserted, timestamp, location_trust_score_multiplier, lat, lon)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            "#)
            .bind(PublicKey::try_from(self.pub_key)?.to_string())
            .bind(self.cbsd_id)
            .bind(0)
            .bind(self.cell_type.as_str_name())
            .bind(self.validity.as_str_name())
            .bind(self.location_validation_timestamp)
            .bind(self.distance_to_asserted as i64)
            .bind(self.timestamp)
            .bind(self.location_trust_score_multiplier as i64)
            .bind(self.lat)
            .bind(self.lon)
            .execute(pool)
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
        } else {
            Ok(())
        }
    }
}
