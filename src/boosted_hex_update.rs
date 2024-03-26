use file_store::FileType;
use helium_proto::{BoostedHexUpdateV1, Message};
use sqlx::{Pool, Postgres};

use crate::{to_datetime, to_optional_datetime, DbTable, Decode, Persist, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeBoostedHexUpdate {}

impl Decode for FileTypeBoostedHexUpdate {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(BoostedHexUpdateV1::decode(buf)?))
    }
}

impl ToPrefix for FileTypeBoostedHexUpdate {
    fn to_prefix(&self) -> String {
        FileType::BoostedHexUpdate.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeBoostedHexUpdate {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS boosted_hex_updates (
                    location bigint not null,
                    start_ts timestamptz,
                    end_ts timestamptz,
                    period_length int,
                    multipliers integer[],
                    version int,
                    written_timestamp timestamptz not null
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
impl Persist for BoostedHexUpdateV1 {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        let update = self.update.unwrap();
        sqlx::query(r#"
            INSERT INTO boosted_hex_updates(location, start_ts, end_ts, period_length, multipliers, version, written_timestamp) values($1,$2,$3,$4,$5,$6,$7)
        "#)
        .bind(update.location as i64)
        .bind(to_optional_datetime(update.start_ts))
        .bind(to_optional_datetime(update.end_ts))
        .bind(update.period_length as i32)
        .bind::<Vec<i32>>(update.multipliers.into_iter().map(|u| u as i32).collect())
        .bind(update.version as i32)
        .bind(to_datetime(self.timestamp))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }
}
