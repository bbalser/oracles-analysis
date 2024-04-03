use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_proto::{BoostedHexUpdateV1, Message};
use sqlx::{Pool, Postgres};

use crate::{to_datetime, to_optional_datetime, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeBoostedHexUpdate {}

#[async_trait::async_trait]
impl Decode for FileTypeBoostedHexUpdate {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(
                |buf| async move { BoostedHexUpdateV1::decode(buf).map_err(anyhow::Error::from) },
            )
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
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
impl Insertable for Vec<BoostedHexUpdateV1> {
    async fn insert(&self, db: &Pool<Postgres>) -> anyhow::Result<()> {
        for report in self {
            let update = report.clone().update.unwrap();
            sqlx::query(r#"
                INSERT INTO boosted_hex_updates(location, start_ts, end_ts, period_length, multipliers, version, written_timestamp) values($1,$2,$3,$4,$5,$6,$7)
            "#)
            .bind(update.location as i64)
            .bind(to_optional_datetime(update.start_ts))
            .bind(to_optional_datetime(update.end_ts))
            .bind(update.period_length as i32)
            .bind::<Vec<i32>>(update.multipliers.into_iter().map(|u| u as i32).collect())
            .bind(update.version as i32)
            .bind(to_datetime(report.timestamp))
            .execute(db)
            .await?;
        }
        Ok(())
    }
}
