use file_store::FileType;
use helium_proto::{services::poc_mobile::OracleBoostingReportV1, Message};
use sqlx::{Pool, Postgres};

use crate::{to_datetime, DbTable, Decode, Persist, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeOracleBoostingReport {}

impl Decode for FileTypeOracleBoostingReport {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(OracleBoostingReportV1::decode(buf)?))
    }
}

impl ToPrefix for FileTypeOracleBoostingReport {
    fn to_prefix(&self) -> String {
        FileType::OracleBoostingReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeOracleBoostingReport {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS oracle_boosting (
                    coverage_object text not null,
                    timestamp timestamptz not null,
                    location bigint not null,
                    urbanized text not null,
                    multiplier int not null
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
impl Persist for OracleBoostingReportV1 {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        let uuid = uuid::Uuid::from_slice(&self.coverage_object)?;
        let timestamp = to_datetime(self.timestamp);

        for hex in self.assignments {
            sqlx::query(r#"
                INSERT INTO oracle_boosting(coverage_object, timestamp, location, urbanized, multiplier)
                VALUES($1,$2,$3,$4,$5)
            "#,
            )
            .bind(uuid)
            .bind(timestamp)
            .bind(i64::from_str_radix(&hex.location, 16)?)
            .bind(hex.urbanized().as_str_name())
            .bind(hex.assignment_multiplier as i32)
            .execute(pool)
            .await
            .map_err(anyhow::Error::from)?;
        }

        Ok(())
    }
}
