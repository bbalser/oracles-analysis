use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_proto::{services::poc_mobile::OracleBoostingReportV1, Message};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{to_datetime, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeOracleBoostingReport {}

#[async_trait::async_trait]
impl Decode for FileTypeOracleBoostingReport {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                OracleBoostingReportV1::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
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
impl Insertable for Vec<OracleBoostingReportV1> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 5) as usize;

        let rows: Vec<_> = self
            .iter()
            .flat_map(|report| {
                let uuid = uuid::Uuid::from_slice(&report.coverage_object).unwrap();
                let timestamp = to_datetime(report.timestamp);

                report
                    .assignments
                    .iter()
                    .map(move |assignment| (uuid, timestamp, assignment))
            })
            .collect();

        for chunk in rows.chunks(NUM_IN_BATCH) {
            let mut qb = QueryBuilder::new("INSERT INTO oracle_boosting(coverage_object, timestamp, location, urbanized, multiplier)");

            qb.push_values(chunk, |mut b, (uuid, timestamp, hex)| {
                b.push_bind(uuid)
                    .push_bind(timestamp)
                    .push_bind(i64::from_str_radix(&hex.location, 16).unwrap())
                    .push_bind(hex.urbanized().as_str_name())
                    .push_bind(hex.assignment_multiplier as i32);
            })
            .build()
            .execute(db)
            .await?;
        }

        Ok(())
    }
}
