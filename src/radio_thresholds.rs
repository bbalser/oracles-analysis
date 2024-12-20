use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKey;
use helium_proto::{
    services::poc_mobile::{
        VerifiedInvalidatedRadioThresholdIngestReportV1, VerifiedRadioThresholdIngestReportV1,
    },
    Message,
};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{to_datetime, to_datetime_ms, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeRadioThreshold {}

#[async_trait::async_trait]
impl Decode for FileTypeRadioThreshold {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                VerifiedRadioThresholdIngestReportV1::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeRadioThreshold {
    fn to_prefix(&self) -> String {
        FileType::VerifiedRadioThresholdIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeRadioThreshold {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS radio_thresholds (
                    received_timestamp timestamptz not null,
                    status text not null,
                    hotspot_key text not null,
                    cbsd_id text,
                    validated bool not null,
                    threshold_timestamp timestamptz
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
impl Insertable for Vec<VerifiedRadioThresholdIngestReportV1> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 6) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            let mut qb = QueryBuilder::new("INSERT INTO radio_thresholds(received_timestamp, status, hotspot_key, cbsd_id, validated, threshold_timestamp)");

            qb.push_values(chunk, |mut b, top_report| {
                let report = top_report.clone().report.unwrap().report.unwrap();
                let pubkey = PublicKey::try_from(report.hotspot_pubkey)
                    .unwrap()
                    .to_string();

                b.push_bind(to_datetime_ms(
                    top_report.clone().report.unwrap().received_timestamp,
                ))
                .push_bind(top_report.status().as_str_name())
                .push_bind(pubkey)
                .push_bind(report.cbsd_id)
                .push_bind(true)
                .push_bind(to_datetime(report.threshold_timestamp));
            })
            .build()
            .execute(db)
            .await?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FileTypeInvalidatedRadioThreshold {}

#[async_trait::async_trait]
impl Decode for FileTypeInvalidatedRadioThreshold {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                VerifiedInvalidatedRadioThresholdIngestReportV1::decode(buf)
                    .map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeInvalidatedRadioThreshold {
    fn to_prefix(&self) -> String {
        FileType::VerifiedInvalidatedRadioThresholdIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeInvalidatedRadioThreshold {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS radio_thresholds (
                    received_timestamp timestamptz not null,
                    status text not null,
                    hotspot_key text not null,
                    cbsd_id text,
                    validated bool not null,
                    threshold_timestamp timestamptz
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
impl Insertable for Vec<VerifiedInvalidatedRadioThresholdIngestReportV1> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 6) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            let mut qb = QueryBuilder::new("INSERT INTO radio_thresholds(received_timestamp, status, hotspot_key, cbsd_id, validated, threshold_timestamp)");

            qb.push_values(chunk, |mut b, top_report| {
                let report = top_report.clone().report.unwrap().report.unwrap();
                let pubkey = PublicKey::try_from(report.hotspot_pubkey)
                    .unwrap()
                    .to_string();

                b.push_bind(to_datetime_ms(
                    top_report.clone().report.unwrap().received_timestamp,
                ))
                .push_bind(top_report.status().as_str_name())
                .push_bind(pubkey)
                .push_bind(report.cbsd_id)
                .push_bind(false)
                .push_bind(to_datetime(report.timestamp));
            })
            .build()
            .execute(db)
            .await?;
        }

        Ok(())
    }
}
