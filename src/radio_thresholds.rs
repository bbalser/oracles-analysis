use file_store::FileType;
use helium_crypto::PublicKey;
use helium_proto::{
    services::poc_mobile::{
        VerifiedInvalidatedRadioThresholdIngestReportV1, VerifiedRadioThresholdIngestReportV1,
    },
    Message,
};
use sqlx::{Pool, Postgres};

use crate::{to_datetime, to_datetime_ms, DbTable, Decode, Persist, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeRadioThreshold {}

impl Decode for FileTypeRadioThreshold {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(
            VerifiedInvalidatedRadioThresholdIngestReportV1::decode(buf)?,
        ))
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
impl Persist for VerifiedRadioThresholdIngestReportV1 {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        let report = self.clone().report.unwrap().report.unwrap();
        let pubkey = PublicKey::try_from(report.hotspot_pubkey)?.to_string();
        let pubkey_bytes = bs58::decode(&pubkey).into_vec()?;
        sqlx::query(r#"
                INSERT INTO radio_thresholds(received_timestamp, status, hotspot_key, cbsd_id, validated, threshold_timestamp, pubkey_bytes)
                VALUES($1,$2,$3,$4,$5,$6,$7)
            "#)
            .bind(to_datetime_ms(self.clone().report.unwrap().received_timestamp))
            .bind(self.status().as_str_name())
            .bind(pubkey)
            .bind(report.cbsd_id)
            .bind(true)
            .bind(to_datetime(report.threshold_timestamp))
            .bind(pubkey_bytes)
            .execute(pool)
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }
}

#[derive(Clone, Debug)]
pub struct FileTypeInvalidatedRadioThreshold {}

impl Decode for FileTypeInvalidatedRadioThreshold {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(
            VerifiedInvalidatedRadioThresholdIngestReportV1::decode(buf)?,
        ))
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
impl Persist for VerifiedInvalidatedRadioThresholdIngestReportV1 {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        let report = self.clone().report.unwrap().report.unwrap();
        let pubkey = PublicKey::try_from(report.hotspot_pubkey)?.to_string();
        let pubkey_bytes = bs58::decode(&pubkey).into_vec()?;
        sqlx::query(r#"
                INSERT INTO radio_thresholds(received_timestamp, status, hotspot_key, cbsd_id, validated, threshold_timestamp, pubkey_bytes)
                VALUES($1,$2,$3,$4,$5,$6,$7)
            "#)
            .bind(to_datetime_ms(self.clone().report.unwrap().received_timestamp))
            .bind(self.status().as_str_name())
            .bind(pubkey)
            .bind(report.cbsd_id)
            .bind(false)
            .bind(to_datetime_ms(report.timestamp))
            .bind(pubkey_bytes)
            .execute(pool)
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }
}
