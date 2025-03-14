use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_mobile::{
        service_provider_boosted_rewards_banned_radio_req_v1::KeyType,
        VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    },
    Message,
};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{to_datetime, to_datetime_ms, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeServiceProviderBan;

#[async_trait::async_trait]
impl Decode for FileTypeServiceProviderBan {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1::decode(buf)
                    .map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeServiceProviderBan {
    fn to_prefix(&self) -> String {
        FileType::VerifiedSPBoostedRewardsBannedRadioIngestReport.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeServiceProviderBan {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS service_provider_bans (
                radio_key text not null,
                radio_type text not null,
                reason text not null,
                ban_type text not null,
                until timestamptz not null,
                received_timestamp timestamptz not null,
                status text not null,
                file_timestamp timestamptz not null
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
impl Insertable for Vec<VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 8) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            QueryBuilder::new("INSERT INTO service_provider_bans(radio_key, radio_type, reason, ban_type, until, received_timestamp, status, file_timestamp)")
            .push_values(chunk, |mut b, r| {
                let ingest_report = r.clone().report.unwrap();
                let report = ingest_report.clone().report.unwrap();

                let (radio_key, radio_type) =
                match report.key_type.clone() {
                    Some(KeyType::HotspotKey(hk)) => (PublicKeyBinary::from(hk).to_string(), "wifi"),
                    Some(KeyType::CbsdId(cbsd_id)) => (cbsd_id, "cbrs"),
                    None => panic!("invalid radio"),
                };

                b.push_bind(radio_key)
                    .push_bind(radio_type)
                    .push_bind(report.reason().as_str_name())
                    .push_bind(report.ban_type().as_str_name())
                    .push_bind(to_datetime(report.until))
                    .push_bind(to_datetime_ms(ingest_report.received_timestamp))
                    .push_bind(r.status().as_str_name())
                    .push_bind(file_timestamp);

            })
            .build()
            .execute(db)
            .await?;
        }

        Ok(())
    }
}
