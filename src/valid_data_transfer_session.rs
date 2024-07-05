use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::{services::packet_verifier::ValidDataTransferSession, Message};
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::{to_datetime_ms, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeValidDataTransferSession;

#[async_trait::async_trait]
impl Decode for FileTypeValidDataTransferSession {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move {
                ValidDataTransferSession::decode(buf).map_err(anyhow::Error::from)
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeValidDataTransferSession {
    fn to_prefix(&self) -> String {
        FileType::ValidDataTransferSession.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeValidDataTransferSession {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS valid_data_transfer_sessions(
                    pub_key text not null,
                    payer text not null,
                    upload_bytes bigint not null,
                    download_bytes bigint not null,
                    num_dcs bigint not null,
                    first_timestamp timestamptz not null,
                    last_timestamp timestamptz not null,
                    rewardable_bytes bigint not null
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
impl Insertable for Vec<ValidDataTransferSession> {
    async fn insert(&self, db: &Pool<Postgres>) -> anyhow::Result<()> {
        const NUM_IN_BATCH: usize = (u16::MAX / 8) as usize;

        for chunk in self.chunks(NUM_IN_BATCH) {
            QueryBuilder::new("INSERT INTO valid_data_transfer_sessions(pub_key, payer, upload_bytes, download_bytes, num_dcs, first_timestamp, last_timestamp, rewardable_bytes)")
            .push_values(chunk, |mut b, report| {
                b.push_bind(PublicKeyBinary::from(report.pub_key.clone()).to_string())
                    .push_bind(PublicKeyBinary::from(report.payer.clone()).to_string())
                    .push_bind(report.upload_bytes as i64)
                    .push_bind(report.download_bytes as i64)
                    .push_bind(report.num_dcs as i64)
                    .push_bind(to_datetime_ms(report.first_timestamp))
                    .push_bind(to_datetime_ms(report.last_timestamp))
                    .push_bind(report.rewardable_bytes as i64);
            })
            .build()
            .execute(db)
            .await?;
        }
        Ok(())
    }
}
