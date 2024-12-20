use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKey;
use helium_proto::{
    services::poc_mobile::{coverage_object_req_v1, CoverageObjectV1},
    Message,
};
use sqlx::{Pool, Postgres};

use crate::{to_datetime, DbTable, Decode, Insertable, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeCoverageObject {}

#[async_trait::async_trait]
impl Decode for FileTypeCoverageObject {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(
                |buf| async move { CoverageObjectV1::decode(buf).map_err(anyhow::Error::from) },
            )
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeCoverageObject {
    fn to_prefix(&self) -> String {
        FileType::CoverageObject.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeCoverageObject {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS coverage_objects (
                    file_timestamp timestamptz not null,
                    radio_key text not null,
                    radio_type text not null,
                    uuid text not null,
                    coverage_claim_time timestamptz not null,
                    indoor bool not null
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
impl Insertable for Vec<CoverageObjectV1> {
    async fn insert(
        &self,
        db: &Pool<Postgres>,
        file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        for object in self {
            let co = object.coverage_object.clone().unwrap();
            let (radio_key, radio_type) = match co.key_type.unwrap() {
                coverage_object_req_v1::KeyType::CbsdId(cbsd_id) => (cbsd_id, "cbrs"),
                coverage_object_req_v1::KeyType::HotspotKey(bytes) => {
                    (PublicKey::try_from(bytes)?.to_string(), "wifi")
                }
            };

            let uuid = uuid::Uuid::from_slice(&co.uuid)?;
            sqlx::query(
                r#"
                INSERT INTO coverage_objects(file_timestamp,radio_key, radio_type, uuid, coverage_claim_time, indoor)
                VALUES ($1,$2,$3,$4,$5,$6)
                "#,
            )
            .bind(file_timestamp)
            .bind(radio_key)
            .bind(radio_type)
            .bind(uuid)
            .bind(to_datetime(co.coverage_claim_time))
            .bind(co.indoor)
            .execute(db)
            .await?;
        }
        Ok(())
    }
}
