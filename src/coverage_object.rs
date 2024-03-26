use file_store::FileType;
use helium_crypto::PublicKey;
use helium_proto::{
    services::poc_mobile::{coverage_object_req_v1, CoverageObjectV1},
    Message,
};
use sqlx::{Pool, Postgres};

use crate::{to_datetime, DbTable, Decode, Persist, ToPrefix};

#[derive(Clone, Debug)]
pub struct FileTypeCoverageObject {}

impl Decode for FileTypeCoverageObject {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(CoverageObjectV1::decode(buf)?))
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
impl Persist for CoverageObjectV1 {
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        let co = self.coverage_object.unwrap();
        let (radio_key, radio_type) = match co.key_type.unwrap() {
            coverage_object_req_v1::KeyType::CbsdId(cbsd_id) => (cbsd_id, "cbrs"),
            coverage_object_req_v1::KeyType::HotspotKey(bytes) => {
                (PublicKey::try_from(bytes)?.to_string(), "wifi")
            }
        };

        let uuid = uuid::Uuid::from_slice(&co.uuid)?;
        sqlx::query(
            r#"
            INSERT INTO coverage_objects(radio_key, radio_type, uuid, coverage_claim_time, indoor)
            VALUES ($1,$2,$3,$4,$5)
        "#,
        )
        .bind(radio_key)
        .bind(radio_type)
        .bind(uuid)
        .bind(to_datetime(co.coverage_claim_time))
        .bind(co.indoor)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
    }
}
