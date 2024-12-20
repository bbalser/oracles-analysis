use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKey;
use helium_proto::{
    services::poc_lora::{iot_reward_share, IotRewardShare},
    Message,
};
use sqlx::{Pool, Postgres};

use crate::{to_datetime, DbTable, Decode, Insertable, ToPrefix};

#[derive(Debug, Clone)]
pub struct FileTypeIotRewardShare {}

#[async_trait::async_trait]
impl Decode for FileTypeIotRewardShare {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(|buf| async move { IotRewardShare::decode(buf).map_err(anyhow::Error::from) })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeIotRewardShare {
    fn to_prefix(&self) -> String {
        FileType::IotRewardShare.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeIotRewardShare {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS iot_gateway_rewards (
                    hotspot_key text not null,
                    beacon_amount bigint not null,
                    witness_amount bigint not null,
                    dc_transfer_amount bigint not null,
                    start_period timestamptz not null,
                    end_period timestamptz not null
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS iot_other_rewards (
                    reward_type text not null,
                    amount bigint not null,
                    start_period timestamptz not null,
                    end_period timestamptz not null
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
impl Insertable for Vec<IotRewardShare> {
    #[allow(deprecated)]
    async fn insert(
        &self,
        pool: &Pool<Postgres>,
        _file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        for share in self {
            match &share.reward {
            Some(iot_reward_share::Reward::GatewayReward(gateway)) => sqlx::query(
                r#"
                    INSERT INTO iot_gateway_rewards(hotspot_key, beacon_amount, witness_amount, dc_transfer_amount, start_period, end_period)
                    VALUES($1,$2,$3,$4,$5,$6)
                "#
                )
                .bind(PublicKey::try_from(gateway.hotspot_key.clone())?.to_string())
                .bind(gateway.beacon_amount as i64)
                .bind(gateway.witness_amount as i64)
                .bind(gateway.dc_transfer_amount as i64)
                .bind(to_datetime(share.start_period))
                .bind(to_datetime(share.end_period))
                .execute(pool)
                .await
                .map(|_| ())?,
            Some(iot_reward_share::Reward::OperationalReward(operational)) => sqlx::query(
                r#"
                    INSERT INTO iot_other_rewards(reward_type, amount, start_period, end_period)
                    VALUES($1,$2,$3,$4)
                "#
                )
                .bind("operational")
                .bind(operational.amount as i64)
                .bind(to_datetime(share.start_period))
                .bind(to_datetime(share.end_period))
                .execute(pool)
                .await
                .map(|_| ())?,
            Some(iot_reward_share::Reward::UnallocatedReward(unallocated)) => sqlx::query(
                r#"
                    INSERT INTO iot_other_rewards(reward_type, amount, start_period, end_period)
                    VALUES($1,$2,$3,$4)
                "#
                )
                .bind(unallocated.reward_type().as_str_name())
                .bind(unallocated.amount as i64)
                .bind(to_datetime(share.start_period))
                .bind(to_datetime(share.end_period))
                .execute(pool)
                .await
                .map(|_| ())?,
            _ => (),
        };
        }
        Ok(())
    }
}
