use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKey;
use helium_proto::{
    services::poc_mobile::{mobile_reward_share, MobileRewardShare},
    Message,
};
use sqlx::{
    postgres::{PgHasArrayType, PgTypeInfo},
    Pool, Postgres,
};

use crate::{to_datetime, DbTable, Decode, Insertable, ToPrefix};

#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(type_name = "boosted_hex")]
struct BoostedHex {
    location: i64,
    multiplier: i32,
}

impl PgHasArrayType for BoostedHex {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_boosted_hex")
    }
}

#[derive(Debug, Clone)]
pub struct FileTypeMobileRewardShare {}

#[async_trait::async_trait]
impl Decode for FileTypeMobileRewardShare {
    async fn decode(&self, stream: BytesMutStream) -> anyhow::Result<Box<dyn Insertable>> {
        let reports = stream
            .map_err(anyhow::Error::from)
            .and_then(
                |buf| async move { MobileRewardShare::decode(buf).map_err(anyhow::Error::from) },
            )
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Box::new(reports))
    }
}

impl ToPrefix for FileTypeMobileRewardShare {
    fn to_prefix(&self) -> String {
        FileType::MobileRewardShare.to_string()
    }
}

#[async_trait::async_trait]
impl DbTable for FileTypeMobileRewardShare {
    async fn create_table(&self, db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            DO $$ BEGIN
                CREATE TYPE boosted_hex AS (
                    location bigint,
                	multiplier int
                );
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_radio_rewards (
                	hotspot_key text NOT NULL,
                	cbsd_id text NULL,
                	coverage_points int8 NOT NULL,
                	amount int8 NOT NULL,
                	start_period timestamptz NOT NULL,
                	end_period timestamptz NULL,
                	location_trust_score_multiplier int4 NOT NULL,
                	speedtest_multiplier int4 NOT NULL,
                	transfer_amount int8 NULL,
                	boosted_hexes boosted_hex[] NOT NULL
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_gateway_rewards (
                	hotspot_key text NOT NULL,
                	amount int8 NOT NULL,
                	start_period timestamptz NOT NULL,
                	end_period timestamptz NOT NULL
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_service_provider_rewards (
                    service_provider text not null,
                    amount bigint not null,
                    start_period timestamptz not null,
                    end_period timestamptz not null
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_unallocated_rewards (
                    reward_type text not null,
                    amount bigint not null,
                    start_period timestamptz not null,
                    end_period timestamptz not null
                )
        "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_subscriber_rewards (
                	subscriber_id bytea NOT NULL,
                	amount int8 NOT NULL,
                	start_period timestamptz NOT NULL,
                	end_period timestamptz NOT NULL
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
impl Insertable for Vec<MobileRewardShare> {
    #[allow(deprecated)]
    async fn insert(&self, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        for share in self {
            match share.reward.clone() {
            Some(mobile_reward_share::Reward::RadioReward(radio)) => {
                let boosted_hexes: Vec<BoostedHex> = radio.boosted_hexes.into_iter()
                    .map(|h| BoostedHex {
                        location: h.location as i64,
                        multiplier: h.multiplier as i32,
                    }).collect();

                sqlx::query(
                    r#"
                        INSERT INTO mobile_radio_rewards(hotspot_key, cbsd_id, coverage_points, amount, start_period, end_period, transfer_amount, boosted_hexes, location_trust_score_multiplier, speedtest_multiplier)
                        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    "#,
                )
                .bind(PublicKey::try_from(radio.hotspot_key)?.to_string())
                .bind(radio.cbsd_id)
                .bind(radio.coverage_points as i64)
                .bind(radio.poc_reward as i64)
                .bind(to_datetime(share.start_period))
                .bind(to_datetime(share.end_period))
                .bind(radio.dc_transfer_reward as i64)
                .bind(boosted_hexes)
                .bind(radio.location_trust_score_multiplier as i32)
                .bind(radio.speedtest_multiplier as i32)
                .execute(pool)
                .await
                .map(|_| ())?
            }
            Some(mobile_reward_share::Reward::GatewayReward(gateway)) => sqlx::query(
                    r#"
                        INSERT INTO mobile_gateway_rewards(hotspot_key, amount, start_period, end_period)
                        VALUES($1, $2, $3, $4)
                    "#,
                )
                .bind(PublicKey::try_from(gateway.hotspot_key)?.to_string())
                .bind(gateway.dc_transfer_reward as i64)
                .bind(to_datetime(share.start_period))
                .bind(to_datetime(share.end_period))
                .execute(pool)
                .await
                .map(|_| ())?,
            Some(mobile_reward_share::Reward::SubscriberReward(sub)) => sqlx::query(
                    r#"
                        INSERT INTO mobile_subscriber_rewards(subscriber_id, amount, start_period, end_period)
                        VALUES($1,$2,$3,$4)
                    "#
                )
                .bind(sub.subscriber_id)
                .bind(sub.discovery_location_amount as i64)
                .bind(to_datetime(share.start_period))
                .bind(to_datetime(share.end_period))
                .execute(pool)
                .await
                .map(|_| ())?,
            Some(mobile_reward_share::Reward::ServiceProviderReward(service)) => sqlx::query(
                r#"
                    INSERT INTO mobile_service_provider_rewards(service_provider, amount, start_period, end_period) VALUES($1,$2,$3,$4)
                "#
                )
                .bind(service.service_provider_id().as_str_name())
                .bind(service.amount as i64)
                .bind(to_datetime(share.start_period))
                .bind(to_datetime(share.end_period))
                .execute(pool)
                .await
                .map(|_| ())?,
            Some(mobile_reward_share::Reward::UnallocatedReward(unallocated)) => sqlx::query(
                r#"
                    INSERT INTO mobile_unallocated_rewards(reward_type, amount, start_period, end_period) VALUES($1,$2,$3,$4)
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
