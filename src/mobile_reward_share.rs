use file_store::FileType;
use helium_crypto::PublicKey;
use helium_proto::{services::poc_mobile::{MobileRewardShare, mobile_reward_share}, Message};
use sqlx::{postgres::{PgHasArrayType, PgTypeInfo}, Pool, Postgres};

use crate::{to_datetime, DbTable, Decode, Persist, ToPrefix};

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

impl Decode for FileTypeMobileRewardShare {
    fn decode(&self, buf: bytes::BytesMut) -> anyhow::Result<Box<dyn Persist>> {
        Ok(Box::new(MobileRewardShare::decode(buf)?))
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
        sqlx::query(r#"
            DO $$ BEGIN
                CREATE TYPE boosted_hex AS (
                    location bigint,
                	multiplier int
                );
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
            "#)
        .execute(db)
        .await?;

        sqlx::query(r#"
                CREATE TABLE IF NOT EXISTS mobile_radio_rewards (
                	hotspot_key text NOT NULL,
                	cbsd_id text NULL,
                	amount int8 NOT NULL,
                	start_period timestamptz NOT NULL,
                	end_period timestamptz NULL,
                	transfer_amount int8 NULL,
                	boosted_hexes boosted_hex[] NOT NULL
                )
            "#)
        .execute(db)
        .await?;

        sqlx::query(r#"
                CREATE TABLE IF NOT EXISTS mobile_gateway_rewards (
                	hotspot_key text NOT NULL,
                	amount int8 NOT NULL,
                	start_period timestamptz NOT NULL,
                	end_period timestamptz NOT NULL
                )
            "#)
        .execute(db)
        .await?;

        sqlx::query(r#"
                CREATE TABLE IF NOT EXISTS mobile_service_provider_rewards (
                    service_provider text not null,
                    amount bigint not null,
                    start_period timestamptz not null,
                    end_period timestamptz not null
                )
            "#)
        .execute(db)
        .await?;

        sqlx::query(r#"
                CREATE TABLE IF NOT EXISTS mobile_unallocated_rewards (
                    reward_type text not null,
                    amount bigint not null,
                    start_period timestamptz not null,
                    end_period timestamptz not null
                )
        "#)
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
impl Persist for MobileRewardShare {
    #[allow(deprecated)]
    async fn save(self: Box<Self>, pool: &Pool<Postgres>) -> anyhow::Result<()> {
        match self.reward {
            Some(mobile_reward_share::Reward::RadioReward(radio)) => {
                let boosted_hexes: Vec<BoostedHex> = radio.boosted_hexes.into_iter()
                    .map(|h| BoostedHex {
                        location: h.location as i64,
                        multiplier: h.multiplier as i32,
                    }).collect();
                
                sqlx::query(
                    r#"
                        INSERT INTO mobile_radio_rewards(hotspot_key, cbsd_id, amount, start_period, end_period, transfer_amount, boosted_hexes)
                        VALUES($1,$2,$3,$4,$5,$6,$7)
                    "#,
                )
                .bind(PublicKey::try_from(radio.hotspot_key)?.to_string())
                .bind(radio.cbsd_id)
                .bind(radio.poc_reward as i64)
                .bind(to_datetime(self.start_period))
                .bind(to_datetime(self.end_period))
                .bind(radio.dc_transfer_reward as i64)
                .bind(boosted_hexes)
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
                .bind(to_datetime(self.start_period))
                .bind(to_datetime(self.end_period))
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
                .bind(to_datetime(self.start_period))
                .bind(to_datetime(self.end_period))
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
                .bind(to_datetime(self.start_period))
                .bind(to_datetime(self.end_period))
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
                .bind(to_datetime(self.start_period))
                .bind(to_datetime(self.end_period))
                .execute(pool)
                .await
                .map(|_| ())?,
            _ => (),
        };
        Ok(())
    }
}
