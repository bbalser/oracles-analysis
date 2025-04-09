use std::str::FromStr;

use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_mobile::{
        mobile_reward_share, GatewayReward, MobileRewardShare, RadioReward, SubscriberReward,
    },
    Message,
};
use radio_reward_v2::BulkRadioRewardV2;
use rust_decimal::Decimal;
use sqlx::{
    postgres::{PgHasArrayType, PgTypeInfo},
    Pool, Postgres,
};

use crate::{to_datetime, DbTable, Decode, Insertable, ToPrefix};

mod radio_reward_v2;

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
                	transfer_amount int8 NULL
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS mobile_radio_rewards_v2 (
                    id BIGSERIAL PRIMARY KEY,
                	start_period timestamptz NOT NULL,
                	end_period timestamptz NULL,
                	hotspot_key text NOT NULL,
                	cbsd_id text NULL,
                	base_coverage_points_sum numeric NOT NULL,
                	boosted_coverage_points_sum numeric NOT NULL,
                	base_reward_shares numeric NOT NULL,
                	boosted_reward_shares numeric NOT NULL,
                	base_poc_reward int8 NOT NULL,
                	boosted_poc_reward int8 NOT NULL,
                	seniority_ts timestamptz NOT NULL,
                	coverage_object text NOT NULL,
                	location_trust_score_multiplier numeric NOT NULL,
                	speedtest_multiplier numeric NOT NULL,
                	sp_boosted_hex_status text NOT NULL,
                	oracle_boosted_hex_status text NOT NULL
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS location_trust_scores (
                    id bigint NOT NULL,
                    meters_to_asserted int8 NOT NULL,
                    trust_score numeric NOT NULL
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS speedtests (
                    id bigint NOT NULL,
                    upload int8 NOT NULL,
                    download int8 NOT NULL,
                    latency int4 NOT NULL,
                    timestamp timestamptz NOT NULL
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS speedtest_average (
                    id bigint NOT NULL,
                    upload int8 NOT NULL,
                    download int8 NOT NULL,
                    latency int4 NOT NULL,
                    timestamp timestamptz NOT NULL
                )
            "#,
        )
        .execute(db)
        .await?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS covered_hexes (
                    id bigint NOT NULL,
                    location int8 NOT NULL,
                    base_coverage_points numeric NOT NULL,
                    boosted_coverage_points numeric NOT NULL,
                    urbanized text NOT NULL,
                    footfall text NOT NULL,
                    landtype text NOT NULL,
                    assignment_multiplier numeric NOT NULL,
                    rank int4 NOT NULL,
                    rank_multiplier numeric NOT NULL,
                    boosted_multiplier int4 NOT NULL
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
                	rewardable_bytes bigint not null,
                	start_period timestamptz NOT NULL,
                	end_period timestamptz NOT NULL,
                	price int8 NOT NULL,
                	file_timestamp timestamptz
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
                CREATE TABLE IF NOT EXISTS mobile_promotion_rewards (
                    start_period timestamptz not null,
                    end_period timestamptz not null,
                    entity text not null,
                    service_provider_amount int8 not null,
                    matched_amount int8 not null
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
                	disco_amount int8 NOT NULL,
                	verification_amount int8 NOT NULL,
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
    async fn insert(
        &self,
        pool: &Pool<Postgres>,
        file_timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let mut bulk_radio_reward = BulkRadioReward::default();
        let mut bulk_gateway_reward = BulkGatewayReward::default();
        let mut bulk_subscriber_reward = BulkSubscriberReward::default();
        let mut bulk_radio_reward_v2 = BulkRadioRewardV2::default();

        for share in self {
            match share.reward.clone() {
                Some(mobile_reward_share::Reward::RadioReward(radio)) => {
                    bulk_radio_reward.add(
                        to_datetime(share.start_period),
                        to_datetime(share.end_period),
                        radio,
                    );
                }
                Some(mobile_reward_share::Reward::RadioRewardV2(reward)) => {
                    bulk_radio_reward_v2.add(
                        reward,
                        to_datetime(share.start_period),
                        to_datetime(share.end_period)
                    );
                }
                Some(mobile_reward_share::Reward::GatewayReward(gateway)) => {
                    bulk_gateway_reward.add(
                        to_datetime(share.start_period),
                        to_datetime(share.end_period),
                        file_timestamp,
                        gateway,
                    );
                }
                Some(mobile_reward_share::Reward::SubscriberReward(sub)) => {
                    bulk_subscriber_reward.add(
                        to_datetime(share.start_period),
                        to_datetime(share.end_period),
                        sub,
                    );
                }
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
                Some(mobile_reward_share::Reward::PromotionReward(promotion)) => sqlx::
                    query(
                      r#"
                        INSERT INTO mobile_promotion_rewards(start_period, end_period, entity, service_provider_amount, matched_amount)
                        VALUES($1, $2, $3, $4, $5)
                      "#
                      )
                    .bind(to_datetime(share.start_period))
                    .bind(to_datetime(share.end_period))
                    .bind(&promotion.entity)
                    .bind(promotion.service_provider_amount as i64)
                    .bind(promotion.matched_amount as i64)
                    .execute(pool)
                    .await
                    .map(|_| ())?,
                Some(mobile_reward_share::Reward::UnallocatedReward(unallocated)) => {
                    sqlx::query(
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
                    .map(|_| ())?},
                _ => (),
            };
        }

        bulk_radio_reward.insert(pool).await?;
        bulk_radio_reward_v2.insert(pool).await?;
        bulk_gateway_reward.insert(pool).await?;
        bulk_subscriber_reward.insert(pool).await?;
        Ok(())
    }
}

fn from_proto_decimal(opt: Option<&helium_proto::Decimal>) -> Decimal {
    opt.ok_or_else(|| anyhow::anyhow!("decimal not present"))
        .and_then(|d| Decimal::from_str(&d.value).map_err(anyhow::Error::from))
        .unwrap_or_default()
}

#[derive(Default)]
struct BulkRadioReward {
    hotspot_key: Vec<String>,
    cbsd_id: Vec<String>,
    coverage_points: Vec<i64>,
    poc_reward: Vec<i64>,
    start_period: Vec<DateTime<Utc>>,
    end_period: Vec<DateTime<Utc>>,
    dc_transfer_reward: Vec<i64>,
    location_trust_score_multiplier: Vec<i32>,
    speedtest_multiplier: Vec<i32>,
}

impl BulkRadioReward {
    #[allow(deprecated)]
    fn add(&mut self, start_period: DateTime<Utc>, end_period: DateTime<Utc>, radio: RadioReward) {
        self.start_period.push(start_period);
        self.end_period.push(end_period);
        self.hotspot_key.push(
            PublicKeyBinary::try_from(radio.hotspot_key)
                .unwrap()
                .to_string(),
        );
        self.cbsd_id.push(radio.cbsd_id);
        self.coverage_points.push(radio.coverage_points as i64);
        self.poc_reward.push(radio.poc_reward as i64);
        self.dc_transfer_reward
            .push(radio.dc_transfer_reward as i64);
        self.location_trust_score_multiplier
            .push(radio.location_trust_score_multiplier as i32);
        self.speedtest_multiplier
            .push(radio.speedtest_multiplier as i32);
    }

    async fn insert(self, db: &Pool<Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO mobile_radio_rewards(hotspot_key, cbsd_id, coverage_points, amount, start_period, end_period, transfer_amount, location_trust_score_multiplier, speedtest_multiplier)
            SELECT * FROM UNNEST($1,$2,$3,$4,$5,$6,$7,$8,$9)
            "#,
        )
        .bind(self.hotspot_key)
        .bind(self.cbsd_id)
        .bind(self.coverage_points)
        .bind(self.poc_reward)
        .bind(self.start_period)
        .bind(self.end_period)
        .bind(self.dc_transfer_reward)
        .bind(self.location_trust_score_multiplier)
        .bind(self.speedtest_multiplier)
        .execute(db)
        .await?;

        Ok(())
    }
}

#[derive(Default)]
struct BulkGatewayReward {
    hotspot_key: Vec<String>,
    amount: Vec<i64>,
    rewardable_bytes: Vec<i64>,
    start_period: Vec<DateTime<Utc>>,
    end_period: Vec<DateTime<Utc>>,
    price: Vec<i64>,
    file_timestamp: Vec<DateTime<Utc>>,
}

impl BulkGatewayReward {
    fn add(
        &mut self,
        start_period: DateTime<Utc>,
        end_period: DateTime<Utc>,
        file_timestamp: DateTime<Utc>,
        gateway: GatewayReward,
    ) {
        self.start_period.push(start_period);
        self.end_period.push(end_period);
        self.file_timestamp.push(file_timestamp);
        self.hotspot_key.push(
            PublicKeyBinary::try_from(gateway.hotspot_key)
                .unwrap()
                .to_string(),
        );
        self.amount.push(gateway.dc_transfer_reward as i64);
        self.rewardable_bytes.push(gateway.rewardable_bytes as i64);
        self.price.push(gateway.price as i64);
    }

    async fn insert(self, db: &Pool<Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                INSERT INTO mobile_gateway_rewards(hotspot_key, amount, rewardable_bytes, start_period, end_period, price, file_timestamp)
                SELECT * FROM UNNEST($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(self.hotspot_key)
        .bind(self.amount)
        .bind(self.rewardable_bytes)
        .bind(self.start_period)
        .bind(self.end_period)
        .bind(self.price)
        .bind(self.file_timestamp)
        .execute(db)
        .await?;

        Ok(())
    }
}

#[derive(Default)]
struct BulkSubscriberReward {
    subscriber_id: Vec<Vec<u8>>,
    disco_amount: Vec<i64>,
    verification_amount: Vec<i64>,
    start_period: Vec<DateTime<Utc>>,
    end_period: Vec<DateTime<Utc>>,
}

impl BulkSubscriberReward {
    fn add(
        &mut self,
        start_period: DateTime<Utc>,
        end_period: DateTime<Utc>,
        reward: SubscriberReward,
    ) {
        self.subscriber_id.push(reward.subscriber_id);
        self.disco_amount
            .push(reward.discovery_location_amount as i64);
        self.verification_amount
            .push(reward.verification_mapping_amount as i64);
        self.start_period.push(start_period);
        self.end_period.push(end_period);
    }

    async fn insert(self, db: &Pool<Postgres>) -> anyhow::Result<()> {
        sqlx::query(
            r#"
                INSERT INTO mobile_subscriber_rewards(subscriber_id, disco_amount, verification_amount, start_period, end_period)
                SELECT * FROM UNNEST($1, $2, $3, $4, $5)
            "#
        )
        .bind(self.subscriber_id)
        .bind(self.disco_amount)
        .bind(self.verification_amount)
        .bind(self.start_period)
        .bind(self.end_period)
        .execute(db)
        .await?;

        Ok(())
    }
}
