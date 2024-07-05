use chrono::{DateTime, Utc};
use file_store::{BytesMutStream, FileType};
use futures::TryStreamExt;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto::{
    services::poc_mobile::{self as proto, mobile_reward_share, MobileRewardShare},
    Message,
};
use sqlx::{
    postgres::{PgHasArrayType, PgTypeInfo}, Pool, Postgres, QueryBuilder, Row
};
use uuid::Uuid;

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
                CREATE TABLE IF NOT EXISTS mobile_radio_rewards_v2 (
                    id BIGSERIAL PRIMARY KEY,
                	start_period timestamptz NOT NULL,
                	end_period timestamptz NULL,
                	hotspot_key text NOT NULL,
                	cbsd_id text NULL,
                	base_coverage_points int8 NOT NULL,
                	boosted_coverage_points int8 NOT NULL,
                	base_reward_shares int8 NOT NULL,
                	boosted_reward_shares int8 NOT NULL,
                	base_poc_reward int8 NOT NULL,
                	boosted_poc_reward int8 NOT NULL,
                	seniority_ts timestamptz NOT NULL,
                	coverage_object text NOT NULL,
                	location_trust_score_multiplier int4 NOT NULL,
                	speedtest_multiplier int4 NOT NULL,
                	boosted_hex_status text NOT NULL
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
                    trust_score int4 NOT NULL
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
                CREATE TABLE IF NOT EXISTS covered_hexes (
                    id bigint NOT NULL,
                    location int8 NOT NULL,
                    base_coverage_points int8 NOT NULL,
                    boosted_coverage_points int8 NOT NULL,
                    urbanized text NOT NULL,
                    footfall text NOT NULL,
                    landtype text NOT NULL,
                    assignment_multiplier int4 NOT NULL,
                    rank int4 NOT NULL,
                    rank_multiplier int4 NOT NULL,
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
            Some(mobile_reward_share::Reward::RadioRewardV2(reward)) => {
                insert_radio_reward_v2(pool, reward, to_datetime(share.start_period), to_datetime(share.end_period)).await?
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

async fn insert_radio_reward_v2(
    pool: &Pool<Postgres>,
    reward: proto::RadioRewardV2,
    start_period: DateTime<Utc>,
    end_period: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut transaction = pool.begin().await?;

    let row = 
    sqlx::query(r#"
        INSERT INTO mobile_radio_rewards_v2(start_period, end_period, hotspot_key, cbsd_id, base_coverage_points, boosted_coverage_points, base_reward_shares, boosted_reward_shares, base_poc_reward, boosted_poc_reward, seniority_ts, coverage_object, location_trust_score_multiplier, speedtest_multiplier, boosted_hex_status)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        RETURNING id
        "#)
        .bind(start_period)
        .bind(end_period)
        .bind(PublicKeyBinary::from(reward.hotspot_key.as_slice()).to_string())
        .bind(&reward.cbsd_id)
        .bind(reward.base_coverage_points as i64)
        .bind(reward.boosted_coverage_points as i64)
        .bind(reward.base_reward_shares as i64)
        .bind(reward.boosted_reward_shares as i64)
        .bind(reward.base_poc_reward as i64)
        .bind(reward.boosted_poc_reward as i64)
        .bind(to_datetime(reward.seniority_timestamp))
        .bind(Uuid::from_slice(reward.coverage_object.as_slice())?)
        .bind(reward.location_trust_score_multiplier as i64)
        .bind(reward.speedtest_multiplier as i64)
        .bind(reward.boosted_hex_status().as_str_name())
        .fetch_one(&mut transaction)
        .await?;

    let id = row.get::<i64, &str>("id");

    let num_in_batch: usize = (u16::MAX / 3) as usize;
    for chunk in reward.location_trust_scores.chunks(num_in_batch) {
        QueryBuilder::new(r#"
            INSERT INTO location_trust_scores(id, meters_to_asserted, trust_score)
            "#)
            .push_values(chunk, |mut b, lt| {
                 b.push_bind(id)
                .push_bind(lt.meters_to_asserted as i64)
                .push_bind(lt.trust_score as i64);
            })
            .build()
            .execute(&mut transaction)
            .await?;
    }

    let num_in_batch: usize = (u16::MAX / 5) as usize;
    for chunk in reward.speedtests.chunks(num_in_batch) {
        QueryBuilder::new(r#"
            INSERT INTO speedtests(id, upload, download, latency, timestamp)
            "#)
            .push_values(chunk, |mut b, st| {
                b.push_bind(id)
                .push_bind(st.upload_speed_bps as i64)
                .push_bind(st.download_speed_bps as i64)
                .push_bind(st.latency_ms as i32)
                .push_bind(to_datetime(st.timestamp));
            })
            .build()
            .execute(&mut transaction)
            .await?;
    }

    let num_in_batch: usize = (u16::MAX / 11) as usize;
    for chunk in reward.covered_hexes.chunks(num_in_batch) {
        QueryBuilder::new(r#"
            INSERT INTO covered_hexes(id, location, base_coverage_points, boosted_coverage_points, urbanized, footfall, landtype, assignment_multiplier, rank, rank_multiplier, boosted_multiplier)
            "#)
            .push_values(chunk, |mut b, h| {
                b.push_bind(id)
                .push_bind(h.location as i64)
                .push_bind(h.base_coverage_points as i64)
                .push_bind(h.boosted_coverage_points as i64)
                .push_bind(h.urbanized().as_str_name())
                .push_bind(h.footfall().as_str_name())
                .push_bind(h.landtype().as_str_name())
                .push_bind(h.assignment_multiplier as i32)
                .push_bind(h.rank as i32)
                .push_bind(h.rank_multiplier as i32)
                .push_bind(h.boosted_multiplier as i32);
            })
            .build()
            .execute(&mut transaction)
            .await?;
    }

    transaction.commit().await?;

    Ok(())
}
