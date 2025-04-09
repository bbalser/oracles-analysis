
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::RadioRewardV2;
use sqlx::{Pool, Postgres, QueryBuilder, Row, Transaction};
use uuid::Uuid;

use crate::to_datetime;

#[derive(Debug, Clone)]
struct Reward {
    reward: RadioRewardV2,
    start_period: DateTime<Utc>,
    end_period: DateTime<Utc>,
}

#[derive(Debug)]
struct IdentifiedReward {
    reward: RadioRewardV2,
    id: i64,
}

#[derive(Default, Debug)]
pub struct BulkRadioRewardV2 {
    rewards: Vec<Reward>,
}

impl BulkRadioRewardV2 {
    pub fn add(
        &mut self,
        reward: RadioRewardV2,
        start_period: DateTime<Utc>,
        end_period: DateTime<Utc>,
    ) {
        self.rewards.push(Reward {
            reward,
            start_period,
            end_period,
        });
    }

    pub async fn insert(self, db: &Pool<Postgres>) -> anyhow::Result<()> {
        let transaction = db.begin().await?;

        const NUM_BATCH: usize = (u16::MAX / 16) as usize;

        let (mut transaction, identified_rewards) = stream::iter(self.rewards)
            .map(anyhow::Ok)
            .try_chunks(NUM_BATCH)
            .err_into::<anyhow::Error>()
            .try_fold(
                (transaction, Vec::new()),
                |(mut tx, mut ids), batch| async move {
                    ids.append(&mut insert_reward(&mut tx, batch).await?);
                    Ok((tx, ids))
                },
            )
            .await?;

        insert_location_trust_scores(&mut transaction, &identified_rewards).await?;
        insert_speedtests(&mut transaction, &identified_rewards).await?;
        insert_speedtest_averages(&mut transaction, &identified_rewards).await?;
        insert_covered_hexes(&mut transaction, &identified_rewards).await?;

        transaction.commit().await?;
        Ok(())
    }
}

async fn insert_reward(
    tx: &mut Transaction<'_, Postgres>,
    rewards: Vec<Reward>,
) -> anyhow::Result<Vec<IdentifiedReward>> {
    let ids = 
    QueryBuilder::new("INSERT INTO mobile_radio_rewards_v2(start_period, end_period, hotspot_key, cbsd_id, base_coverage_points_sum, boosted_coverage_points_sum, base_reward_shares, boosted_reward_shares, base_poc_reward, boosted_poc_reward, seniority_ts, coverage_object, location_trust_score_multiplier, speedtest_multiplier, sp_boosted_hex_status, oracle_boosted_hex_status)")
    .push_values(rewards.clone(), |mut b, reward| {
        b.push_bind(reward.start_period)
        .push_bind(reward.end_period)
        .push_bind(PublicKeyBinary::from(reward.reward.hotspot_key.as_slice()).to_string())
        .push_bind(reward.reward.cbsd_id.clone())
        .push_bind(super::from_proto_decimal(reward.reward.base_coverage_points_sum.as_ref()))
        .push_bind(super::from_proto_decimal(reward.reward.boosted_coverage_points_sum.as_ref()))
        .push_bind(super::from_proto_decimal(reward.reward.base_reward_shares.as_ref()))
        .push_bind(super::from_proto_decimal(reward.reward.boosted_reward_shares.as_ref()))
        .push_bind(reward.reward.base_poc_reward as i64)
        .push_bind(reward.reward.boosted_poc_reward as i64)
        .push_bind(to_datetime(reward.reward.seniority_timestamp))
        .push_bind(Uuid::from_slice(reward.reward.coverage_object.as_slice()).unwrap())
        .push_bind(super::from_proto_decimal(reward.reward.location_trust_score_multiplier.as_ref()))
        .push_bind(super::from_proto_decimal(reward.reward.speedtest_multiplier.as_ref()))
        .push_bind(reward.reward.sp_boosted_hex_status().as_str_name())
        .push_bind(reward.reward.oracle_boosted_hex_status().as_str_name());
    })
    .push("RETURNING id")
    .build()
    .fetch_all(tx)
    .await?;

    Ok(rewards.into_iter()
        .zip(ids)
        .map(|(reward, row)| {
            IdentifiedReward {
                reward: reward.reward,
                id: row.get(0),
            }
        })
        .collect())
}

async fn insert_location_trust_scores(
    transaction: &mut Transaction<'_, Postgres>,
    identified_rewards: &[IdentifiedReward],
) -> anyhow::Result<()> {

    let scores : Vec<_> =
        identified_rewards
            .iter()
            .flat_map(|ir| {
                ir.reward.location_trust_scores.iter()
                    .map(|lts| (ir.id, lts))
                    .collect::<Vec<_>>()
            })
            .collect();

    const NUM_IN_BATCH: usize = (u16::MAX / 3) as usize;
    for chunk in scores.chunks(NUM_IN_BATCH) {
        QueryBuilder::new(
            r#"
            INSERT INTO location_trust_scores(id, meters_to_asserted, trust_score)
            "#,
        )
        .push_values(chunk, |mut b, (id, lt)| {
            b.push_bind(id)
                .push_bind(lt.meters_to_asserted as i64)
                .push_bind(super::from_proto_decimal(lt.trust_score.as_ref()));
        })
        .build()
        .execute(&mut *transaction)
        .await?;
    }
    
    Ok(())
}

async fn insert_speedtests(
    transaction: &mut Transaction<'_, Postgres>,
    identified_rewards: &[IdentifiedReward],
) -> anyhow::Result<()> {
    let tests : Vec<_> =
        identified_rewards
            .iter()
            .flat_map(|ir| {
                ir.reward.speedtests.iter()
                    .map(|st| (ir.id, st))
                    .collect::<Vec<_>>()
            })
            .collect();

    const NUM_IN_BATCH: usize = (u16::MAX / 5) as usize;
    for chunk in tests.chunks(NUM_IN_BATCH) {
        QueryBuilder::new(
            r#"
            INSERT INTO speedtests(id, upload, download, latency, timestamp)
            "#,
        )
        .push_values(chunk, |mut b, (id, st)| {
            b.push_bind(id)
                .push_bind(st.upload_speed_bps as i64)
                .push_bind(st.download_speed_bps as i64)
                .push_bind(st.latency_ms as i32)
                .push_bind(to_datetime(st.timestamp));
        })
        .build()
        .execute(&mut *transaction)
        .await?;
    }

    Ok(())
}

async fn insert_speedtest_averages(
    transaction: &mut Transaction<'_, Postgres>,
    identified_rewards: &[IdentifiedReward]
) -> anyhow::Result<()> {

    let averages: Vec<_> =
        identified_rewards
            .iter()
            .filter_map(|ir| ir.reward.speedtest_average.as_ref().map(|sa| (ir.id, sa)))
            .collect();

    const NUM_IN_BATCH: usize = (u16::MAX / 5) as usize;
    for chunk in averages.chunks(NUM_IN_BATCH) {
        QueryBuilder::new(
            r#"
            INSERT INTO speedtest_average(id, upload, download, latency, timestamp)
            "#,
        )
        .push_values(chunk, |mut b, (id, st)| {
            b.push_bind(id)
                .push_bind(st.upload_speed_bps as i64)
                .push_bind(st.download_speed_bps as i64)
                .push_bind(st.latency_ms as i32)
                .push_bind(to_datetime(st.timestamp));
        })
        .build()
        .execute(&mut *transaction)
        .await?;
    }

    Ok(())
}

async fn insert_covered_hexes(
    transaction: &mut Transaction<'_, Postgres>,
    identified_rewards: &[IdentifiedReward],
) -> anyhow::Result<()> {

    let covered_hexes: Vec<_> =
        identified_rewards
            .iter()
            .flat_map(|ir| {
                ir.reward.covered_hexes.iter()
                    .map(|ch| (ir.id, ch))
                    .collect::<Vec<_>>()
            })
            .collect();


    const NUM_IN_BATCH: usize = (u16::MAX / 11) as usize;
    for chunk in covered_hexes.chunks(NUM_IN_BATCH) {
        QueryBuilder::new(r#"
            INSERT INTO covered_hexes(id, location, base_coverage_points, boosted_coverage_points, urbanized, footfall, landtype, assignment_multiplier, rank, rank_multiplier, boosted_multiplier)
            "#)
            .push_values(chunk, |mut b, (id, h)| {
                b.push_bind(id)
                .push_bind(h.location as i64)
                .push_bind(super::from_proto_decimal(h.base_coverage_points.as_ref()))
                .push_bind(super::from_proto_decimal(h.boosted_coverage_points.as_ref()))
                .push_bind(h.urbanized().as_str_name())
                .push_bind(h.footfall().as_str_name())
                .push_bind(h.landtype().as_str_name())
                .push_bind(super::from_proto_decimal(h.assignment_multiplier.as_ref()))
                .push_bind(h.rank as i32)
                .push_bind(super::from_proto_decimal(h.rank_multiplier.as_ref()))
                .push_bind(h.boosted_multiplier as i32);
            })
            .build()
            .execute(&mut *transaction)
            .await?;
    }
    
    Ok(())
}
