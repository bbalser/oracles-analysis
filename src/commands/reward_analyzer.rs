use std::str::FromStr;

use chrono::{DateTime, Utc};
use coverage_point_calculator::{
    BytesPs, CoveragePoints, LocationTrust, OracleBoostingStatus, RadioType, Speedtest,
};
use helium_crypto::PublicKeyBinary;
use rust_decimal::Decimal;
use sqlx::{Pool, Postgres, Row};

use super::DbArgs;

mod coverage;

#[derive(Debug, clap::Args)]
pub struct RewardAnalyzer {
    #[command(flatten)]
    db: DbArgs,
    #[arg(short, long)]
    start_period: DateTime<Utc>,
    #[arg(short, long)]
    end_period: DateTime<Utc>,
    #[arg(short, long)]
    pubkey: String,
}

impl RewardAnalyzer {
    pub async fn run(self) -> anyhow::Result<()> {
        let db = self.db.connect().await?;
        let hotspot_key = PublicKeyBinary::from_str(&self.pubkey)?;

        println!("loading coverage map...");
        let coverage_map =
            coverage::load_coverage_map(&db, self.start_period, self.end_period).await?;

        let radio_type = match is_indoor(&db, &hotspot_key, self.end_period).await? {
            true => RadioType::IndoorWifi,
            false => RadioType::OutdoorWifi,
        };

        let mut points = CoveragePoints::new(
            radio_type,
            coverage_point_calculator::SPBoostedRewardEligibility::Eligible,
            speedtests(&db, &hotspot_key, self.end_period).await?,
            location_trust_scores(&db, &hotspot_key, self.start_period, self.end_period).await?,
            coverage_map
                .get_wifi_coverage(hotspot_key.as_ref())
                .to_vec(),
            oracle_boosting_status(&db, &hotspot_key, self.start_period, self.end_period).await?,
        )?;

        points.speedtests = vec![];
        points.location_trust_scores = vec![];
        points.covered_hexes = vec![];

        dbg!(points);

        Ok(())
    }
}

async fn oracle_boosting_status(
    db: &Pool<Postgres>,
    hotspot_key: &PublicKeyBinary,
    start_period: DateTime<Utc>,
    end_period: DateTime<Utc>,
) -> anyhow::Result<OracleBoostingStatus> {
    let qualified = sqlx::query_scalar(
        r#"
        SELECT unique_connections > 25 AS qualified
        FROM unique_connections 
        WHERE hotspot_pubkey = $1
        	AND received_timestamp >= $2
        	AND received_timestamp < $3
        "#,
    )
    .bind(hotspot_key)
    .bind(start_period)
    .bind(end_period)
    .fetch_optional(db)
    .await?;

    let banned = sqlx::query_scalar(
        r#"
        SELECT exists(
        	SELECT 1
        	FROM sp_boosted_rewards_bans
        	WHERE radio_key = $1
        		AND ban_type = 'poc'
        		AND radio_type = 'wifi'
        		AND received_timestamp < $2
        		AND until > $2
        		AND COALESCE(invalidated_at > $2, TRUE)
        )
        "#,
    )
    .bind(hotspot_key)
    .bind(end_period)
    .fetch_one(db)
    .await?;

    if qualified.unwrap_or(false) {
        Ok(OracleBoostingStatus::Qualified)
    } else if banned {
        Ok(OracleBoostingStatus::Banned)
    } else {
        Ok(OracleBoostingStatus::Eligible)
    }
}

async fn location_trust_scores(
    db: &Pool<Postgres>,
    hotspot_key: &PublicKeyBinary,
    start_period: DateTime<Utc>,
    end_period: DateTime<Utc>,
) -> anyhow::Result<Vec<LocationTrust>> {
    let scores = sqlx::query(
        r#"
        SELECT distance_to_asserted, location_trust_score_multiplier
        FROM wifi_heartbeats
        WHERE hotspot_key = $1
            AND truncated_timestamp >= $2
            AND truncated_timestamp < $3
        "#,
    )
    .bind(hotspot_key)
    .bind(start_period)
    .bind(end_period)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|row| LocationTrust {
        meters_to_asserted: row.get::<i64, &str>("distance_to_asserted") as u32,
        trust_score: row.get::<Decimal, &str>("location_trust_score_multiplier"),
    })
    .collect();

    Ok(scores)
}

async fn speedtests(
    db: &Pool<Postgres>,
    hotspot_key: &PublicKeyBinary,
    end_period: DateTime<Utc>,
) -> anyhow::Result<Vec<Speedtest>> {
    let speedtests = sqlx::query(
        r#"
        SELECT upload_speed, download_speed, latency, timestamp
        FROM speedtests
        WHERE pubkey = $1
            AND timestamp >= $2 - INTERVAL '48 hours' 
            AND timestamp < $2
        "#,
    )
    .bind(hotspot_key)
    .bind(end_period)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|row| Speedtest {
        upload_speed: BytesPs::new(row.get::<i64, &str>("upload_speed") as u64),
        download_speed: BytesPs::new(row.get::<i64, &str>("download_speed") as u64),
        latency_millis: row.get::<i32, &str>("latency") as u32,
        timestamp: row.get::<DateTime<Utc>, &str>("timestamp"),
    })
    .collect();

    Ok(speedtests)
}

async fn is_indoor(
    db: &Pool<Postgres>,
    hotspot_key: &PublicKeyBinary,
    end_period: DateTime<Utc>,
) -> anyhow::Result<bool> {
    sqlx::query_scalar(
        r#"
        SELECT indoor
        FROM coverage_objects
        WHERE radio_key = $1
            AND inserted_at < $2
        ORDER BY inserted_at DESC
        LIMIT 1
    "#,
    )
    .bind(hotspot_key)
    .bind(end_period)
    .fetch_one(db)
    .await
    .map_err(anyhow::Error::from)
}
