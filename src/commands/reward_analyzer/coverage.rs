use std::{collections::HashMap, num::NonZeroU32};

use chrono::{DateTime, Utc};
use coverage_map::{
    BoostedHexMap, CoverageMap, CoverageMapBuilder, CoverageObject, UnrankedCoverage,
};
use futures::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use hex_assignments::assignment::HexAssignments;
use hextree::Cell;
use sqlx::{Pool, Postgres};

pub async fn load_coverage_map(
    db: &Pool<Postgres>,
    start_period: DateTime<Utc>,
    end_period: DateTime<Utc>,
) -> anyhow::Result<CoverageMap> {
    let coverage = sqlx::query_as::<_, RadioCoverage>(include_str!("coverage.sql"))
        .bind(start_period)
        .bind(end_period)
        .fetch(db)
        .try_fold(
            HashMap::<PublicKeyBinary, CoverageObject>::new(),
            |mut map, rc| async move {
                match map.get_mut(&rc.hotspot_key) {
                    Some(co) => {
                        co.coverage.push(UnrankedCoverage {
                            location: rc.hex,
                            signal_power: rc.signal_power,
                            signal_level: rc.signal_level.into(),
                            assignments: rc.assignments,
                        });
                    }
                    None => {
                        map.insert(
                            rc.hotspot_key.clone(),
                            CoverageObject {
                                indoor: rc.indoor,
                                hotspot_key: rc.hotspot_key.into(),
                                cbsd_id: None,
                                seniority_timestamp: rc.seniority_ts,
                                coverage: vec![UnrankedCoverage {
                                    location: rc.hex,
                                    signal_power: rc.signal_power,
                                    signal_level: rc.signal_level.into(),
                                    assignments: rc.assignments,
                                }],
                            },
                        );
                    }
                }

                Ok(map)
            },
        )
        .await?
        .into_values()
        .fold(CoverageMapBuilder::default(), |mut builder, co| {
            builder.insert_coverage_object(co);

            builder
        })
        .build(&NoBoostedHexes, start_period);

    Ok(coverage)
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RadioCoverage {
    pub hotspot_key: PublicKeyBinary,
    pub seniority_ts: DateTime<Utc>,
    #[sqlx(try_from = "i64")]
    pub hex: Cell,
    pub indoor: bool,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
    #[sqlx(flatten)]
    pub assignments: HexAssignments,
}

struct NoBoostedHexes;

impl BoostedHexMap for NoBoostedHexes {
    fn get_current_multiplier(&self, _cell: Cell, _ts: DateTime<Utc>) -> Option<NonZeroU32> {
        None
    }
}

#[derive(Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "signal_level")]
#[sqlx(rename_all = "lowercase")]
pub enum SignalLevel {
    None,
    Low,
    Medium,
    High,
}

impl From<SignalLevel> for coverage_map::SignalLevel {
    fn from(value: SignalLevel) -> Self {
        match value {
            SignalLevel::None => coverage_map::SignalLevel::None,
            SignalLevel::Low => coverage_map::SignalLevel::Low,
            SignalLevel::Medium => coverage_map::SignalLevel::Medium,
            SignalLevel::High => coverage_map::SignalLevel::High,
        }
    }
}
