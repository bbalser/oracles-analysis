use std::str::FromStr;

use chrono::{DateTime, Utc};
use clap::Parser;

use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use oracle_persist::commands::{clean::Clean, import::Import};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::Row;

#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, clap::Subcommand)]
enum Cmd {
    Import(Import),
    Clean(Clean),
    AssertedDistance(AssertedDistance),
    ToHex(ToHex),
}

#[derive(Debug, clap::Args)]
struct ToHex {
    #[arg(long)]
    pubkey: String,
}

impl ToHex {
    async fn run(self) -> anyhow::Result<()> {
        let bytes = bs58::decode(self.pubkey).into_vec()?;
        let hex = hex::encode(bytes);
        println!("hex: {}", hex);

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
struct AssertedDistance {
    #[command(flatten)]
    db: oracle_persist::commands::DbArgs,
    #[arg(long)]
    pubkey: String,
}

impl AssertedDistance {
    async fn run(self) -> anyhow::Result<()> {
        let db = self.db.connect().await?;
        let pubkey = PublicKeyBinary::from_str(&self.pubkey)?;
        let entity_key = bs58::decode(pubkey.to_string()).into_vec()?;
        let hex = hex::encode(&entity_key);
        println!("entity_key = {:?}", hex);

        let location: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT m.location::bigint
            FROM key_to_assets k 
                INNER JOIN mobile_hotspot_infos m on k.asset = m.asset
            WHERE k.entity_key = $1
            "#,
        )
        .bind(entity_key)
        .fetch_optional(&db)
        .await?;

        println!("asserted location: {:?}", location);

        let asserted_latlng: LatLng = CellIndex::try_from(location.unwrap() as u64)?.into();
        dbg!(asserted_latlng);

        let results = sqlx::query(
            r#"
            SELECT timestamp, lat, lon
            FROM mobile_wifi_ingest_reports
            WHERE hotspot_key = $1
            ORDER BY timestamp asc
        "#,
        )
        .bind(self.pubkey)
        .fetch_all(&db)
        .await?;

        for row in results {
            let ts = row.get::<DateTime<Utc>, &str>("timestamp");
            let lat = row.get::<Decimal, &str>("lat").to_f64().unwrap();
            let lon = row.get::<Decimal, &str>("lon").to_f64().unwrap();

            let ll: LatLng = LatLng::new(lat, lon)?
                .to_cell(h3o::Resolution::Twelve)
                .into();

            let distance = asserted_latlng.distance_m(ll).round();
            if distance > 30.0 {
                println!(
                    "timestamp = {}, lat = {}, lng = {}, distance = {}",
                    ts, lat, lon, distance
                );
            }
        }

        Ok(())
    }
}

impl Cmd {
    async fn run(self) -> anyhow::Result<()> {
        match self {
            Cmd::Import(import) => import.run().await,
            Cmd::Clean(clean) => clean.run().await,
            Cmd::AssertedDistance(asserted_distance) => asserted_distance.run().await,
            Cmd::ToHex(to_hex) => to_hex.run().await,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    dbg!(&args);

    args.command.run().await
}
