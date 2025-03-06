use chrono::{DateTime, Utc};
use clap::Parser;

use h3o::{CellIndex, LatLng};
use oracle_persist::commands::{
    animal_names::AnimalNames, clean::Clean, import::Import, reward_analyzer::RewardAnalyzer,
};
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
    AnimalNames(AnimalNames),
    RewardAnalyzer(RewardAnalyzer),
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
    #[arg(long)]
    asserted_location: u64,
}

impl AssertedDistance {
    async fn run(self) -> anyhow::Result<()> {
        let db = self.db.connect().await?;

        println!("asserted location: {:?}", self.asserted_location);

        let asserted_latlng: LatLng = CellIndex::try_from(self.asserted_location)?.into();
        dbg!(asserted_latlng);

        let results = sqlx::query(
        r#"
            WITH t1 AS (
            	SELECT *, lag(distance_to_asserted) OVER (PARTITION BY hotspot_key ORDER BY timestamp) AS prev_distance_to_asserted
            	FROM mobile_validated_heartbeats mvh 
            	WHERE mvh.hotspot_key = $1
            )
            SELECT timestamp, distance_to_asserted, location_validation_timestamp, lat, lon
            FROM t1
            WHERE distance_to_asserted IS DISTINCT FROM prev_distance_to_asserted
        "#,
        )
        .bind(self.pubkey)
        .fetch_all(&db)
        .await?;

        for row in results {
            let ts = row.get::<DateTime<Utc>, &str>("timestamp");
            let lat = row.get::<Decimal, &str>("lat").to_f64().unwrap();
            let lon = row.get::<Decimal, &str>("lon").to_f64().unwrap();
            let original_distance_to_asserted = row.get::<i64, &str>("distance_to_asserted");

            let ll: LatLng = LatLng::new(lat, lon)?
                .to_cell(h3o::Resolution::Twelve)
                .into();

            let distance = asserted_latlng.distance_m(ll).round();
            println!(
                "timestamp = {}, lat = {}, lng = {}, original = {}, distance = {}",
                ts, lat, lon, original_distance_to_asserted, distance
            );
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
            Cmd::AnimalNames(an) => an.run().await,
            Cmd::RewardAnalyzer(ra) => ra.run().await,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    dbg!(&args);

    args.command.run().await
}

#[cfg(test)]
mod tests {
    use angry_purple_tiger::AnimalName;
    use h3o::LatLng;
    use sqlx::{postgres::PgPoolOptions, Row};

    #[test]
    fn brian() -> anyhow::Result<()> {
        let co_ll: LatLng = h3o::CellIndex::try_from(631781452644486655)?.into();
        let hb_ll: LatLng = LatLng::new(33.1158099001481, -96.8272952806648)?
            .to_cell(h3o::Resolution::Twelve)
            .into();

        let d = co_ll.distance_m(hb_ll);
        dbg!(d);

        Ok(())
    }

    #[tokio::test]
    async fn animal_names() -> anyhow::Result<()> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://postgres:postgres@localhost/hip_125")
            .await?;

        let result = sqlx::query("select public_key from wifi_tower_analysis")
            .fetch_all(&pool)
            .await?;

        for row in result {
            let pk = row.get::<String, &str>("public_key");
            let animal_name = pk.parse::<AnimalName>()?;

            sqlx::query("update wifi_tower_analysis set animal_name = $1 where public_key = $2")
                .bind(animal_name.to_string())
                .bind(pk)
                .execute(&pool)
                .await?;
        }

        Ok(())
    }
}
