use std::str::FromStr;

use chrono::{DateTime, NaiveDateTime, Utc};
use clap::Parser;
use file_store::FileStore;
use futures::TryStreamExt;

use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use oracle_persist::SupportedFileTypes;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};

#[derive(Debug, clap::Args)]
struct DbArgs {
    #[arg(short, long)]
    db_url: String,
}

#[derive(Debug, clap::Args)]
struct TimeArgs {
    #[arg(long)]
    after: Option<NaiveDateTime>,
    #[arg(long)]
    before: Option<NaiveDateTime>,
}

#[derive(Debug, clap::Args)]
struct S3Args {
    #[arg(short, long)]
    bucket: String,
    #[arg(short, long, default_value = "us-west-2")]
    region: String,
    #[arg(short, long)]
    endpoint: Option<String>,
}

#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, clap::Args)]
struct Import {
    #[arg(short, long)]
    file_type: SupportedFileTypes,
    #[command(flatten)]
    db: DbArgs,
    #[command(flatten)]
    s3: S3Args,
    #[command(flatten)]
    time: TimeArgs,
}

impl Import {
    async fn run(self) -> anyhow::Result<()> {
        let db = connect_db(&self.db.db_url).await?;
        let store = create_file_store(self.s3.bucket, self.s3.endpoint, self.s3.region).await?;

        let file_infos = store.list(
            &self.file_type.prefix(),
            self.time.after.as_ref().map(NaiveDateTime::and_utc),
            self.time.before.as_ref().map(NaiveDateTime::and_utc),
        );

        self.file_type.create_table(&db).await?;

        store
            .source(file_infos)
            .map_err(anyhow::Error::from)
            .try_fold((db, self.file_type), |(db, file_type), buf| async move {
                file_type
                    .decode(buf)?
                    .save(&db)
                    .await
                    .map(|_| (db, file_type))
            })
            .await?;
        Ok(())
    }
}

#[derive(Debug, clap::Args)]
struct Clean {
    #[arg(short, long)]
    file_type: SupportedFileTypes,
    #[command(flatten)]
    db: DbArgs,
    #[command(flatten)]
    time: TimeArgs,
}

impl Clean {
    async fn run(self) -> anyhow::Result<()> {
        println!("clean");
        Ok(())
    }
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
    db: DbArgs,
    #[arg(long)]
    pubkey: String,
    #[arg(long)]
    cbsd_id: String,
}

impl AssertedDistance {
    async fn run(self) -> anyhow::Result<()> {
        let db = connect_db(&self.db.db_url).await?;
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
            FROM mobile_cbrs_ingest_reports
            WHERE cbsd_id = $1
        "#,
        )
        .bind(self.cbsd_id)
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
                println!("timestamp = {}, distance = {}", ts, distance);
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

async fn connect_db(db_url: &str) -> anyhow::Result<Pool<Postgres>> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .map_err(anyhow::Error::from)
}

async fn create_file_store(
    bucket: String,
    endpoint: Option<String>,
    region: String,
) -> anyhow::Result<FileStore> {
    FileStore::new(bucket, endpoint, Some(region))
        .await
        .map_err(anyhow::Error::from)
}
