use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

pub mod animal_names;
pub mod clean;
pub mod import;

#[derive(Debug, clap::Args)]
pub struct DbArgs {
    #[arg(short, long)]
    db_url: String,
}

impl DbArgs {
    pub async fn connect(&self) -> anyhow::Result<Pool<Postgres>> {
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.db_url)
            .await
            .map_err(anyhow::Error::from)
    }
}

#[derive(Debug, clap::Args)]
pub struct TimeArgs {
    #[arg(long)]
    after: Option<NaiveDateTime>,
    #[arg(long)]
    before: Option<NaiveDateTime>,
}

impl TimeArgs {
    pub fn after_utc(&self) -> Option<DateTime<Utc>> {
        self.after.as_ref().map(NaiveDateTime::and_utc)
    }

    pub fn before_utc(&self) -> Option<DateTime<Utc>> {
        self.before.as_ref().map(NaiveDateTime::and_utc)
    }
}

#[derive(Debug, clap::Args)]
pub struct S3Args {
    #[arg(short, long)]
    bucket: String,
    #[arg(short, long, default_value = "us-west-2")]
    region: String,
    #[arg(short, long)]
    endpoint: Option<String>,
}

impl S3Args {
    pub async fn file_store(&self) -> anyhow::Result<FileStore> {
        FileStore::new(
            self.bucket.clone(),
            self.endpoint.clone(),
            Some(self.region.clone()),
            None,
            None,
            None,
            None,
        )
        .await
        .map_err(anyhow::Error::from)
    }
}
