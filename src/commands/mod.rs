use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

pub mod animal_names;
pub mod clean;
pub mod import;
pub mod reward_analyzer;

#[derive(Debug, clap::Args)]
pub struct DbArgs {
    #[arg(short, long)]
    db_url: String,
    #[arg(long)]
    schema: Option<String>,
}

impl DbArgs {
    pub async fn connect(&self) -> anyhow::Result<Pool<Postgres>> {
        let schema = self.schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .before_acquire(move |conn, _meta| {
                let schema = schema.clone();
                Box::pin(async move {
                    if let Some(schema) = schema {
                        sqlx::query(&format!("SET search_path TO {schema}"))
                            .execute(conn)
                            .await?;
                    }

                    Ok(true)
                })
            })
            .connect(&self.db_url)
            .await?;

        Ok(pool)
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
