use sqlx::{postgres::PgPoolOptions, Row};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgresql://postgres:postgres@localhost/hip_125")
        .await?;

    let results = sqlx::query(
        r#"
        SELECT kta.*
        FROM key_to_assets kta
            inner join mobile_hotspot_infos mhi on kta.asset = mhi.asset
        WHERE mhi.device_type #>> '{}' != 'cbrs'
        "#,
    )
    .fetch_all(&pool)
    .await?;

    for row in results {
        let entity_key: Vec<u8> = row.get("entity_key");
        let address: String = row.get("address");

        let key = bs58::encode(entity_key).into_string();

        sqlx::query(
            r#"
            UPDATE key_to_assets
            SET public_key = $1
            WHERE address = $2
        "#,
        )
        .bind(&key)
        .bind(&address)
        .execute(&pool)
        .await?;
    }

    Ok(())
}
