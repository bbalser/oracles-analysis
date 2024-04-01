use futures::TryStreamExt;

use crate::SupportedFileTypes;

use super::{DbArgs, S3Args, TimeArgs};

#[derive(Debug, clap::Args)]
pub struct Import {
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
    pub async fn run(self) -> anyhow::Result<()> {
        let db = self.db.connect().await?;
        let store = self.s3.file_store().await?;

        let file_infos = store
            .list_all(
                &self.file_type.prefix(),
                self.time.after_utc(),
                self.time.before_utc(),
            )
            .await?;

        self.file_type.create_table(&db).await?;

        for file_info in file_infos {
            println!("processing file: {}", file_info);
            store
                .stream_file(file_info)
                .await?
                .map_err(anyhow::Error::from)
                .try_fold((&db, &self.file_type), |(db, file_type), buf| async move {
                    file_type
                        .decode(buf)?
                        .save(db)
                        .await
                        .map(|_| (db, file_type))
                })
                .await?;
        }

        Ok(())
    }
}
