use crate::SupportedFileTypes;

use super::{DbArgs, TimeArgs};

#[derive(Debug, clap::Args)]
pub struct Clean {
    #[arg(short, long)]
    file_type: Option<SupportedFileTypes>,
    #[command(flatten)]
    db: DbArgs,
    #[command(flatten)]
    time: TimeArgs,
}

impl Clean {
    pub async fn run(self) -> anyhow::Result<()> {
        println!("clean");
        Ok(())
    }
}
