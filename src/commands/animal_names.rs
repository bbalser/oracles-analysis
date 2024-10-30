use std::path::PathBuf;

use angry_purple_tiger::AnimalName;
use clap::arg;

#[derive(Debug, clap::Args)]
pub struct AnimalNames {
    #[arg(long)]
    csv: PathBuf,
}

impl AnimalNames {
    pub async fn run(self) -> anyhow::Result<()> {
        let mut rdr = csv::Reader::from_path(self.csv)?;
        let mut wtr = csv::Writer::from_writer(std::io::stdout());

        let mut headers: Vec<String> = rdr.headers()?.iter().map(|s| s.to_owned()).collect();
        headers[0] = "animal_name".to_string();

        wtr.write_record(headers)?;

        for result in rdr.into_records() {
            let mut row: Vec<String> = result?.iter().map(|s| s.to_owned()).collect();
            row[0] = row[0].parse::<AnimalName>()?.to_string();
            wtr.write_record(row)?;
        }

        wtr.flush()?;

        Ok(())
    }
}
