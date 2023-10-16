use clap::Parser;
use std::fs;
use std::path::PathBuf;
use std::time;

mod dbf_via_string;
mod dbf_write_bytes;
mod headerdata;

#[derive(Parser)]
struct Args {
    #[arg(default_value = "c:/Users/Hagen/RustProjects/dbfstuff/testdata/")]
    path: std::path::PathBuf,
}

fn main() {
    let timer = time::Instant::now();
    let args = Args::parse();
    let mut tablefiles: Vec<PathBuf> = vec![];
    if let Ok(entries) = fs::read_dir(&args.path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension.to_ascii_lowercase() == "dbf" {
                    tablefiles.push(path);
                }
            }
        }
    }
    let pb = indicatif::ProgressBar::new(tablefiles.len() as u64);
    for table in &tablefiles {
        //dbf_write_bytes::write_dbf_to_csv(table);
        dbf_via_string::convert_dbf_to_csv(table);
        pb.println(format!("{:?} converted", table));
        pb.inc(1);
    }
    let message = format!(
        "It took {:?} to convert {:?} files",
        timer.elapsed(),
        tablefiles.len(),
    );
    //pb.finish_with_message(message); doesn't seem to work
    println!("{}", message);
}
