use clap::Parser;
use std::fs::{self, File};
use std::io::{LineWriter, Write};
use std::path::PathBuf;
use std::time;

#[derive(Debug)]
struct DbfHeader {
    _version: u8,
    _last_update: String,
    records: u32,
    bytes_header: u16,
    bytes_record: u16,
}

impl DbfHeader {
    fn new(bytes: &[u8]) -> Self {
        Self {
            _version: bytes[0],
            _last_update: get_date_for_header(&bytes[1..4]),
            records: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            bytes_header: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            bytes_record: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
        }
    }
}

#[derive(Debug, PartialEq)]
enum MemoFileType {
    Old,
    New,
}

#[derive(Debug)]
struct MemoHeader {
    memo_type: MemoFileType,
    block_size: u16,
}

impl MemoHeader {
    fn new(bytes: &[u8]) -> Self {
        let size = u16::from_be_bytes(bytes[6..8].try_into().unwrap());
        Self {
            memo_type: if size > 0 {
                MemoFileType::New
            } else {
                MemoFileType::Old
            },
            block_size: {
                if size > 0 {
                    size
                } else {
                    u16::from_le_bytes(bytes[20..22].try_into().unwrap())
                }
            },
        }
    }
}

#[derive(Debug)]
struct DbfFields {
    fieldname: String,
    fieldtype: char, // FieldTypes,
    displacement: u32,
    length: usize,
    decimal_places: usize,
}

impl DbfFields {
    fn new(bytes: &[u8]) -> Self {
        Self {
            fieldname: latin1_to_string(&bytes[0..11]),
            fieldtype: bytes[11] as char,
            displacement: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            length: bytes[16] as usize,
            decimal_places: bytes[17] as usize,
        }
    }
}

fn get_date_for_header(bytes: &[u8]) -> String {
    format!("{}.{}.{}", bytes[2], bytes[1], bytes[0] as usize + 1900)
}

fn get_fields(bytes: &[u8]) -> Vec<DbfFields> {
    let mut next_field_record_startbyte = 32;
    let field_definition_end_marker = 0x0D;
    let mut result = vec![];
    let mut last_displacement = 1; // das erste Feld hat displacement 1 wegen delete-Flag
    while bytes[next_field_record_startbyte] != field_definition_end_marker {
        let mut field =
            DbfFields::new(&bytes[next_field_record_startbyte..next_field_record_startbyte + 32]);
        if field.displacement == 0 {
            // jedes Feld muss displacement haben
            // gehen davon aus, dass es nie angegeben ist wenn es einmal fehlt
            field.displacement = last_displacement;
            last_displacement += field.length as u32;
        }
        result.push(field);
        next_field_record_startbyte += 32;
    }
    result
}

fn get_field_header_as_csv(fields: &Vec<DbfFields>) -> String {
    let mut result: String = String::from("");
    for field in fields {
        result.push_str(&field.fieldname);
        result.push(';');
    }
    result.push_str("\r\n");
    result
}

fn get_record_as_csv(
    bytes: &[u8],
    fields: &Vec<DbfFields>,
    memos: &Option<Vec<u8>>,
    memo_header: &MemoHeader,
) -> String {
    let mut result: String = String::from("");
    for field in fields {
        let content = get_field_content_as_string(
            &bytes[field.displacement as usize..field.displacement as usize + field.length],
            &field.fieldtype,
            memos,
            memo_header,
        );
        result.push_str(content.trim());
        result.push(';');
    }
    result.push_str("\r\n");
    result
}

fn get_memo_content(bytes: &[u8], block: u32, memo_header: &MemoHeader) -> String {
    let startbyte = (block * memo_header.block_size as u32) as usize;
    if memo_header.memo_type == MemoFileType::New {
        let length = u32::from_be_bytes(bytes[startbyte + 4..startbyte + 8].try_into().unwrap());
        latin1_to_string(&bytes[startbyte + 8..startbyte + 8 + length as usize])
    } else {
        let length = u32::from_le_bytes(bytes[startbyte + 4..startbyte + 8].try_into().unwrap());
        latin1_to_string(&bytes[startbyte + 8..startbyte + length as usize])
    }
}

fn get_field_content_as_string(
    bytes: &[u8],
    fieldtype: &char,
    memos: &Option<Vec<u8>>,
    memo_header: &MemoHeader,
) -> String {
    match fieldtype {
        'C' | 'N' => latin1_to_string(bytes),
        'D' => {
            let yyyymmdd = latin1_to_string(bytes);
            if yyyymmdd.trim().len() != 8 {
                String::from("")
            } else {
                format!(
                    "{}.{}.{}",
                    &yyyymmdd[6..8],
                    &yyyymmdd[4..6],
                    &yyyymmdd[0..4]
                )
            }
        }
        'F' => String::from("missing implementation for float"),
        'L' => {
            let value = latin1_to_string(bytes);
            match value.as_str() {
                "y" | "Y" | "t" | "T" => String::from("true"),
                "n" | "N" | "f" | "F" => String::from("false"),
                _ => String::from(""),
            }
        }
        'T' => String::from("missing implementation for time"),
        'I' => u32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
        'Y' => String::from("missing implementation for currency"),
        'M' => {
            let block_number;
            if bytes.len() == 4 {
                block_number = u32::from_le_bytes(bytes.try_into().unwrap());
            } else {
                let mut block_string = latin1_to_string(bytes);
                block_string = String::from(block_string.trim());
                block_number = match block_string.parse::<u32>() {
                    Ok(nmb) => nmb,
                    _ => 0,
                };
            };
            if block_number == 0 {
                String::from("")
            } else {
                //eprintln!("Memo block number: {}", block_number);
                match memos {
                    Some(memo) => {
                        "\"".to_owned()
                            + get_memo_content(memo, block_number, memo_header).as_str()
                            + "\""
                    }
                    None => String::from("memofile missing"),
                }
            }
        }
        'B' => String::from("missing implementation for double"),
        'G' => String::from("missing implementation for general"),
        'P' => String::from("missing implementation for picture"),
        '+' => String::from("missing implementation for autoinc"),
        'O' => String::from("missing implementation for double"),
        '@' => String::from("missing implementation for timestamp"),
        'V' => String::from("missing implementation for varchar"),
        _ => String::from("missing implementation unknown fieldtype"),
    }
}

fn convert_dbf_to_csv(table: &PathBuf) {
    let dbffile = std::fs::read(table).unwrap();
    let memopath = table
        .to_str()
        .unwrap()
        .to_lowercase()
        .replace(".dbf", ".fpt");
    let mut memofile: Option<Vec<u8>> = match std::fs::read(&memopath) {
        Ok(file) => Some(file),
        _ => None,
    };
    if memofile.is_none() {
        let memopath = memopath.replace(".fpt", ".dbt");
        memofile = match std::fs::read(&memopath) {
            Ok(file) => Some(file),
            _ => None,
        };
    }
    let header = DbfHeader::new(&dbffile[0..32]);
    //eprintln!("DbfHeader: {:?}", header);
    let fields = get_fields(&dbffile);
    //eprintln!("DbfFields: {:?}", fields);
    let field_header = get_field_header_as_csv(&fields);
    let memo_header = match &memofile {
        Some(file) => MemoHeader::new(&file[0..512]),
        None => MemoHeader {
            memo_type: MemoFileType::New,
            block_size: 0,
        },
    };
    //eprintln!("Memoheader: {:?}", memo_header);
    let mut linenumber = 0;
    let mut allcsv = field_header.clone();
    let mut delcsv = String::from("");
    while linenumber < header.records {
        let startbyte =
            (header.bytes_header as u32 + linenumber * header.bytes_record as u32) as usize;
        let endbyte = startbyte + header.bytes_record as usize;
        let line = get_record_as_csv(
            &dbffile[startbyte..endbyte],
            &fields,
            &memofile,
            &memo_header,
        );
        if dbffile[startbyte] == 32 {
            allcsv.push_str(&line);
        } else {
            delcsv.push_str(&line);
        }
        linenumber += 1;
    }
    let resultpath = table
        .to_str()
        .unwrap()
        .to_lowercase()
        .replace(".dbf", ".csv");
    std::fs::write(&resultpath, allcsv).unwrap();
    if !delcsv.is_empty() {
        delcsv.insert_str(0, &field_header);
        let delpath = resultpath.replace(".csv", "_del.csv");
        std::fs::write(delpath, delcsv).unwrap();
    }
}

fn write_memo_content_to_file(
    bytes: &[u8],
    block: u32,
    memo_header: &MemoHeader,
    write_file: &mut LineWriter<File>,
) {
    let startbyte = (block * memo_header.block_size as u32) as usize;
    if memo_header.memo_type == MemoFileType::New {
        let length = u32::from_be_bytes(bytes[startbyte + 4..startbyte + 8].try_into().unwrap());
        write_file
            .write_all(&bytes[startbyte + 8..startbyte + 8 + length as usize])
            .unwrap();
    } else {
        let length = u32::from_le_bytes(bytes[startbyte + 4..startbyte + 8].try_into().unwrap());
        write_file
            .write_all(&bytes[startbyte + 8..startbyte + length as usize])
            .unwrap();
    }
}

fn write_field_content_to_file(
    bytes: &[u8],
    fieldtype: &char,
    memos: &Option<Vec<u8>>,
    memo_header: &MemoHeader,
    write_file: &mut LineWriter<File>,
) {
    match fieldtype {
        'C' | 'N' => write_file.write_all(bytes).unwrap(),
        'D' => {
            if bytes.len() == 8 {
                write_file.write_all(&bytes[6..8]).unwrap();
                write_file.write_all(b".").unwrap();
                write_file.write_all(&bytes[4..6]).unwrap();
                write_file.write_all(b".").unwrap();
                write_file.write_all(&bytes[0..4]).unwrap();
            }
        }
        'F' => write_file
            .write_all(b"missing implementation for float")
            .unwrap(),
        'L' => {
            //let value = latin1_to_string(bytes);
            match bytes {
                //value.as_str() {
                b"y" | b"Y" | b"t" | b"T" => write_file.write_all(b"true").unwrap(),
                b"n" | b"N" | b"f" | b"F" => write_file.write_all(b"false").unwrap(),
                _ => (),
            }
        }
        'T' => write_file
            .write_all(b"missing implementation for time")
            .unwrap(),
        'I' => {
            let int = u32::from_le_bytes(bytes.try_into().unwrap());
            write_file.write_all(int.to_string().as_bytes()).unwrap();
        }
        'Y' => write_file
            .write_all(b"missing implementation for currency")
            .unwrap(),
        'M' => {
            let block_number;
            if bytes.len() == 4 {
                block_number = u32::from_le_bytes(bytes.try_into().unwrap());
            } else {
                let mut block_string = latin1_to_string(bytes);
                block_string = String::from(block_string.trim());
                block_number = match block_string.parse::<u32>() {
                    Ok(nmb) => nmb,
                    _ => 0,
                };
            };
            if block_number > 0 {
                match memos {
                    Some(memo) => {
                        write_file.write_all(b"\"").unwrap();
                        write_memo_content_to_file(memo, block_number, memo_header, write_file);
                        write_file.write_all(b"\"").unwrap();
                    }
                    None => write_file.write_all(b"memofile missing").unwrap(),
                }
            }
        }
        'B' => write_file
            .write_all(b"missing implementation for double")
            .unwrap(),
        'G' => write_file
            .write_all(b"missing implementation for general")
            .unwrap(),
        'P' => write_file
            .write_all(b"missing implementation for picture")
            .unwrap(),
        '+' => write_file
            .write_all(b"missing implementation for autoinc")
            .unwrap(),
        'O' => write_file
            .write_all(b"missing implementation for double")
            .unwrap(),
        '@' => write_file
            .write_all(b"missing implementation for timestamp")
            .unwrap(),
        'V' => write_file
            .write_all(b"missing implementation for varchar")
            .unwrap(),
        _ => write_file
            .write_all(b"missing implementation unknown fieldtype")
            .unwrap(),
    }
}

fn write_record_to_file(
    bytes: &[u8],
    fields: &Vec<DbfFields>,
    memos: &Option<Vec<u8>>,
    memo_header: &MemoHeader,
    write_file: &mut LineWriter<File>,
) {
    for field in fields {
        write_field_content_to_file(
            &bytes[field.displacement as usize..field.displacement as usize + field.length],
            &field.fieldtype,
            memos,
            memo_header,
            write_file,
        );
        write_file.write_all(b";").unwrap();
    }
    write_file.write_all(b"\r\n").unwrap();
}

fn write_dbf_to_csv(table: &PathBuf) {
    let dbffile = std::fs::read(table).unwrap();
    let memopath = table
        .to_str()
        .unwrap()
        .to_lowercase()
        .replace(".dbf", ".fpt");
    let mut memofile: Option<Vec<u8>> = match std::fs::read(&memopath) {
        Ok(file) => Some(file),
        _ => None,
    };
    if memofile.is_none() {
        let memopath = memopath.replace(".fpt", ".dbt");
        memofile = match std::fs::read(&memopath) {
            Ok(file) => Some(file),
            _ => None,
        };
    }
    let header = DbfHeader::new(&dbffile[0..32]);
    let fields = get_fields(&dbffile);
    let field_header = get_field_header_as_csv(&fields);
    let memo_header = match &memofile {
        Some(file) => MemoHeader::new(&file[0..512]),
        None => MemoHeader {
            memo_type: MemoFileType::New,
            block_size: 0,
        },
    };
    let mut linenumber = 0;
    let resultpath = table
        .to_str()
        .unwrap()
        .to_lowercase()
        .replace(".dbf", ".csv");
    let delpath = resultpath.replace(".csv", "_del.csv");

    let allcsv = File::create(resultpath).unwrap();
    let mut allcsv = LineWriter::new(allcsv);
    let delcsv = File::create(delpath).unwrap();
    let mut delcsv = LineWriter::new(delcsv);
    allcsv.write_all(field_header.as_bytes()).unwrap();
    delcsv.write_all(field_header.as_bytes()).unwrap();
    while linenumber < header.records {
        let startbyte =
            (header.bytes_header as u32 + linenumber * header.bytes_record as u32) as usize;
        let endbyte = startbyte + header.bytes_record as usize;
        write_record_to_file(
            &dbffile[startbyte..endbyte],
            &fields,
            &memofile,
            &memo_header,
            if dbffile[startbyte] == 32 {
                &mut allcsv
            } else {
                &mut delcsv
            },
        );
        linenumber += 1;
    }
}

fn latin1_to_string(latin1_data: &[u8]) -> String {
    latin1_data
        .iter()
        .filter(|b| **b != 0)
        .map(|&c| c as char)
        .collect()
}

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
        write_dbf_to_csv(table);
        //convert_dbf_to_csv(table);
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
