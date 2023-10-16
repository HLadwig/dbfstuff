use crate::headerdata;
use std::fs::File;
use std::io::{LineWriter, Write};
use std::path::PathBuf;

fn write_memo_content_to_file(
    bytes: &[u8],
    block: u32,
    memo_header: &headerdata::MemoHeader,
    write_file: &mut LineWriter<File>,
) {
    let startbyte = (block * memo_header.block_size as u32) as usize;
    if memo_header.memo_type == headerdata::MemoFileType::New {
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

fn latin1_to_string(latin1_data: &[u8]) -> String {
    latin1_data
        .iter()
        .filter(|b| **b != 0)
        .map(|&c| c as char)
        .collect()
}

fn write_field_content_to_file(
    bytes: &[u8],
    fieldtype: &char,
    memos: &Option<Vec<u8>>,
    memo_header: &headerdata::MemoHeader,
    write_file: &mut LineWriter<File>,
) {
    match fieldtype {
        'C' | 'N' => write_file.write_all(bytes).unwrap(),
        'D' => {
            if bytes.len() == 8 && !bytes.iter().all(|&x| x == 0x20) {
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
        'L' => match bytes {
            b"y" | b"Y" | b"t" | b"T" => write_file.write_all(b"true").unwrap(),
            b"n" | b"N" | b"f" | b"F" => write_file.write_all(b"false").unwrap(),
            _ => (),
        },
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
    fields: &Vec<headerdata::DbfFields>,
    memos: &Option<Vec<u8>>,
    memo_header: &headerdata::MemoHeader,
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

pub fn write_dbf_to_csv(table: &PathBuf) {
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
    let header = headerdata::DbfHeader::new(&dbffile[0..32]);
    let fields = headerdata::get_fields(&dbffile);
    let field_header = headerdata::get_field_header_as_csv(&fields);
    let memo_header = match &memofile {
        Some(file) => headerdata::MemoHeader::new(&file[0..512]),
        None => headerdata::MemoHeader {
            memo_type: headerdata::MemoFileType::New,
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
