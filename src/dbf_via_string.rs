use crate::headerdata;
use encoding::label::encoding_from_whatwg_label;
use encoding::{DecoderTrap, EncodingRef};
use std::path::PathBuf;
fn get_record_as_csv(
    bytes: &[u8],
    fields: &Vec<headerdata::DbfFields>,
    memos: &Option<Vec<u8>>,
    memo_header: &headerdata::MemoHeader,
    use_encoding: EncodingRef,
) -> String {
    let mut result: String = String::from("");
    for field in fields {
        let content = get_field_content_as_string(
            &bytes[field.displacement as usize..field.displacement as usize + field.length],
            &field.fieldtype,
            memos,
            memo_header,
            use_encoding,
        );
        result.push_str(content.as_str()); //.trim());
        result.push(';');
    }
    result.push_str("\r\n");
    result
}

fn get_memo_content(
    bytes: &[u8],
    block: u32,
    memo_header: &headerdata::MemoHeader,
    use_encoding: EncodingRef,
) -> String {
    let startbyte = (block * memo_header.block_size as u32) as usize;
    if memo_header.memo_type == headerdata::MemoFileType::New {
        let length = u32::from_be_bytes(bytes[startbyte + 4..startbyte + 8].try_into().unwrap());
        use_encoding
            .decode(
                &bytes[startbyte + 8..startbyte + 8 + length as usize],
                DecoderTrap::Ignore,
            )
            .unwrap()
    } else {
        let length = u32::from_le_bytes(bytes[startbyte + 4..startbyte + 8].try_into().unwrap());
        use_encoding
            .decode(
                &bytes[startbyte + 8..startbyte + length as usize],
                DecoderTrap::Ignore,
            )
            .unwrap()
    }
}

fn get_memo_content_fix_blocksize(bytes: &[u8], block: u32, use_encoding: EncodingRef) -> String {
    let blocksize = 512;
    let startbyte = (block * blocksize) as usize;
    let length = bytes[startbyte..]
        .iter()
        .take_while(|&&x| x != 0x1a)
        .count();
    use_encoding
        .decode(&bytes[startbyte..startbyte + length], DecoderTrap::Ignore)
        .unwrap()
}

fn get_field_content_as_string(
    bytes: &[u8],
    fieldtype: &char,
    memos: &Option<Vec<u8>>,
    memo_header: &headerdata::MemoHeader,
    use_encoding: EncodingRef,
) -> String {
    match fieldtype {
        'C' | 'N' => use_encoding.decode(bytes, DecoderTrap::Ignore).unwrap(),
        'D' => {
            let yyyymmdd = use_encoding.decode(bytes, DecoderTrap::Ignore).unwrap();
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
            match bytes {
                //value.as_str() {
                b"y" | b"Y" | b"t" | b"T" => String::from("true"),
                b"n" | b"N" | b"f" | b"F" => String::from("false"),
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
                let mut block_string = use_encoding.decode(bytes, DecoderTrap::Ignore).unwrap();
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
                        //+ get_memo_content(memo, block_number, memo_header, use_encoding)
			    + get_memo_content_fix_blocksize(memo, block_number, use_encoding)
                                .as_str()
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

pub fn convert_dbf_to_csv(table: &PathBuf) {
    let mut dbffile = std::fs::read(table).unwrap();
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
    //eprintln!("DbfHeader: {:?}", header);
    let fields = headerdata::get_fields(&dbffile);
    //eprintln!("DbfFields: {:?}", fields);
    let field_header = headerdata::get_field_header_as_csv(&fields);
    let memo_header = match &memofile {
        Some(file) => headerdata::MemoHeader::new(&file[0..512]),
        None => headerdata::MemoHeader {
            memo_type: headerdata::MemoFileType::New,
            block_size: 0,
        },
    };
    //eprintln!("Memoheader: {:?}", memo_header);
    let mut linenumber = 0;
    let mut allcsv = field_header.clone();
    let mut delcsv = String::from("");

    if header.language == 0x10 {
        // bytes der Sonderzeichen ersetzen
        dbffile = dbffile
            .iter()
            .map(|x| match x {
                0x8e => 0xc4,
                0x84 => 0xe4,
                0x99 => 0xd6,
                0x94 => 0xf6,
                0x9a => 0xdc,
                0x81 => 0xfc,
                0xe1 => 0xdf,
                _ => *x,
            })
            .collect();
    }
    let encoding_label = match header.language {
        0x10 => "windows-1252", // wird nicht von encoding unterstuetzt
        0x03 => "windows-1252",
        _ => "windows-1252",
    };
    let use_encoding = encoding_from_whatwg_label(encoding_label).unwrap();

    while linenumber < header.records {
        let startbyte =
            (header.bytes_header as u32 + linenumber * header.bytes_record as u32) as usize;
        let endbyte = startbyte + header.bytes_record as usize;
        let line = get_record_as_csv(
            &dbffile[startbyte..endbyte],
            &fields,
            &memofile,
            &memo_header,
            use_encoding,
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
