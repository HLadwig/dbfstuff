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

#[derive(Debug)]
struct MemoHeader {
    block_size: u16,
}

impl MemoHeader {
    fn new(bytes: &[u8]) -> Self {
        Self {
            block_size: u16::from_be_bytes(bytes[6..8].try_into().unwrap()),
        }
    }
}

enum FieldTypes {}

#[derive(Debug)]
struct DbfFields {
    fieldname: String,
    fieldtype: char, // FieldTypes,
    displacement: usize,
    length: usize,
    decimal_places: usize,
}

impl DbfFields {
    fn new(bytes: &[u8]) -> Self {
        Self {
            fieldname: latin1_to_string(&bytes[0..11]),
            fieldtype: bytes[11] as char,
            displacement: get_sizes_for_header(&bytes[12..16]),
            length: bytes[16] as usize,
            decimal_places: bytes[17] as usize,
        }
    }
}

fn get_date_for_header(bytes: &[u8]) -> String {
    format!("{}.{}.{}", bytes[2], bytes[1], bytes[0] as usize + 1900)
}

fn get_sizes_for_header(bytes: &[u8]) -> usize {
    let mut i = 0;
    let mut result = 0;
    while i < bytes.len() {
        result += bytes[i] as usize * ((256 as usize).pow(i as u32));
        i += 1;
    }
    result
}

fn get_fields(bytes: &[u8]) -> Vec<DbfFields> {
    let mut next_field_record_startbyte = 32;
    let field_definition_end_marker = 0x0D;
    let mut result = vec![];
    while *&bytes[next_field_record_startbyte] != field_definition_end_marker {
        let field =
            DbfFields::new(&bytes[next_field_record_startbyte..next_field_record_startbyte + 32]);
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
    String::from(result.trim_end_matches(';'))
}

fn get_record_as_csv(
    bytes: &[u8],
    fields: &Vec<DbfFields>,
    memos: &[u8],
    memo_blocksize: &u16,
) -> String {
    let mut result: String = String::from("");
    for field in fields {
        let content = get_field_content_as_string(
            &bytes[field.displacement..field.displacement + field.length],
            &field.fieldtype,
            memos,
            memo_blocksize,
        );
        result.push_str(&content.trim());
        result.push(';');
    }
    String::from(result.trim_end_matches(';'))
}

fn get_memo_content(bytes: &[u8], block: u32, memo_blocksize: &u16) -> String {
    let startbyte = (block * *memo_blocksize as u32) as usize;
    let length = u32::from_be_bytes(bytes[startbyte + 4..startbyte + 8].try_into().unwrap());
    latin1_to_string(&bytes[startbyte + 8..startbyte + 8 + length as usize])
}

fn get_field_content_as_string(
    bytes: &[u8],
    fieldtype: &char,
    memos: &[u8],
    memo_blocksize: &u16,
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
            let mut block_number: u32 = 0;
            if bytes.len() == 4 {
                block_number = u32::from_le_bytes(bytes.try_into().unwrap());
            } else {
                let block_string = latin1_to_string(bytes);
                block_number = u32::from_str_radix(&block_string, 10).unwrap();
            }
            if block_number == 0 {
                String::from("")
            } else {
                get_memo_content(memos, block_number, memo_blocksize)
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

fn latin1_to_string(latin1_data: &[u8]) -> String {
    latin1_data
        .iter()
        .filter(|b| **b != 0)
        .map(|&c| c as char)
        .collect()
}

fn main() {
    let dbffile = std::fs::read("c:/Users/Hagen/RustProjects/dbfstuff/testdata/ama.dbf").unwrap();
    let memofile = std::fs::read("c:/Users/Hagen/RustProjects/dbfstuff/testdata/ama.fpt").unwrap();
    let header = DbfHeader::new(&dbffile[0..32]);
    let fields = get_fields(&dbffile);
    let field_header = get_field_header_as_csv(&fields);
    let memo_header = MemoHeader::new(&memofile[0..512]);
    println!("{:?}", memo_header);
    println!("{}", field_header);
    let mut linenumber = 0;
    //println!("{}", header._last_update);
    while linenumber < header.records {
        let startbyte =
            (header.bytes_header as u32 + linenumber * header.bytes_record as u32) as usize;
        let endbyte = (startbyte + header.bytes_record as usize) as usize;

        println!(
            "Zeile {}: {:?}",
            linenumber,
            get_record_as_csv(
                &dbffile[startbyte..endbyte],
                &fields,
                &memofile,
                &memo_header.block_size
            )
        );
        //let s = String::from_utf8_lossy(&dbffile[startbyte..endbyte]);
        //println!("Als UTF8: {}", s);
        linenumber += 1;
    }
}
