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
            records: get_sizes_for_header(&bytes[4..8]) as u32,
            bytes_header: get_sizes_for_header(&bytes[8..10]) as u16,
            bytes_record: get_sizes_for_header(&bytes[10..12]) as u16,
        }
    }
}

enum FieldTypes {}

#[derive(Debug)]
struct DbfFields {
    fieldname: String,
    fieldtype: char, // FieldTypes,
    displacement: u32,
    length: u8,
    decimal_places: u8,
}

impl DbfFields {
    fn new(bytes: &[u8]) -> Self {
        Self {
            fieldname: latin1_to_string(&bytes[0..11]),
            fieldtype: bytes[11] as char,
            displacement: get_sizes_for_header(&bytes[12..16]) as u32,
            length: bytes[16],
            decimal_places: bytes[17],
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

fn latin1_to_string(latin1_data: &[u8]) -> String {
    latin1_data.iter().map(|&c| c as char).collect()
}

fn main() {
    let dbffile = std::fs::read("c:/Users/Hagen/RustProjects/dbfstuff/testdata/amf.dbf").unwrap();
    let header = DbfHeader::new(&dbffile[0..32]);
    let mut next_field_record_startbyte = 32;
    while *&dbffile[next_field_record_startbyte] != 13 {
        let field =
            DbfFields::new(&dbffile[next_field_record_startbyte..next_field_record_startbyte + 32]);
        println!("{:?}", field);
        next_field_record_startbyte += 32;
    }
    let mut linenumber = 0;
    println!("{}", header._last_update);
    while linenumber < header.records {
        let startbyte =
            (header.bytes_header as u32 + linenumber * header.bytes_record as u32) as usize;
        let endbyte = (startbyte + header.bytes_record as usize) as usize;
        println!(
            "Zeile {}: {:?}",
            linenumber,
            latin1_to_string(&dbffile[startbyte..endbyte])
        );
        //let s = String::from_utf8_lossy(&dbffile[startbyte..endbyte]);
        //println!("Als UTF8: {}", s);
        linenumber += 1;
    }
}
