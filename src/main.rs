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
    let field_definition_end_marker = 13;
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

fn get_record_as_csv(bytes: &[u8], fields: &Vec<DbfFields>) -> String {
    let mut result: String = String::from("");
    for field in fields {
        let content =
            latin1_to_string(&bytes[field.displacement..field.displacement + field.length]);
        result.push_str(&content.trim());
        result.push(';');
    }
    String::from(result.trim_end_matches(';'))
}

fn latin1_to_string(latin1_data: &[u8]) -> String {
    latin1_data
        .iter()
        .filter(|b| **b != 0)
        .map(|&c| c as char)
        .collect()
}

fn main() {
    let dbffile = std::fs::read("c:/Users/Hagen/RustProjects/dbfstuff/testdata/amf.dbf").unwrap();
    let header = DbfHeader::new(&dbffile[0..32]);
    let fields = get_fields(&dbffile);
    let field_header = get_field_header_as_csv(&fields);
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
            get_record_as_csv(&dbffile[startbyte..endbyte], &fields)
        );
        //let s = String::from_utf8_lossy(&dbffile[startbyte..endbyte]);
        //println!("Als UTF8: {}", s);
        linenumber += 1;
    }
}
