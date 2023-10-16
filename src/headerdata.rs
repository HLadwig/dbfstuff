#[derive(Debug)]
pub struct DbfHeader {
    _version: u8,
    _last_update: String,
    pub records: u32,
    pub bytes_header: u16,
    pub bytes_record: u16,
    pub language: u8,
}

impl DbfHeader {
    pub fn new(bytes: &[u8]) -> Self {
        Self {
            _version: bytes[0],
            _last_update: get_date_for_header(&bytes[1..4]),
            records: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            bytes_header: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            bytes_record: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
            language: bytes[29],
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum MemoFileType {
    Old,
    New,
}

#[derive(Debug)]
pub struct MemoHeader {
    pub memo_type: MemoFileType,
    pub block_size: u16,
}

impl MemoHeader {
    pub fn new(bytes: &[u8]) -> Self {
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
pub struct DbfFields {
    fieldname: String,
    pub fieldtype: char, // FieldTypes,
    pub displacement: u32,
    pub length: usize,
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

fn latin1_to_string(latin1_data: &[u8]) -> String {
    latin1_data
        .iter()
        .filter(|b| **b != 0)
        .map(|&c| c as char)
        .collect()
}

fn get_date_for_header(bytes: &[u8]) -> String {
    format!("{}.{}.{}", bytes[2], bytes[1], bytes[0] as usize + 1900)
}

pub fn get_fields(bytes: &[u8]) -> Vec<DbfFields> {
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

pub fn get_field_header_as_csv(fields: &Vec<DbfFields>) -> String {
    let mut result: String = String::from("");
    for field in fields {
        result.push_str(&field.fieldname);
        result.push(';');
    }
    result.push_str("\r\n");
    result
}
