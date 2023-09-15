use std::str;

#[derive(Debug)]
struct DbfHeader {
    version: u8,
    last_update: String,
    records: u32,
    bytes_header: u16,
    bytes_record: u16,
}

impl DbfHeader {
    fn new(bytes: &[u8]) -> Self {
        Self {
            version: bytes[0],
            last_update: String::from(""),
            records: getu32_from_lsbf(&bytes[4..8]),
            bytes_header: getu16_from_lsbf(&bytes[8..10]),
            bytes_record: getu16_from_lsbf(&bytes[10..12]),
        }
    }
}

fn getu16_from_lsbf(bytes: &[u8]) -> u16 {
    let mut i = 0;
    let mut result = 0;
    while i < bytes.len() {
        result += bytes[i] as u16 * ((256 as u16).pow(i as u32));
        i += 1;
    }
    result
}

fn getu32_from_lsbf(bytes: &[u8]) -> u32 {
    let mut i = 0;
    let mut result = 0;
    while i < bytes.len() {
        result += bytes[i] as u32 * ((256 as u32).pow(i as u32));
        i += 1;
    }
    result
}

fn main() {
    //let mut dbffile = File::open("c:/Users/Hagen/RustProjects/dbfstuff/testdata/amf.dbf").unwrap();
    //let mut dbffile_content: Vec<u8> = vec![];
    //dbffile.read_to_end(&mut dbffile_content).unwrap();
    let dbffile = std::fs::read("c:/Users/Hagen/RustProjects/dbfstuff/testdata/amf.dbf").unwrap();
    let header = DbfHeader::new(&dbffile[0..32]);
    let mut linenumber = 0;
    while linenumber < header.records {
        let startbyte =
            (header.bytes_header as u32 + linenumber * header.bytes_record as u32) as usize;
        let endbyte = (startbyte + header.bytes_record as usize) as usize;
        println!("Zeile {}: {:?}", linenumber, &dbffile[startbyte..endbyte]);
        linenumber += 1;
    }

    println!("{:?}", header);
    println!("{:x?}", &dbffile[0..32]);
    println!("{:?}", &dbffile[0..32]);
    println!("{:b}", &dbffile[1]);
    println!("{}", &dbffile[4].reverse_bits());
    println!("{:b}", &dbffile[4].reverse_bits());
    println!("{}", &dbffile[5].reverse_bits());
    println!("{:b}", &dbffile[5].reverse_bits());
    println!("{:b}", &dbffile[6].reverse_bits());
    println!("{:b}", 407);
}
