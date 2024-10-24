use std::error::Error;
use dbase::Reader;
use crate::data_mapper::map_record_to_parcel;
use crate::parcel_record::ParcelRecord;

pub fn read_dbf(path: String) -> duckdb::Result<Vec<ParcelRecord>, Box<dyn Error>> {
    // Open the DBF file
    let mut reader = Reader::from_path(path)?;

    // Access the header
    let header = reader.header();
    println!("DBF Version: {:?}", header.last_update);
    println!("Number of records: {}", header.num_records);

    // Read records and map them into ParcelRecord structs
    let mut records = Vec::new();

    for result in reader.iter_records() {
        let record = result?;
        let parcel_record = map_record_to_parcel(&record)?;
        records.push(parcel_record);
    }
    Ok(records)
}
