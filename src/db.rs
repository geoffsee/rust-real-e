use std::sync::{Arc, Mutex};
use std::thread;
use duckdb::{Connection, ToSql};
use crate::data_mapper::parcel_record_2_sql;
use crate::parcel_record::ParcelRecord;

pub fn save_to_duckdb(db_path: &str, records: &[ParcelRecord]) -> duckdb::Result<()> {
    // Create a new DuckDB connection
    let conn = Arc::new(Mutex::new(create_connection(db_path)?));

    // Ensure the parcel table exists
    create_parcel_table(&conn)?;

    // Process records in chunks and save them to the database
    let chunk_size = 256;
    let record_chunks: Vec<&[ParcelRecord]> = records.chunks(chunk_size).collect();
    save_records_in_chunks(&conn, &record_chunks);

    println!("Saved records to DuckDB");

    Ok(())
}

// Function to create a new DuckDB connection
fn create_connection(db_path: &str) -> duckdb::Result<Connection> {
    Connection::open(db_path)
}

// Function to create the parcel table if it doesn't exist
fn create_parcel_table(conn: &Arc<Mutex<Connection>>) -> duckdb::Result<()> {
    let conn = conn.lock().unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS parcel (
            id DOUBLE,
            clean_parcel TEXT,
            county_code DOUBLE,
            county_name TEXT,
            parcel_id TEXT,
            district_code DOUBLE,
            district_name TEXT,
            map TEXT,
            parcel_number TEXT,
            suffix TEXT,
            legal_description TEXT,
            legal_description_1 TEXT,
            legal_description_2 TEXT,
            full_legal_description TEXT,
            deeded_acre DOUBLE,
            calculated_acre DOUBLE,
            tax_year DOUBLE,
            tax_district TEXT,
            tax_class TEXT,
            deed_book TEXT,
            deed_page TEXT,
            property_class TEXT,
            property_type TEXT,
            owner_1 TEXT,
            owner_2 TEXT,
            full_owner_name TEXT,
            owner_address TEXT,
            owner_address_1 TEXT,
            owner_address_2 TEXT,
            owner_city TEXT,
            owner_state TEXT,
            owner_zip TEXT,
            care_of TEXT,
            full_owner_address TEXT,
            new_owner TEXT,
            new_owner_address TEXT,
            new_owner_address_1 TEXT,
            new_owner_address_2 TEXT,
            full_new_owner TEXT,
            new_deed_book TEXT,
            new_deed_page TEXT,
            physical_number DOUBLE,
            physical_direction TEXT,
            physical_street TEXT,
            physical_suffix TEXT,
            physical_unit_type TEXT,
            physical_city TEXT,
            physical_zip TEXT,
            physical_unit_id TEXT,
            full_physical_address TEXT,
            occupancy_description TEXT,
            hazard_occupancy TEXT,
            land_use TEXT,
            land_use_code TEXT,
            year_built DOUBLE,
            grade TEXT,
            style_code TEXT,
            style_description TEXT,
            commercial DOUBLE,
            stories DOUBLE,
            commercial_type_1 TEXT,
            basement_type TEXT,
            exterior_wall TEXT,
            exterior_1 TEXT,
            construction TEXT,
            total_rooms DOUBLE,
            use_type TEXT,
            business_license DOUBLE,
            structure_area DOUBLE,
            cubic_feet DOUBLE,
            units DOUBLE,
            commercial_type_2 DOUBLE,
            card DOUBLE,
            cards DOUBLE,
            dwelling_value DOUBLE,
            commercial_type_3 DOUBLE,
            other_building DOUBLE,
            land_appraised DOUBLE,
            building_appraised DOUBLE,
            total_appraised DOUBLE,
            sams_address TEXT,
            sams_city TEXT,
            sams_state TEXT,
            sams_zip TEXT,
            pre_address_number TEXT,
            address_number TEXT,
            address_number_suffix TEXT,
            full_name TEXT,
            unit_type TEXT,
            unit_id TEXT,
            alternate_unit_type TEXT,
            alternate_unit_id TEXT,
            flood_risks TEXT,
            oby_count DOUBLE,
            sale_price DOUBLE,
            developer_id TEXT,
            building_permits DOUBLE
        )",
        [],
    )?;
    Ok(())
}

// Function to save records in chunks using multiple threads
fn save_records_in_chunks(conn: &Arc<Mutex<Connection>>, record_chunks: &[&[ParcelRecord]]) {
    let mut threads = Vec::new();

    for chunk in record_chunks {
        let conn = Arc::clone(&conn);
        let chunk = chunk.to_vec();

        let handle = thread::spawn(move || save_record_chunk(&conn, &chunk));
        threads.push(handle);
    }

    for handle in threads {
        handle.join().expect("Thread panicked");
    }
}

// Function to save a single chunk of records
fn save_record_chunk(conn: &Arc<Mutex<Connection>>, chunk: &[ParcelRecord]) {
    let conn = conn.lock().expect("Failed to acquire the lock on connection");
    let mut appender = conn.appender("parcel").expect("Failed to create appender");

    for record in chunk {
        let values = parcel_record_2_sql(record);
        if let Err(e) = appender.append_row(&values[..]) {
            // Early return to avoid poisoning the mutex
            return;
        }
    }

    if let Err(e) = appender.flush() {
        eprintln!("Failed to flush appender: {:?}", e);
    }
}
