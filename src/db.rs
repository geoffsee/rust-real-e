use std::sync::{Arc, Mutex};
use std::thread;
use duckdb::{Connection, ToSql};
use crate::parcel_record::ParcelRecord;
use crate::pretty_print::pretty_print_table;

pub fn save_to_duckdb(db_path: &str, records: &[ParcelRecord]) -> duckdb::Result<()> {
    // Create a new DuckDB connection
    let conn = Arc::new(Mutex::new(Connection::open(db_path)?));

    // Create the table if it doesn't exist
    {
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
    }

    // Define the chunk size
    let chunk_size = 1000;
    let record_chunks: Vec<&[ParcelRecord]> = records.chunks(chunk_size).collect();

    let mut threads = Vec::new();

    for chunk in record_chunks {
        let conn = Arc::clone(&conn);
        let chunk = chunk.to_vec();

        let handle = thread::spawn(move || {
            let conn = conn.lock().expect("Failed to acquire the lock on connection");
            let mut appender = conn.appender("parcel").expect("Failed to create appender");

            for record in chunk {
                let values: Vec<&dyn ToSql> = vec![
                    &record.id,
                    &record.clean_parcel,
                    &record.county_code,
                    &record.county_name,
                    &record.parcel_id,
                    &record.district_code,
                    &record.district_name,
                    &record.map,
                    &record.parcel_number,
                    &record.suffix,
                    &record.legal_description,
                    &record.legal_description_1,
                    &record.legal_description_2,
                    &record.full_legal_description,
                    &record.deeded_acre,
                    &record.calculated_acre,
                    &record.tax_year,
                    &record.tax_district,
                    &record.tax_class,
                    &record.deed_book,
                    &record.deed_page,
                    &record.property_class,
                    &record.property_type,
                    &record.owner_1,
                    &record.owner_2,
                    &record.full_owner_name,
                    &record.owner_address,
                    &record.owner_address_1,
                    &record.owner_address_2,
                    &record.owner_city,
                    &record.owner_state,
                    &record.owner_zip,
                    &record.care_of,
                    &record.full_owner_address,
                    &record.new_owner,
                    &record.new_owner_address,
                    &record.new_owner_address_1,
                    &record.new_owner_address_2,
                    &record.full_new_owner,
                    &record.new_deed_book,
                    &record.new_deed_page,
                    &record.physical_number,
                    &record.physical_direction,
                    &record.physical_street,
                    &record.physical_suffix,
                    &record.physical_unit_type,
                    &record.physical_city,
                    &record.physical_zip,
                    &record.physical_unit_id,
                    &record.full_physical_address,
                    &record.occupancy_description,
                    &record.hazard_occupancy,
                    &record.land_use,
                    &record.land_use_code,
                    &record.year_built,
                    &record.grade,
                    &record.style_code,
                    &record.style_description,
                    &record.commercial,
                    &record.stories,
                    &record.commercial_type_1,
                    &record.basement_type,
                    &record.exterior_wall,
                    &record.exterior_1,
                    &record.construction,
                    &record.total_rooms,
                    &record.use_type,
                    &record.business_license,
                    &record.structure_area,
                    &record.cubic_feet,
                    &record.units,
                    &record.commercial_type_2,
                    &record.card,
                    &record.cards,
                    &record.dwelling_value,
                    &record.commercial_type_3,
                    &record.other_building,
                    &record.land_appraised,
                    &record.building_appraised,
                    &record.total_appraised,
                    &record.sams_address,
                    &record.sams_city,
                    &record.sams_state,
                    &record.sams_zip,
                    &record.pre_address_number,
                    &record.address_number,
                    &record.address_number_suffix,
                    &record.full_name,
                    &record.unit_type,
                    &record.unit_id,
                    &record.alternate_unit_type,
                    &record.alternate_unit_id,
                    &record.flood_risks,
                    &record.oby_count,
                    &record.sale_price,
                    &record.developer_id,
                    &record.building_permits,
                ];

                if let Err(e) = appender.append_row(&values[..]) {
                    eprintln!("Failed to append row: {:?}", e);
                    return; // Early return to avoid poisoning the mutex
                }
            }

            if let Err(e) = appender.flush() {
                eprintln!("Failed to flush appender: {:?}", e);
            }
        });

        threads.push(handle);
    }

    for handle in threads {
        handle.join().expect("Thread panicked");
    }
    println!("Saved records to duckdb");

    Ok(())
}

pub fn print_first_five_records() -> Result<(), Box<dyn std::error::Error>> {
    // Open a connection to the DuckDB database
    let conn = Connection::open("parcel_data.db")?;

    // Prepare the SQL query
    let mut stmt = conn.prepare("SELECT * FROM parcel LIMIT 5")?;

    // Execute the query and obtain an iterator over the results
    let mut rows = stmt.query([])?;

    // Iterate over the rows and print each one
    while let Some(row) = rows.next()? {
        // Extract each column from the row
        let col0: Option<f64> = row.get(0)?;
        let col1: Option<String> = row.get(1)?;
        let col2: Option<String> = row.get(2)?;
        let col3: Option<f64> = row.get(3)?;
        let col4: Option<String> = row.get(4)?;
        let col5: Option<f64> = row.get(5)?;
        let col6: Option<f64> = row.get(6)?;
        let col7: Option<f64> = row.get(7)?;

        // Print the tuple of column values
        println!(
            "{:?}",
            (col0, col1, col2, col3, col4, col5, col6, col7)
        );
    }

    Ok(())
}

pub fn get_names_with_most_parcels(limit: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Open a connection to the DuckDB database
    let conn = Connection::open("parcel_data.db")?;

    // Prepare the SQL query
    let query = format!(
        "SELECT
            full_owner_name,
            COUNT(*) AS parcel_count
        FROM
            parcel
        GROUP BY
            full_owner_name
        ORDER BY
            parcel_count DESC
        LIMIT
            {}",
        limit
    );

    // Prepare and execute the query
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query([])?;

    // Collect records into a vector
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        let full_owner_name: Option<String> = row.get(0)?;
        let parcel_count: i64 = row.get(1)?;

        records.push((full_owner_name, parcel_count));
    }

    // Define headers for printing
    let headers = vec!["Owner", "Parcel Count"];

    // Callback to generate row data
    let get_row_data = |record: &(Option<String>, i64)| -> Vec<String> {
        vec![
            record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
            record.1.to_string(),
        ]
    };

    // Print the table using the generic function
    pretty_print_table(headers, &records, get_row_data);

    Ok(())
}

pub fn get_total_appraised_value_per_owner(limit: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Open a connection to the DuckDB database
    let conn = Connection::open("parcel_data.db")?;

    // Prepare the SQL query
    let query = format!(
        "SELECT
            full_owner_name,
            SUM(total_appraised) AS total_appraised_value
        FROM
            parcel
        GROUP BY
            full_owner_name
        ORDER BY
            total_appraised_value DESC
        LIMIT
            {}",
        limit
    );

    // Prepare and execute the query
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query([])?;

    // Collect results into a vector of tuples
    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        let full_owner_name: Option<String> = row.get(0)?;
        let total_appraised_value: Option<f64> = row.get(1)?;
        results.push((
            full_owner_name.unwrap_or_else(|| "Unknown".to_string()),
            total_appraised_value.unwrap_or(0.0),
        ));
    }

    // Define headers for printing
    let headers = vec!["Owner", "Total Appraised Value"];

    // Callback to convert the result tuple into a vector of strings
    let get_row_data = |record: &(String, f64)| -> Vec<String> {
        vec![
            record.0.clone(),
            format!("{:.2}", record.1),
        ]
    };

    // Pretty print the table
    pretty_print_table(headers, &results, get_row_data);

    Ok(())
}

pub fn get_parcels_by_land_use(land_use_type: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Open a connection to the DuckDB database
    let conn = Connection::open("parcel_data.db")?;

    // Prepare the SQL query
    let query = "SELECT * FROM parcel WHERE land_use = ?";

    // Prepare and execute the query
    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query([land_use_type])?;

    // Collect records into a vector
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        let id: Option<f64> = row.get(0)?;
        let full_owner_name: Option<String> = row.get(1)?;
        let parcel_id: Option<String> = row.get(2)?;
        let deeded_acre: Option<f64> = row.get(3)?;
        let land_use: Option<String> = row.get(4)?;
        let land_appraised: Option<f64> = row.get(5)?;
        let building_appraised: Option<f64> = row.get(6)?;
        let total_appraised: Option<f64> = row.get(7)?;

        records.push((
            id,
            full_owner_name,
            parcel_id,
            deeded_acre,
            land_use,
            land_appraised,
            building_appraised,
            total_appraised,
        ));
    }

    // Define headers for printing
    let headers = vec![
        "ID",
        "Owner",
        "Parcel ID",
        "Deeded Acre",
        "Land Use",
        "Land Appraised",
        "Building Appraised",
        "Total Appraised",
    ];

    // Callback to generate row data
    let get_row_data = |record: &(
        Option<f64>,
        Option<String>,
        Option<String>,
        Option<f64>,
        Option<String>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
    )| -> Vec<String> {
        vec![
            record.0.map_or("".to_string(), |v| v.to_string()),
            record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
            record.2.clone().unwrap_or_else(|| "Unknown".to_string()),
            record.3.map_or("".to_string(), |v| format!("{:.2}", v)),
            record.4.clone().unwrap_or_else(|| "Unknown".to_string()),
            record.5.map_or("".to_string(), |v| format!("{:.2}", v)),
            record.6.map_or("".to_string(), |v| format!("{:.2}", v)),
            record.7.map_or("".to_string(), |v| format!("{:.2}", v)),
        ]
    };

    // Print the table using the generic function
    pretty_print_table(headers, &records, get_row_data);

    Ok(())
}
