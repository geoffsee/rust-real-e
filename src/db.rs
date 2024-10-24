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


// pub fn detect_inflated_property_appraisals() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             p1.*
//         FROM
//             parcel p1
//         JOIN (
//             SELECT
//                 property_class,
//                 property_type,
//                 AVG(total_appraised) AS avg_appraised
//             FROM
//                 parcel
//             WHERE
//                 total_appraised IS NOT NULL
//             GROUP BY
//                 property_class,
//                 property_type
//         ) p2 ON
//             p1.property_class = p2.property_class AND
//             p1.property_type = p2.property_type
//         WHERE
//             p1.total_appraised > p2.avg_appraised * 1.5
//             AND p1.total_appraised IS NOT NULL;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         // Extract all necessary columns
//         let id: Option<f64> = row.get("id")?;
//         let full_owner_name: Option<String> = row.get("full_owner_name")?;
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let deeded_acre: Option<f64> = row.get("deeded_acre")?;
//         let land_use: Option<String> = row.get("land_use")?;
//         let land_appraised: Option<f64> = row.get("land_appraised")?;
//         let building_appraised: Option<f64> = row.get("building_appraised")?;
//         let total_appraised: Option<f64> = row.get("total_appraised")?;
//         // Add other fields as necessary
//
//         records.push((
//             id,
//             full_owner_name,
//             parcel_id,
//             deeded_acre,
//             land_use,
//             land_appraised,
//             building_appraised,
//             total_appraised,
//         ));
//     }
//
//     let headers = vec![
//         "ID",
//         "Owner",
//         "Parcel ID",
//         "Deeded Acre",
//         "Land Use",
//         "Land Appraised",
//         "Building Appraised",
//         "Total Appraised",
//     ];
//
//     let get_row_data = |record: &(
//         Option<f64>,
//         Option<String>,
//         Option<String>,
//         Option<f64>,
//         Option<String>,
//         Option<f64>,
//         Option<f64>,
//         Option<f64>,
//     )| -> Vec<String> {
//         vec![
//             record.0.map_or("".to_string(), |v| v.to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.3.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.4.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.5.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.6.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.7.map_or("".to_string(), |v| format!("{:.2}", v)),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }


// pub fn detect_ghost_owners() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             *
//         FROM
//             parcel
//         WHERE
//             (new_owner IS NULL OR new_owner = '') OR
//             (new_owner_address IS NULL OR new_owner_address = '');
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         // Extract relevant columns
//         let id: Option<f64> = row.get("id")?;
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let new_owner: Option<String> = row.get("new_owner")?;
//         let new_owner_address: Option<String> = row.get("new_owner_address")?;
//         let deed_book: Option<String> = row.get("deed_book")?;
//         let deed_page: Option<String> = row.get("deed_page")?;
//         // Add other fields as necessary
//
//         records.push((
//             id,
//             parcel_id,
//             new_owner,
//             new_owner_address,
//             deed_book,
//             deed_page,
//         ));
//     }
//
//     let headers = vec![
//         "ID",
//         "Parcel ID",
//         "New Owner",
//         "New Owner Address",
//         "Deed Book",
//         "Deed Page",
//     ];
//
//     let get_row_data = |record: &(
//         Option<f64>,
//         Option<String>,
//         Option<String>,
//         Option<String>,
//         Option<String>,
//         Option<String>,
//     )| -> Vec<String> {
//         vec![
//             record.0.map_or("".to_string(), |v| v.to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.3.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.4.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.5.clone().unwrap_or_else(|| "Unknown".to_string()),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_rapid_flipping() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             parcel_id,
//             tax_year,
//             COUNT(DISTINCT new_owner) AS ownership_changes
//         FROM
//             parcel
//         WHERE
//             tax_year IS NOT NULL
//         GROUP BY
//             parcel_id,
//             tax_year
//         HAVING
//             COUNT(DISTINCT new_owner) > 3;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let tax_year: Option<f64> = row.get("tax_year")?;
//         let ownership_changes: i64 = row.get("ownership_changes")?;
//
//         records.push((parcel_id, tax_year, ownership_changes));
//     }
//
//     let headers = vec!["Parcel ID", "Tax Year", "Ownership Changes"];
//
//     let get_row_data = |record: &(Option<String>, Option<f64>, i64)| -> Vec<String> {
//         vec![
//             record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.1.map_or("Unknown".to_string(), |v| v.to_string()),
//             record.2.to_string(),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_land_grab_discrepancies() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             legal_description,
//             COUNT(*) AS occurrences
//         FROM
//             parcel
//         WHERE
//             legal_description IS NOT NULL
//         GROUP BY
//             legal_description
//         HAVING
//             COUNT(*) > 5;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         let legal_description: Option<String> = row.get("legal_description")?;
//         let occurrences: i64 = row.get("occurrences")?;
//
//         records.push((legal_description, occurrences));
//     }
//
//     let headers = vec!["Legal Description", "Occurrences"];
//
//     let get_row_data = |record: &(Option<String>, i64)| -> Vec<String> {
//         vec![
//             record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.1.to_string(),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_undervalued_property_assessments() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             p1.*
//         FROM
//             parcel p1
//         JOIN (
//             SELECT
//                 county_code,
//                 property_class,
//                 property_type,
//                 AVG(total_appraised) AS avg_appraised
//             FROM
//                 parcel
//             WHERE
//                 total_appraised IS NOT NULL
//             GROUP BY
//                 county_code,
//                 property_class,
//                 property_type
//         ) p2 ON
//             p1.county_code = p2.county_code AND
//             p1.property_class = p2.property_class AND
//             p1.property_type = p2.property_type
//         WHERE
//             p1.total_appraised < p2.avg_appraised * 0.7
//             AND p1.total_appraised IS NOT NULL;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         // Extract all necessary columns
//         let id: Option<f64> = row.get("id")?;
//         let full_owner_name: Option<String> = row.get("full_owner_name")?;
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let deeded_acre: Option<f64> = row.get("deeded_acre")?;
//         let land_use: Option<String> = row.get("land_use")?;
//         let land_appraised: Option<f64> = row.get("land_appraised")?;
//         let building_appraised: Option<f64> = row.get("building_appraised")?;
//         let total_appraised: Option<f64> = row.get("total_appraised")?;
//         // Add other fields as necessary
//
//         records.push((
//             id,
//             full_owner_name,
//             parcel_id,
//             deeded_acre,
//             land_use,
//             land_appraised,
//             building_appraised,
//             total_appraised,
//         ));
//     }
//
//     let headers = vec![
//         "ID",
//         "Owner",
//         "Parcel ID",
//         "Deeded Acre",
//         "Land Use",
//         "Land Appraised",
//         "Building Appraised",
//         "Total Appraised",
//     ];
//
//     let get_row_data = |record: &(
//         Option<f64>,
//         Option<String>,
//         Option<String>,
//         Option<f64>,
//         Option<String>,
//         Option<f64>,
//         Option<f64>,
//         Option<f64>,
//     )| -> Vec<String> {
//         vec![
//             record.0.map_or("".to_string(), |v| v.to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.3.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.4.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.5.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.6.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.7.map_or("".to_string(), |v| format!("{:.2}", v)),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_suspicious_zoning_changes() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             p1.*
//         FROM
//             parcel p1
//         JOIN (
//             SELECT
//                 parcel_id,
//                 COUNT(DISTINCT land_use) AS land_use_changes,
//                 COUNT(DISTINCT land_use_code) AS land_use_code_changes
//             FROM
//                 parcel
//             GROUP BY
//                 parcel_id
//             HAVING
//                 COUNT(DISTINCT land_use) > 1 OR
//                 COUNT(DISTINCT land_use_code) > 1
//         ) p2 ON
//             p1.parcel_id = p2.parcel_id
//         WHERE
//             p1.land_use <> p2.land_use OR
//             p1.land_use_code <> p2.land_use_code;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         // Extract relevant columns
//         let id: Option<f64> = row.get("id")?;
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let land_use: Option<String> = row.get("land_use")?;
//         let land_use_code: Option<String> = row.get("land_use_code")?;
//         // Add other fields as necessary
//
//         records.push((
//             id,
//             parcel_id,
//             land_use,
//             land_use_code,
//         ));
//     }
//
//     let headers = vec![
//         "ID",
//         "Parcel ID",
//         "Land Use",
//         "Land Use Code",
//     ];
//
//     let get_row_data = |record: &(Option<f64>, Option<String>, Option<String>, Option<String>)| -> Vec<String> {
//         vec![
//             record.0.map_or("".to_string(), |v| v.to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.3.clone().unwrap_or_else(|| "Unknown".to_string()),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_irregular_building_permits() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             *
//         FROM
//             parcel
//         WHERE
//             (year_built < 1800 OR year_built > EXTRACT(YEAR FROM CURRENT_DATE))
//             OR
//             (stories > 10)
//             OR
//             (structure_area > 10000);
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         // Extract relevant columns
//         let id: Option<f64> = row.get("id")?;
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let year_built: Option<f64> = row.get("year_built")?;
//         let stories: Option<f64> = row.get("stories")?;
//         let structure_area: Option<f64> = row.get("structure_area")?;
//         // Add other fields as necessary
//
//         records.push((
//             id,
//             parcel_id,
//             year_built,
//             stories,
//             structure_area,
//         ));
//     }
//
//     let headers = vec![
//         "ID",
//         "Parcel ID",
//         "Year Built",
//         "Stories",
//         "Structure Area",
//     ];
//
//     let get_row_data = |record: &(
//         Option<f64>,
//         Option<String>,
//         Option<f64>,
//         Option<f64>,
//         Option<f64>,
//     )| -> Vec<String> {
//         vec![
//             record.0.map_or("".to_string(), |v| v.to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.map_or("".to_string(), |v| v.to_string()),
//             record.3.map_or("".to_string(), |v| v.to_string()),
//             record.4.map_or("".to_string(), |v| format!("{:.2}", v)),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_multiple_properties_high_risk() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             owner_1,
//             owner_2,
//             COUNT(*) AS property_count
//         FROM
//             parcel
//         WHERE
//             flood_risks = 'High'
//         GROUP BY
//             owner_1,
//             owner_2
//         HAVING
//             COUNT(*) > 3;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         let owner_1: Option<String> = row.get("owner_1")?;
//         let owner_2: Option<String> = row.get("owner_2")?;
//         let property_count: i64 = row.get("property_count")?;
//
//         records.push((owner_1, owner_2, property_count));
//     }
//
//     let headers = vec!["Owner 1", "Owner 2", "Property Count"];
//
//     let get_row_data = |record: &(Option<String>, Option<String>, i64)| -> Vec<String> {
//         vec![
//             record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.to_string(),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_unusual_business_license_patterns() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     // Assuming there is a BusinessLicenses table; if not, adjust accordingly
//     let query = "
//         SELECT
//             p.*
//         FROM
//             parcel p
//         LEFT JOIN
//             BusinessLicenses b ON p.business_license = b.license_number
//         WHERE
//             p.business_license IS NOT NULL
//             AND b.license_number IS NULL;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         // Extract relevant columns
//         let id: Option<f64> = row.get("id")?;
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let business_license: Option<f64> = row.get("business_license")?;
//         let owner_1: Option<String> = row.get("owner_1")?;
//         let owner_2: Option<String> = row.get("owner_2")?;
//         let property_type: Option<String> = row.get("property_type")?;
//         let commercial_type_1: Option<String> = row.get("commercial_type_1")?;
//         // Add other fields as necessary
//
//         records.push((
//             id,
//             parcel_id,
//             business_license,
//             owner_1,
//             owner_2,
//             property_type,
//             commercial_type_1,
//         ));
//     }
//
//     let headers = vec![
//         "ID",
//         "Parcel ID",
//         "Business License",
//         "Owner 1",
//         "Owner 2",
//         "Property Type",
//         "Commercial Type 1",
//     ];
//
//     let get_row_data = |record: &(
//         Option<f64>,
//         Option<String>,
//         Option<f64>,
//         Option<String>,
//         Option<String>,
//         Option<String>,
//         Option<String>,
//     )| -> Vec<String> {
//         vec![
//             record.0.map_or("".to_string(), |v| v.to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.map_or("".to_string(), |v| v.to_string()),
//             record.3.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.4.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.5.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.6.clone().unwrap_or_else(|| "Unknown".to_string()),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_inconsistent_owner_addresses() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             owner_1,
//             owner_2,
//             COUNT(DISTINCT full_owner_address) AS address_changes
//         FROM
//             parcel
//         GROUP BY
//             owner_1,
//             owner_2
//         HAVING
//             COUNT(DISTINCT full_owner_address) > 3;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         let owner_1: Option<String> = row.get("owner_1")?;
//         let owner_2: Option<String> = row.get("owner_2")?;
//         let address_changes: i64 = row.get("address_changes")?;
//
//         records.push((owner_1, owner_2, address_changes));
//     }
//
//     let headers = vec!["Owner 1", "Owner 2", "Address Changes"];
//
//     let get_row_data = |record: &(Option<String>, Option<String>, i64)| -> Vec<String> {
//         vec![
//             record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.to_string(),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_disproportionate_property_values() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             *
//         FROM
//             parcel
//         WHERE
//             (calculated_acre IS NOT NULL AND total_appraised < calculated_acre * 1000)
//             OR
//             (structure_area IS NOT NULL AND total_appraised < structure_area * 50);
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         // Extract relevant columns
//         let id: Option<f64> = row.get("id")?;
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let calculated_acre: Option<f64> = row.get("calculated_acre")?;
//         let structure_area: Option<f64> = row.get("structure_area")?;
//         let total_appraised: Option<f64> = row.get("total_appraised")?;
//         // Add other fields as necessary
//
//         records.push((
//             id,
//             parcel_id,
//             calculated_acre,
//             structure_area,
//             total_appraised,
//         ));
//     }
//
//     let headers = vec![
//         "ID",
//         "Parcel ID",
//         "Calculated Acre",
//         "Structure Area",
//         "Total Appraised",
//     ];
//
//     let get_row_data = |record: &(
//         Option<f64>,
//         Option<String>,
//         Option<f64>,
//         Option<f64>,
//         Option<f64>,
//     )| -> Vec<String> {
//         vec![
//             record.0.map_or("".to_string(), |v| v.to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.3.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.4.map_or("".to_string(), |v| format!("{:.2}", v)),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_frequent_property_class_changes() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             parcel_id,
//             tax_district,
//             tax_year,
//             COUNT(DISTINCT property_class) AS class_changes,
//             COUNT(DISTINCT tax_class) AS tax_class_changes
//         FROM
//             parcel
//         GROUP BY
//             parcel_id,
//             tax_district,
//             tax_year
//         HAVING
//             COUNT(DISTINCT property_class) > 1
//             OR
//             COUNT(DISTINCT tax_class) > 1;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let tax_district: Option<String> = row.get("tax_district")?;
//         let tax_year: Option<f64> = row.get("tax_year")?;
//         let class_changes: i64 = row.get("class_changes")?;
//         let tax_class_changes: i64 = row.get("tax_class_changes")?;
//
//         records.push((parcel_id, tax_district, tax_year, class_changes, tax_class_changes));
//     }
//
//     let headers = vec![
//         "Parcel ID",
//         "Tax District",
//         "Tax Year",
//         "Property Class Changes",
//         "Tax Class Changes",
//     ];
//
//     let get_row_data = |record: &(
//         Option<String>,
//         Option<String>,
//         Option<f64>,
//         i64,
//         i64,
//     )| -> Vec<String> {
//         vec![
//             record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.1.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.2.map_or("Unknown".to_string(), |v| v.to_string()),
//             record.3.to_string(),
//             record.4.to_string(),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_insurance_fraud_high_flood() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             parcel_id,
//             COUNT(*) AS sale_count,
//             MAX(sale_price) AS max_sale_price,
//             MIN(sale_price) AS min_sale_price,
//             AVG(sale_price) AS avg_sale_price
//         FROM
//             parcel
//         WHERE
//             flood_risks = 'High'
//         GROUP BY
//             parcel_id
//         HAVING
//             COUNT(*) > 5
//             OR
//             MAX(sale_price) > AVG(sale_price) * 2;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let sale_count: i64 = row.get("sale_count")?;
//         let max_sale_price: Option<f64> = row.get("max_sale_price")?;
//         let min_sale_price: Option<f64> = row.get("min_sale_price")?;
//         let avg_sale_price: Option<f64> = row.get("avg_sale_price")?;
//
//         records.push((
//             parcel_id,
//             sale_count,
//             max_sale_price,
//             min_sale_price,
//             avg_sale_price,
//         ));
//     }
//
//     let headers = vec![
//         "Parcel ID",
//         "Sale Count",
//         "Max Sale Price",
//         "Min Sale Price",
//         "Avg Sale Price",
//     ];
//
//     let get_row_data = |record: &(
//         Option<String>,
//         i64,
//         Option<f64>,
//         Option<f64>,
//         Option<f64>,
//     )| -> Vec<String> {
//         vec![
//             record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.1.to_string(),
//             record.2.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.3.map_or("".to_string(), |v| format!("{:.2}", v)),
//             record.4.map_or("".to_string(), |v| format!("{:.2}", v)),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_disproportionate_building_permits() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             developer_id,
//             COUNT(*) AS permit_count
//         FROM
//             parcel
//         WHERE
//             building_permits IS NOT NULL
//         GROUP BY
//             developer_id
//         HAVING
//             COUNT(*) > 50;
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         let developer_id: Option<String> = row.get("developer_id")?;
//         let permit_count: i64 = row.get("permit_count")?;
//
//         records.push((developer_id, permit_count));
//     }
//
//     let headers = vec!["Developer ID", "Permit Count"];
//
//     let get_row_data = |record: &(Option<String>, i64)| -> Vec<String> {
//         vec![
//             record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.1.to_string(),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }

// pub fn detect_unexplained_unit_acquisitions() -> Result<(), Box<dyn std::error::Error>> {
//     let conn = Connection::open("parcel_data.db")?;
//
//     let query = "
//         SELECT
//             parcel_id,
//             COUNT(*) AS unit_count,
//             COUNT(DISTINCT unit_type) AS distinct_unit_types,
//             COUNT(DISTINCT unit_id) AS distinct_unit_ids
//         FROM
//             parcel
//         GROUP BY
//             parcel_id
//         HAVING
//             COUNT(*) > 10
//             AND
//             (COUNT(DISTINCT unit_type) > 1 OR COUNT(DISTINCT unit_id) > 10);
//     ";
//
//     let mut stmt = conn.prepare(query)?;
//     let mut rows = stmt.query([])?;
//
//     let mut records = Vec::new();
//     while let Some(row) = rows.next()? {
//         let parcel_id: Option<String> = row.get("parcel_id")?;
//         let unit_count: i64 = row.get("unit_count")?;
//         let distinct_unit_types: i64 = row.get("distinct_unit_types")?;
//         let distinct_unit_ids: i64 = row.get("distinct_unit_ids")?;
//
//         records.push((
//             parcel_id,
//             unit_count,
//             distinct_unit_types,
//             distinct_unit_ids,
//         ));
//     }
//
//     let headers = vec![
//         "Parcel ID",
//         "Unit Count",
//         "Distinct Unit Types",
//         "Distinct Unit IDs",
//     ];
//
//     let get_row_data = |record: &(
//         Option<String>,
//         i64,
//         i64,
//         i64,
//     )| -> Vec<String> {
//         vec![
//             record.0.clone().unwrap_or_else(|| "Unknown".to_string()),
//             record.1.to_string(),
//             record.2.to_string(),
//             record.3.to_string(),
//         ]
//     };
//
//     pretty_print_table(headers, &records, get_row_data);
//
//     Ok(())
// }
