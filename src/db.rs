use std::sync::{Arc, Mutex};
use std::thread;
use duckdb::{Connection, ToSql};
use crate::parcel_record::ParcelRecord;
use crate::pretty_print_table::pretty_print_table;

pub fn save_to_duckdb(db_path: &str, records: &[ParcelRecord]) -> duckdb::Result<()> {
    // Create a new DuckDB connection
    let conn = Arc::new(Mutex::new(Connection::open(db_path)?));

    // Create the table if it doesn't exist
    {
        let conn = conn.lock().unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS parcel (
                id DOUBLE,
                full_owner_name TEXT,
                parcel_id TEXT,
                deeded_acre DOUBLE,
                land_use TEXT,
                land_appraised DOUBLE,
                building_appraised DOUBLE,
                total_appraised DOUBLE
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
            let conn = conn.lock().unwrap();
            let mut appender = conn.appender("parcel").expect("Failed to create appender");

            for record in chunk {
                let values: [&dyn ToSql; 8] = [
                    &record.id,
                    &record.full_owner_name.as_deref(),
                    &record.parcel_id.as_deref(),
                    &record.deeded_acre,
                    &record.land_use.as_deref(),
                    &record.land_appraised,
                    &record.building_appraised,
                    &record.total_appraised,
                ];

                appender.append_row(&values).expect("Failed to append row");
            }

            appender.flush().expect("Failed to flush appender");
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
