use prettytable::{format, Cell, Row, Table};
use crate::parcel_record::ParcelRecord;

// A generic function to pretty print any tabular data
pub fn pretty_print_table<T>(
    headers: Vec<&str>,
    records: &[T],
    get_row_data: impl Fn(&T) -> Vec<String>,
) {
    // Use prettytable to display the data
    let mut table = Table::new();

    // Set table format
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    // Add headers
    table.set_titles(Row::new(
        headers.into_iter().map(|h| Cell::new(h)).collect(),
    ));

    // Add rows to the table
    for record in records {
        let row_data = get_row_data(record);
        let cells: Vec<Cell> = row_data.into_iter().map(|data| Cell::new(&data)).collect();
        table.add_row(Row::new(cells));
    }

    // Print the table
    table.printstd();
}


pub fn pretty_print_parcel_records(records: &[ParcelRecord]) {
    // Define headers specific to ParcelRecord for use in the table
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

    // Callback function to get row data for ParcelRecord
    let get_row_data = |record: &ParcelRecord| -> Vec<String> {
        vec![
            record.id.map_or("".to_string(), |v| v.to_string()),
            record.full_owner_name.as_deref().unwrap_or("").to_string(),
            record.parcel_id.as_deref().unwrap_or("").to_string(),
            record.deeded_acre
                .map_or("".to_string(), |v| format!("{:.2}", v)),
            record.land_use.as_deref().unwrap_or("").to_string(),
            record.land_appraised
                .map_or("".to_string(), |v| format!("{:.2}", v)),
            record.building_appraised
                .map_or("".to_string(), |v| format!("{:.2}", v)),
            record.total_appraised
                .map_or("".to_string(), |v| format!("{:.2}", v)),
        ]
    };

    // Use the generic function to pretty print the parcel records
    pretty_print_table(headers, &records, get_row_data);
}


pub fn pretty_print_5(records: &mut Vec<ParcelRecord>) {
    // Use prettytable to display the data
    let mut table = Table::new();

    // Set table format
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    // Add headers
    table.set_titles(Row::new(vec![
        Cell::new("ID"),
        Cell::new("Owner"),
        Cell::new("Parcel ID"),
        Cell::new("Deeded Acre"),
        Cell::new("Land Use"),
        Cell::new("Land Appraised"),
        Cell::new("Building Appraised"),
        Cell::new("Total Appraised"),
    ]));

    // Add first 5 records to the table
    for record in records.iter().take(5) {
        table.add_row(Row::new(vec![
            Cell::new(&record.id.map_or("".to_string(), |v| v.to_string())),
            Cell::new(record.full_owner_name.as_deref().unwrap_or("")),
            Cell::new(record.parcel_id.as_deref().unwrap_or("")),
            Cell::new(&record.deeded_acre.map_or("".to_string(), |v| format!("{:.2}", v))),
            Cell::new(record.land_use.as_deref().unwrap_or("")),
            Cell::new(&record.land_appraised.map_or("".to_string(), |v| format!("{:.2}", v))),
            Cell::new(&record.building_appraised.map_or("".to_string(), |v| format!("{:.2}", v))),
            Cell::new(&record.total_appraised.map_or("".to_string(), |v| format!("{:.2}", v))),
        ]));
    }

    // Print the table
    table.printstd();
}