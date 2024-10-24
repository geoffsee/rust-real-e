use prettytable::{format, Cell, Row, Table};
use crate::parcel_record::ParcelRecord;

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