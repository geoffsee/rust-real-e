use prettytable::{format, Cell, Row, Table};

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
