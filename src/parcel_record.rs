use std::fmt;
use duckdb::ToSql;
use crate::pretty_print_table::pretty_print_table;

#[derive(Debug,Clone)]
pub(crate) struct ParcelRecord {
    pub id: Option<f64>,
    pub clean_parcel: Option<String>,
    pub county_code: Option<f64>,
    pub county_name: Option<String>,
    pub parcel_id: Option<String>,
    pub district_code: Option<f64>,
    pub district_name: Option<String>,
    pub map: Option<String>,
    pub parcel_number: Option<String>,
    pub suffix: Option<String>,
    pub legal_description: Option<String>,
    pub legal_description_1: Option<String>,
    pub legal_description_2: Option<String>,
    pub full_legal_description: Option<String>,
    pub deeded_acre: Option<f64>,
    pub calculated_acre: Option<f64>,
    pub tax_year: Option<f64>,
    pub tax_district: Option<String>,
    pub tax_class: Option<String>,
    pub deed_book: Option<String>,
    pub deed_page: Option<String>,
    pub property_class: Option<String>,
    pub property_type: Option<String>,
    pub owner_1: Option<String>,
    pub owner_2: Option<String>,
    pub full_owner_name: Option<String>,
    pub owner_address: Option<String>,
    pub owner_address_1: Option<String>,
    pub owner_address_2: Option<String>,
    pub owner_city: Option<String>,
    pub owner_state: Option<String>,
    pub owner_zip: Option<String>,
    pub care_of: Option<String>,
    pub full_owner_address: Option<String>,
    pub new_owner: Option<String>,
    pub new_owner_address: Option<String>,
    pub new_owner_address_1: Option<String>,
    pub new_owner_address_2: Option<String>,
    pub full_new_owner: Option<String>,
    pub new_deed_book: Option<String>,
    pub new_deed_page: Option<String>,
    pub physical_number: Option<f64>,
    pub physical_direction: Option<String>,
    pub physical_street: Option<String>,
    pub physical_suffix: Option<String>,
    pub physical_unit_type: Option<String>,
    pub physical_city: Option<String>,
    pub physical_zip: Option<String>,
    pub physical_unit_id: Option<String>,
    pub full_physical_address: Option<String>,
    pub occupancy_description: Option<String>,
    pub hazard_occupancy: Option<String>,
    pub land_use: Option<String>,
    pub land_use_code: Option<String>,
    pub year_built: Option<f64>,
    pub grade: Option<String>,
    pub style_code: Option<String>,
    pub style_description: Option<String>,
    pub commercial: Option<f64>,
    pub stories: Option<f64>,
    pub commercial_type_1: Option<String>,
    pub basement_type: Option<String>,
    pub exterior_wall: Option<String>,
    pub exterior_1: Option<String>,
    pub construction: Option<String>,
    pub total_rooms: Option<f64>,
    pub use_type: Option<String>,
    pub business_license: Option<f64>,
    pub structure_area: Option<f64>,
    pub cubic_feet: Option<f64>,
    pub units: Option<f64>,
    pub commercial_type_2: Option<f64>,
    pub card: Option<f64>,
    pub cards: Option<f64>,
    pub dwelling_value: Option<f64>,
    pub commercial_type_3: Option<f64>,
    pub other_building: Option<f64>,
    pub land_appraised: Option<f64>,
    pub building_appraised: Option<f64>,
    pub total_appraised: Option<f64>,
    pub sams_address: Option<String>,
    pub sams_city: Option<String>,
    pub sams_state: Option<String>,
    pub sams_zip: Option<String>,
    pub pre_address_number: Option<String>,
    pub address_number: Option<String>,
    pub address_number_suffix: Option<String>,
    pub full_name: Option<String>,
    pub unit_type: Option<String>,
    pub unit_id: Option<String>,
    pub alternate_unit_type: Option<String>,
    pub alternate_unit_id: Option<String>,
    pub flood_risks: Option<String>,
    pub oby_count: Option<f64>,
    pub sale_price: Option<f64>,
    pub developer_id: Option<String>,
    pub building_permits: Option<f64>
}

// Implement Display for ParcelRecord
impl fmt::Display for ParcelRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id.map_or("".to_string(), |v| v.to_string()))
    }
}

pub fn pretty_print_parcel_records(records: &[ParcelRecord]) {
    // Define headers specific to ParcelRecord
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