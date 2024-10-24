use std::error::Error;
use dbase::{FieldValue, Record};
use crate::parcel_record::ParcelRecord;

pub fn map_record_to_parcel(record: &Record) -> duckdb::Result<ParcelRecord, Box<dyn Error>> {
    // Helper function to extract a String field
    fn get_string_field(record: &Record, field_name: &str) -> Option<String> {
        match record.get(field_name) {
            Some(FieldValue::Character(Some(value))) => Some(value.clone()),
            _ => None,
        }
    }

    // Helper function to extract a Numeric field as f64
    fn get_numeric_field(record: &Record, field_name: &str) -> Option<f64> {
        match record.get(field_name) {
            Some(FieldValue::Numeric(Some(value))) => Some(*value),
            Some(FieldValue::Float(Some(value))) => Some(*value as f64),
            _ => None,
        }
    }

    let parcel_record = ParcelRecord {
        id: get_numeric_field(record, "ID"),

        clean_parcel: None,
        county_code: None,
        full_owner_name: get_string_field(record, "FullOwnerN"),

        owner_address: None,
        owner_address_1: None,
        owner_address_2: None,
        owner_city: None,
        owner_state: None,
        owner_zip: None,
        care_of: None,
        full_owner_address: None,
        new_owner: None,
        new_owner_address: None,
        new_owner_address_1: None,
        new_owner_address_2: None,
        full_new_owner: None,
        new_deed_book: None,
        new_deed_page: None,
        physical_number: None,
        physical_direction: None,
        physical_street: None,
        physical_suffix: None,
        physical_unit_type: None,
        physical_city: None,
        physical_zip: None,
        physical_unit_id: None,
        full_physical_address: None,
        occupancy_description: None,
        parcel_id: get_string_field(record, "ParcelID"),

        district_code: None,
        district_name: None,
        map: None,
        parcel_number: None,
        suffix: None,
        legal_description: None,
        legal_description_1: None,
        legal_description_2: None,
        full_legal_description: None,
        deeded_acre: get_numeric_field(record, "DeededAcre"),

        calculated_acre: None,
        tax_year: None,
        tax_district: None,
        tax_class: None,
        deed_book: None,
        deed_page: None,
        property_class: None,
        property_type: None,
        owner_1: None,
        land_use: get_string_field(record, "LandUse"),

        land_use_code: None,
        year_built: None,
        grade: None,
        style_code: None,
        style_description: None,
        commercial: None,
        stories: None,
        commercial_type_1: None,
        basement_type: None,
        exterior_wall: None,
        exterior_1: None,
        construction: None,
        total_rooms: None,
        use_type: None,
        business_license: None,
        structure_area: None,
        cubic_feet: None,
        units: None,
        commercial_type_2: None,
        card: None,
        cards: None,
        dwelling_value: None,
        commercial_type_3: None,
        other_building: None,
        land_appraised: get_numeric_field(record, "LandApprai"),
        building_appraised: get_numeric_field(record, "BuildingAp"),
        total_appraised: get_numeric_field(record, "TotalAppra"),

        sams_address: None,
        sams_city: None,
        sams_state: None,
        sams_zip: None,
        pre_address_number: None,
        address_number: None,
        address_number_suffix: None,
        full_name: None,
        unit_type: None,
        unit_id: None,
        alternate_unit_type: None,
        alternate_unit_id: None,
        flood_risks: None,
        county_name: None,
        owner_2: None,
        hazard_occupancy: None,
        oby_count: None,
    };

    Ok(parcel_record)
}