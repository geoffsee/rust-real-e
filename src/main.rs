mod parcel_record;
mod db;
mod data_mapper;
mod pretty_print;
mod read_dbf;
mod pretty_print_table;

use crate::parcel_record::ParcelRecord;
use crate::read_dbf::read_dbf;
use duckdb::{Result, ToSql};
use std::error::Error;

const dbfFile: &str = "test_data/Berkeley_02_WVGISTCTax_2024_UTM83/ParcelSummary_2024_Berkeley.dbf";
const duckdbFile: &str = "parcel_data.db";

fn main() -> Result<(), Box<dyn Error>> {
    // Read the parcel data
    let mut records: Vec<ParcelRecord> = read_dbf(String::from(dbfFile))?;

    // Save to DuckDB
    db::save_to_duckdb(duckdbFile, &mut records)?;

    // Print first five records as a test to make sure things are working
    db::print_first_five_records()?;
    println!("\n\n\nEverything seems to be setup correctly! Executing Analysis...");


    println!("\n\n\nGetting names with most parcels\n");
    db::get_names_with_most_parcels(100)?;

    println!("\n\n\nGet the top owners by total appraised value\n");
    db::get_total_appraised_value_per_owner(100)?;

    // println!("\n\n\nDetecting inflated property appraisals\n");
    // db::detect_inflated_property_appraisals()?;

    // println!("\n\n\nDetecting ghost owners in property transfers\n");
    // db::detect_ghost_owners()?;

    // println!("\n\n\nDetecting rapid flipping of parcels\n");
    // db::detect_rapid_flipping()?;

    // println!("\n\n\nDetecting discrepancies in legal descriptions for land grab schemes\n");
    // db::detect_land_grab_discrepancies()?;

    // println!("\n\n\nDetecting undervalued property assessments\n");
    // db::detect_undervalued_property_assessments()?;

    // println!("\n\n\nDetecting suspicious zoning changes\n");
    // db::detect_suspicious_zoning_changes()?;

    // println!("\n\n\nDetecting irregularities in building permits\n");
    // db::detect_irregular_building_permits()?;

    // println!("\n\n\nDetecting multiple properties owned by individuals in high-risk areas\n");
    // db::detect_multiple_properties_high_risk()?;

    // println!("\n\n\nDetecting unusual patterns in business licenses linked to property ownership\n");
    // db::detect_unusual_business_license_patterns()?;

    // println!("\n\n\nDetecting inconsistent owner addresses indicating concealed identities\n");
    // db::detect_inconsistent_owner_addresses()?;

    // println!("\n\n\nDetecting disproportionate property values compared to physical attributes\n");
    // db::detect_disproportionate_property_values()?;

    // println!("\n\n\nDetecting frequent changes in property classification for tax evasion\n");
    // db::detect_frequent_property_class_changes()?;

    // println!("\n\n\nDetecting patterns of sale in high flood risk areas suggesting insurance fraud\n");
    // db::detect_insurance_fraud_high_flood()?;

    // println!("\n\n\nDetecting disproportionate allocation of building permits to specific developers\n");
    // db::detect_disproportionate_building_permits()?;

    // println!("\n\n\nDetecting unexplained acquisition of multiple units in a single parcel\n");
    // db::detect_unexplained_unit_acquisitions()?;

    Ok(())
}
