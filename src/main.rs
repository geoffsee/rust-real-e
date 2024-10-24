mod parcel_record;
mod db;
mod data_mapper;
mod pretty_print;
mod dbf;
mod query;

use crate::parcel_record::ParcelRecord;
use crate::dbf::read;
use duckdb::{Result};
use std::error::Error;
use crate::pretty_print::print_parcel_table_schema;
use crate::query::find_potential_redemption_properties;

const dbfFile: &str = "test_data/Berkeley_02_WVGISTCTax_2024_UTM83/ParcelSummary_2024_Berkeley.dbf";
const duckdbFile: &str = "parcel_data.db";

fn main() -> Result<(), Box<dyn Error>> {


    // Read the parcel data
    let mut records: Vec<ParcelRecord> = read(String::from(dbfFile))?;

    // Save to DuckDB
    db::save_to_duckdb(duckdbFile, &mut records).expect("Fatal error saving to duckdb");
    print_parcel_table_schema().expect("no duckdb file found");
    // // Print first five records as a test to make sure things are working
    // query::print_first_five_records()?;

    println!("\n\n\nGetting names with most parcels\n");
    query::get_names_with_most_parcels(7)?;

    println!("\n\n\nGet the top owners by total appraised value\n");
    query::get_total_appraised_value_per_owner(7)?;

    println!("\n\n\nfind_potential_redemption_properties\n");
    find_potential_redemption_properties()?;



    Ok(())
}
