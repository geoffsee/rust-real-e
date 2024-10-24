mod parcel_record;
mod db;
mod data_mapper;
mod pretty_print;
mod read_dbf;
mod pretty_print_table;

use crate::read_dbf::read_dbf;
use duckdb::{Result, ToSql};
use std::error::Error;
use crate::parcel_record::ParcelRecord;
use crate::pretty_print::pretty_print_5;

const dbfFile: &str = "test_data/Berkeley_02_WVGISTCTax_2024_UTM83/ParcelSummary_2024_Berkeley.dbf";
const duckdbFile: &str = "parcel_data.db";

fn main() -> Result<(), Box<dyn Error>> {
    // read the parcel data
    let mut records: Vec<ParcelRecord> = read_dbf(String::from(dbfFile))?;

    // save
    db::save_to_duckdb(duckdbFile, &mut records)?;

    // pretty_print_5(&mut records);

    db::print_first_five_records()?;

    db::get_names_with_most_parcels(5)?;

    // Get the top 10 names with the most parcels
    db::get_names_with_most_parcels(10)?;

    // Get the top 10 owners by total appraised value
    db::get_total_appraised_value_per_owner(10)?;

    // Find parcels with a specific land use type
    db::get_parcels_by_land_use("Residential")?;

    Ok(())
}







