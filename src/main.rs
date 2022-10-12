use chrono::{Datelike, NaiveDate};
use clap::Parser;
use serde::Serialize;
use std::{collections::HashMap, error::Error, fs::File, io::Write, ops::Range};

// https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd

// Challenges:
// - no square footage data, so the calculated aggregate value will include all property sizes
// - small number of properties sold in a postcode in a given month, especially of a particular type (flat, terraced, etc.),
//   so it's better to use either larger regions or larger time periods

const DEFAULT_FILE_NAME: &str = "pp-complete.csv";
const DATE_FORMAT: &str = "%Y-%m-%d %H:%M";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value_t = DEFAULT_FILE_NAME.to_string())]
    file: String,
    // #[arg(short, long, default_value_t = 1)]
    // count: u8,
}

#[derive(Hash, Clone, Copy, Eq, PartialEq, Debug, Serialize)]
enum PropertyType {
    Detached,
    SemiDetached,
    Terraced,
    Flat,
    Other,
}

#[derive(Hash, Clone, Copy, Eq, PartialEq, Debug, Serialize)]
enum PropertyAge {
    New,
    Old,
}

#[derive(Debug)]
enum DurationOfTransfer {
    Freehold,
    Leasehold,
}

#[derive(Debug)]
struct Entry {
    price: i32,
    date: NaiveDate,
    postcode: String, // postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset
    property_type: PropertyType,
    property_age: PropertyAge,
    duration: DurationOfTransfer,
}

#[derive(Debug, Serialize)]
struct YearEntry {
    #[serde(skip_serializing)]
    prices: HashMap<PropertyType, HashMap<PropertyAge, Vec<i32>>>,
    year: i32,
}

#[derive(Debug, Default, Serialize)]
struct PriceBucket {
    count: usize,
    median: f32,
    range: Range<i32>,
}

fn to_price_bucket(prices: &mut Vec<i32>) -> PriceBucket {
    let mut result = PriceBucket::default();

    prices.sort_unstable();
    result.count = prices.len();
    result.median = prices[prices.len() / 2] as f32;
    let min = *prices.iter().min().unwrap_or(&0);
    let max = *prices.iter().max().unwrap_or(&0);
    result.range = min..max;

    result
}

fn process_year_entry(entry: &mut YearEntry) -> ProcessedYearEntry {
    let mut result = ProcessedYearEntry {
        year: entry.year,
        buckets: HashMap::new(),
    };

    for (property_type, age_entries) in entry.prices.iter_mut() {
        for (property_age, prices) in age_entries.iter_mut() {
            result
                .buckets
                .entry(*property_type)
                .or_insert(HashMap::from([(*property_age, to_price_bucket(prices))]))
                .entry(*property_age)
                .or_insert(to_price_bucket(prices));
        }
    }

    result
}

#[derive(Debug, Serialize)]
struct ProcessedYearEntries {
    year: i32,
    postcodes: HashMap<String, Vec<ProcessedYearEntry>>,
}

#[derive(Debug, Serialize)]
struct ProcessedYearEntry {
    year: i32, // duplicate the year in this struct to make it easier to read the resulting JSON
    buckets: HashMap<PropertyType, HashMap<PropertyAge, PriceBucket>>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    println!("Parsing CSV file...");

    let mut reader = csv::Reader::from_path(args.file)?;
    let mut entries: Vec<Entry> = Vec::new();

    for result in reader.records() {
        let record = result?;

        let price: i32 = record.get(1).unwrap().parse().unwrap();
        let date = NaiveDate::parse_from_str(record.get(2).unwrap(), DATE_FORMAT)?;
        let postcode = record
            .get(3)
            .unwrap()
            .split(" ")
            .nth(0)
            .unwrap()
            .to_string();
        let property_type = to_property_type(record.get(4).unwrap());
        let property_age = to_property_age(record.get(5).unwrap());
        let duration = to_duration_of_transfer(record.get(6).unwrap());

        let entry = Entry {
            price,
            date,
            postcode,
            property_type,
            property_age,
            duration,
        };
        entries.push(entry);
    }

    println!("Sorting entries by date...");

    entries.sort_unstable_by(|entry1, entry2| entry1.date.cmp(&entry2.date));

    println!("Calculating stats per postcode per year...");

    let mut year: i32 = entries[0].date.year();
    let mut postcode_year_prices: HashMap<String, YearEntry> = HashMap::new();

    let mut out_file = File::create("stats.json")?;
    out_file.write("[".as_bytes())?;
    let mut it = entries.iter().peekable();
    while let Some(entry) = it.next() {
        if entry.date.year() != year || it.peek().is_none() {
            let mut processed_year_entries: HashMap<String, Vec<ProcessedYearEntry>> =
                HashMap::new();
            for (postcode, year_entry) in postcode_year_prices.iter_mut() {
                let processed_year_entry = process_year_entry(year_entry);
                let postcode_processed_year_entries = processed_year_entries
                    .entry(postcode.clone())
                    .or_insert(vec![]);
                postcode_processed_year_entries.push(processed_year_entry);
            }
            serde_json::to_writer(
                &out_file,
                &ProcessedYearEntries {
                    year,
                    postcodes: processed_year_entries,
                },
            )?;
            out_file.write(",".as_bytes())?;

            year = entry.date.year();
            postcode_year_prices.clear();
        }

        let prices = postcode_year_prices
            .entry(entry.postcode.clone())
            .or_insert(YearEntry {
                prices: HashMap::from([(
                    entry.property_type,
                    HashMap::from([(entry.property_age, vec![])]),
                )]),
                year: entry.date.year(),
            })
            .prices
            .entry(entry.property_type)
            .or_insert(HashMap::from([(entry.property_age, vec![])]))
            .entry(entry.property_age)
            .or_insert(vec![]);

        prices.push(entry.price);
    }
    serde_json::to_writer(&out_file, &postcode_year_prices)?;
    out_file.write("]".as_bytes())?;

    Ok(())
}

fn to_property_type(str: &str) -> PropertyType {
    match str {
        "D" => PropertyType::Detached,
        "S" => PropertyType::SemiDetached,
        "T" => PropertyType::Terraced,
        "F" => PropertyType::Flat,
        _ => PropertyType::Other, // e.g. property comprises more than one large parcel of land
    }
}

fn to_property_age(str: &str) -> PropertyAge {
    match str {
        "Y" => PropertyAge::New,
        _ => PropertyAge::Old,
    }
}

fn to_duration_of_transfer(str: &str) -> DurationOfTransfer {
    match str {
        "F" => DurationOfTransfer::Freehold,
        _ => DurationOfTransfer::Leasehold, // leases of 7 years or less are not recorded in Price Paid Dataset
    }
}
