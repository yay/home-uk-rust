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

#[derive(Debug, PartialEq)]
enum DurationOfTransfer {
    Freehold,
    Leasehold,
}

#[derive(Debug)]
struct Entry {
    price: i32,
    date: NaiveDate,
    address: String,
    postcode: String, // postcodes can be reallocated and these changes are not reflected in the Price Paid Dataset
    property_type: PropertyType,
    property_age: PropertyAge,
    duration: DurationOfTransfer,
}

#[derive(Debug, Serialize)]
struct YearEntry {
    #[serde(skip_serializing)]
    properties: HashMap<PropertyType, HashMap<PropertyAge, Vec<Property>>>,
    year: i32,
}

#[derive(Debug, Default, Serialize)]
struct PriceBucket {
    count: usize,
    median: f32,
    range: Range<i32>,
    properties: Vec<Property>,
}

#[derive(Debug, Default, Serialize, Clone)]
struct Property {
    address: String,
    price: i32,
}

fn to_price_bucket(properties: &mut Vec<Property>) -> PriceBucket {
    let mut result = PriceBucket::default();

    let mut prices: Vec<i32> = properties.iter().map(|p| p.price).collect();
    prices.sort_unstable();
    result.count = prices.len();
    result.median = find_median(&prices);
    let min = *prices.iter().min().unwrap_or(&0);
    let max = *prices.iter().max().unwrap_or(&0);
    result.range = min..max;
    result.properties = properties
        .iter()
        .filter(|p| p.price >= 300_000 && p.price <= 800_000)
        .cloned()
        .collect();

    result
}

fn find_median(prices: &Vec<i32>) -> f32 {
    let len = prices.len();
    if len >= 2 && len % 2 == 0 {
        let middle = len / 2;
        (prices[middle - 1] + prices[middle]) as f32 / 2f32
    } else {
        prices[len / 2] as f32
    }
}

fn process_year_entry(entry: &mut YearEntry) -> ProcessedYearEntry {
    let mut result = ProcessedYearEntry {
        year: entry.year,
        buckets: HashMap::new(),
    };

    for (property_type, age_entries) in entry.properties.iter_mut() {
        for (property_age, properties) in age_entries.iter_mut() {
            result
                .buckets
                .entry(*property_type)
                .or_insert(HashMap::new())
                .entry(*property_age)
                .or_insert(to_price_bucket(properties));
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

        let date = NaiveDate::parse_from_str(record.get(2).unwrap(), DATE_FORMAT)?;
        if date.year() < 2021 {
            continue;
        }
        let duration = to_duration_of_transfer(record.get(6).unwrap());
        if duration != DurationOfTransfer::Leasehold {
            continue;
        }
        let postcode = record.get(3).unwrap().split(" ").nth(0).unwrap();
        if !INCLUDED_POSTCODES.contains(&postcode) {
            continue;
        }
        let property_type = to_property_type(record.get(4).unwrap());
        if property_type == PropertyType::Other {
            continue;
        }

        let price: i32 = record.get(1).unwrap().parse().unwrap();
        let property_age = to_property_age(record.get(5).unwrap());
        let paon = record.get(7).unwrap();
        let saon = record.get(8).unwrap();
        let street = record.get(9).unwrap();
        let city = record.get(11).unwrap();
        let mut address = "".to_string();
        if !paon.is_empty() {
            address += paon;
            address += ", ";
        }
        if !saon.is_empty() {
            address += saon;
            address += ", ";
        }
        address += street;
        address += ", ";
        address += city;

        let entry = Entry {
            price,
            date,
            address,
            postcode: postcode.to_string(),
            property_type,
            property_age,
            duration,
        };
        entries.push(entry);
    }

    println!("Sorting and filtering entries...");

    entries.sort_unstable_by(|entry1, entry2| entry1.date.cmp(&entry2.date));
    // It's less pretty but faster to filter in the reader loop above than here.
    // Given the huge size of our CSV, any performance improvement is welcome.
    // entries = entries
    //     .into_iter()
    //     .filter(|entry| entry.date.year() >= 2021)
    //     .filter(|entry| entry.duration == DurationOfTransfer::Freehold)
    //     .filter(|entry| INCLUDED_POSTCODES.contains(&entry.postcode.as_str()))
    //     .collect();

    println!("Calculating stats per postcode per year...");

    let mut year: i32 = entries[0].date.year();
    let mut postcode_year_entries: HashMap<String, YearEntry> = HashMap::new();

    let mut out_file = File::create("stats.json")?;
    out_file.write("[".as_bytes())?;
    let mut it = entries.iter().peekable();
    while let Some(entry) = it.next() {
        if entry.date.year() != year || it.peek().is_none() {
            let mut processed_year_entries: HashMap<String, Vec<ProcessedYearEntry>> =
                HashMap::new();
            for (postcode, year_entry) in postcode_year_entries.iter_mut() {
                let processed_year_entry = process_year_entry(year_entry);
                let postcode_processed_year_entries = processed_year_entries
                    .entry(postcode.clone())
                    .or_insert(vec![]);
                postcode_processed_year_entries.push(processed_year_entry);
            }
            println!("Saving stats for year: {:?}", year);
            serde_json::to_writer(
                &out_file,
                &ProcessedYearEntries {
                    year,
                    postcodes: processed_year_entries,
                },
            )?;
            out_file.write(",".as_bytes())?;

            year = entry.date.year();
            postcode_year_entries.clear();
        }

        let properties = postcode_year_entries
            .entry(entry.postcode.clone())
            .or_insert(YearEntry {
                properties: HashMap::new(),
                year: entry.date.year(),
            })
            .properties
            .entry(entry.property_type)
            .or_insert(HashMap::new())
            .entry(entry.property_age)
            .or_insert(vec![]);

        properties.push(Property {
            address: entry.address.clone(),
            price: entry.price,
        });
    }
    serde_json::to_writer(&out_file, &postcode_year_entries)?;
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

// Greater London is too big and includes fairly remote areas.
const LONDON_POSTCODES: &'static [&'static str] = &[
    "EC1A", "EC1M", "EC1N", "EC1P", "EC1R", "EC1V", "EC1Y", "EC2A", "EC2M", "EC2N", "EC2P", "EC2R",
    "EC2V", "EC2Y", "EC3A", "EC3M", "EC3N", "EC3P", "EC3R", "EC3V", "EC4A", "EC4M", "EC4N", "EC4P",
    "EC4R", "EC4V", "EC4Y", "WC1A", "WC1B", "WC1E", "WC1H", "WC1N", "WC1R", "WC1V", "WC1X", "WC2A",
    "WC2B", "WC2E", "WC2H", "WC2N", "WC2R", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9",
    "E10", "E11", "E12", "E13", "E14", "E15", "E16", "E17", "E18", "E19", "E20", "N1", "N2", "N3",
    "N4", "N5", "N6", "N7", "N8", "N9", "N10", "N11", "N12", "N13", "N14", "N15", "N16", "N17",
    "N18", "N19", "N20", "N21", "N22", "NW1", "NW2", "NW3", "NW4", "NW5", "NW6", "NW7", "NW8",
    "NW9", "NW10", "NW11", "SE1", "SE2", "SE3", "SE4", "SE5", "SE6", "SE7", "SE8", "SE9", "SE10",
    "SE11", "SE12", "SE13", "SE14", "SE15", "SE16", "SE17", "SE18", "SE19", "SE20", "SE21", "SE22",
    "SE23", "SE24", "SE25", "SE26", "SE27", "SE28", "SW1", "SW2", "SW3", "SW4", "SW5", "SW6",
    "SW7", "SW8", "SW9", "SW10", "SW11", "SW12", "SW13", "SW14", "SW15", "SW16", "SW17", "SW18",
    "SW19", "SW20", "W1", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9", "W10", "W11", "W12",
    "W13", "W14",
];

// Inner London still includes relatively far away areas (like E4 and N4).
// https://en.wikipedia.org/wiki/Inner_London

const CENTRAL_LONDON_POSTCODES: &'static [&'static str] = &[
    "EC1A", "EC1M", "EC1N", "EC1R", "EC1V", "EC1Y", "EC2A", "EC2M", "EC2N", "EC2R", "EC2V", "EC2Y",
    "EC3A", "EC3M", "EC3N", "EC3R", "EC3V", "EC4A", "EC4M", "EC4N", "EC4R", "EC4V", "EC4Y", "WC1A",
    "WC1B", "WC1E", "WC1H", "WC1N", "WC1R", "WC1V", "WC1X", "WC2A", "WC2B", "WC2E", "WC2H", "WC2N",
    "WC2R", "E1", "E2", "E3", "E8", "E9", "E14", "E15", "E16", "N1", "N5", "N8", "N16", "NW1",
    "NW3", "NW5", "NW6", "NW8", "NW10", "SE1", "SE3", "SE4", "SE5", "SE7", "SE8", "SE10", "SE11",
    "SE13", "SE14", "SE15", "SE16", "SE17", "SE18", "SW1", "SW2", "SW3", "SW4", "SW5", "SW6",
    "SW7", "SW8", "SW9", "SW10", "SW11", "W1", "W2", "W8", "W9", "W10", "W11", "W14",
];

const DESIRABLE_POSTCODES: &'static [&'static str] = &["E14", "E16", "SE1", "SE16"];

const INCLUDED_POSTCODES: &'static [&'static str] = DESIRABLE_POSTCODES;
