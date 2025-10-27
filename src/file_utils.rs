// Function to open a geospatial file using GDAL DATASET
use gdal::Dataset;
use std::error::Error;
use std::path::Path;

pub fn open_dataset<P: AsRef<Path>>(file_path: P) -> Result<Dataset, Box<dyn Error>> {
    let dataset = Dataset::open(file_path)?;
    Ok(dataset)
}
