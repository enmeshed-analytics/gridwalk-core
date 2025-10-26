use gdal::Dataset;
use gdal::vector::{Layer, LayerAccess};
use std::collections::HashMap;

/// Selector for identifying a layer by either index or name
#[derive(Debug, Clone)]
pub enum LayerSelector {
    Index(usize),
    Name(String),
}

impl LayerSelector {
    fn get_layer<'a>(
        &self,
        dataset: &'a Dataset,
    ) -> Result<Layer<'a>, Box<dyn std::error::Error + Send>> {
        match self {
            LayerSelector::Index(index) => dataset.layer(*index).map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to get layer by index {}: {}", index, e),
                )) as Box<dyn std::error::Error + Send>
            }),
            LayerSelector::Name(name) => dataset.layer_by_name(name).map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to get layer by name '{}': {}", name, e),
                )) as Box<dyn std::error::Error + Send>
            }),
        }
    }
}

/// Represents a single feature ready for PostGIS insertion
#[derive(Debug, Clone)]
pub struct Feature {
    pub geometry_wkb: Vec<u8>,               // WKB-encoded geometry
    pub srid: Option<i32>,                   // Spatial reference ID
    pub fields: HashMap<String, FieldValue>, // All attribute fields
}

/// Represents different field value types that can be inserted into PostGIS
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Text(String),
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Date(String),     // ISO 8601 date string
    DateTime(String), // ISO 8601 datetime string
    Binary(Vec<u8>),  // For binary data
    Null,             // Explicit null value
}

/// Iterator for reading features from a GDAL layer
pub struct FeatureIterator {
    dataset: Dataset,
    layer_selector: LayerSelector,
    current_index: u64,
    feature_count: u64,
}

impl FeatureIterator {
    pub fn new(
        dataset: Dataset,
        layer_selector: LayerSelector,
    ) -> Result<Self, Box<dyn std::error::Error + Send>> {
        let layer = layer_selector.get_layer(&dataset)?;
        let feature_count = layer.feature_count() as u64;
        drop(layer); // Release the layer reference

        Ok(Self {
            dataset,
            layer_selector,
            current_index: 0,
            feature_count,
        })
    }

    /// Convenience constructor for layer by index
    pub fn new_by_index(
        dataset: Dataset,
        index: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send>> {
        Self::new(dataset, LayerSelector::Index(index))
    }

    /// Convenience constructor for layer by name
    pub fn new_by_name(
        dataset: Dataset,
        name: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send>> {
        Self::new(dataset, LayerSelector::Name(name))
    }
}

impl Iterator for FeatureIterator {
    type Item = Result<Feature, Box<dyn std::error::Error + Send>>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get the layer for this iteration

        let layer = match self.layer_selector.get_layer(&self.dataset) {
            Ok(layer) => layer,
            Err(e) => return Some(Err(e)), // This already returns Send-compatible error
        };

        loop {
            if self.current_index >= self.feature_count {
                return None;
            }

            match layer.feature(self.current_index) {
                Some(gdal_feature) => {
                    self.current_index += 1;

                    // Now we can safely access layer info since we're not using the iterator
                    let layer_defn = layer.defn();
                    let srid = layer.spatial_ref().and_then(|srs| srs.auth_code().ok());
                    let feature = convert_gdal_feature(&gdal_feature, &layer_defn, srid);
                    return Some(feature);
                }
                None => {
                    self.current_index += 1;
                    // Continue loop to try next feature
                }
            }
        }
    }
}

// Start: Convert a GDAL feature into our Feature struct
fn convert_gdal_feature(
    gdal_feature: &gdal::vector::Feature,
    layer_defn: &gdal::vector::Defn,
    srid: Option<i32>,
) -> Result<Feature, Box<dyn std::error::Error + Send>> {
    // Extract geometry as WKB
    let geometry_wkb = gdal_feature
        .geometry()
        .ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Feature has no geometry",
            )) as Box<dyn std::error::Error + Send>
        })?
        .wkb()
        .map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to convert geometry to WKB: {}", e),
            )) as Box<dyn std::error::Error + Send>
        })?;

    // Extract all field values
    let mut fields = HashMap::new();

    for (field_idx, field_defn) in layer_defn.fields().enumerate() {
        let field_name = field_defn.name();
        let field_value = match gdal_feature.field(field_idx).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to read field {}: {}", field_name, e),
            )) as Box<dyn std::error::Error + Send>
        })? {
            Some(gdal::vector::FieldValue::StringValue(s)) => FieldValue::Text(s),
            Some(gdal::vector::FieldValue::IntegerValue(i)) => FieldValue::Integer(i.into()),
            Some(gdal::vector::FieldValue::Integer64Value(i)) => FieldValue::Integer(i),
            Some(gdal::vector::FieldValue::RealValue(f)) => FieldValue::Real(f),
            Some(gdal::vector::FieldValue::DateValue(date)) => {
                FieldValue::Date(date.format("%Y-%m-%d").to_string())
            }
            Some(gdal::vector::FieldValue::DateTimeValue(datetime)) => {
                FieldValue::DateTime(datetime.format("%Y-%m-%dT%H:%M:%S").to_string())
            }
            Some(gdal::vector::FieldValue::IntegerListValue(list)) => {
                FieldValue::Text(format!("{:?}", list))
            }
            Some(gdal::vector::FieldValue::Integer64ListValue(list)) => {
                FieldValue::Text(format!("{:?}", list))
            }
            Some(gdal::vector::FieldValue::StringListValue(list)) => {
                FieldValue::Text(list.join(","))
            }
            Some(gdal::vector::FieldValue::RealListValue(list)) => {
                FieldValue::Text(format!("{:?}", list))
            }
            None => FieldValue::Null,
        };
        fields.insert(field_name, field_value);
    }

    Ok(Feature {
        geometry_wkb,
        srid,
        fields,
    })
}

// End
