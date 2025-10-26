use crate::VectorConnector;
use gdal::Dataset;
use gdal::vector::LayerAccess;
use tokio::task;

/// Represents a field definition from a GDAL layer
/// Raw field definition before connector-specific type mapping
#[derive(Debug, Clone)]
struct RawFieldDefinition {
    pub name: String,
    pub gdal_field_type: String,
    pub width: Option<i32>,
    pub precision: Option<i32>,
    pub is_nullable: bool,
}

/// Raw schema extracted from GDAL before type mapping
#[derive(Debug, Clone)]
struct RawLayerSchema {
    pub layer_name: String,
    pub geometry_type: String,
    pub srid: Option<i32>,
    pub fields: Vec<RawFieldDefinition>,
    pub feature_count: i64,
}

/// Represents a field definition from a GDAL layer
#[derive(Debug, Clone)]
pub struct FieldDefinition {
    pub name: String,
    pub field_type: String, // PostgreSQL type string
    pub width: Option<i32>,
    pub precision: Option<i32>,
    pub is_nullable: bool,
}

/// Represents the complete schema of a GDAL layer
#[derive(Debug, Clone)]
pub struct LayerSchema {
    pub layer_name: String,
    pub geometry_type: String,
    pub srid: Option<i32>,
    pub fields: Vec<FieldDefinition>,
    pub feature_count: i64,
}

/// Extract schema information from a geospatial file
pub async fn extract_layer_schema(
    dataset: Dataset,
    connector: &dyn VectorConnector,
) -> Result<LayerSchema, Box<dyn std::error::Error + Send + Sync>> {
    // TODO: Run file processing in a queue
    // Run GDAL operations in a blocking task since GDAL is not async
    let raw_schema = task::spawn_blocking(move || {
        // Get the first layer (GeoJSON typically has one layer)
        let layer = dataset.layer(0)?;

        // Extract basic layer information
        let layer_name = layer.name();
        let feature_count = layer.feature_count();

        // Extract spatial reference system and SRID
        let srid = if let Some(srs) = layer.spatial_ref() {
            srs.auth_code().ok()
        } else {
            None
        };

        // Get layer definition to extract field information
        let layer_defn = layer.defn();

        // Extract field definitions
        let mut raw_fields = Vec::new();
        for field_defn in layer_defn.fields() {
            let raw_field = RawFieldDefinition {
                name: field_defn.name(),
                gdal_field_type: format!("{:?}", field_defn.field_type()),
                width: if field_defn.width() > 0 {
                    Some(field_defn.width())
                } else {
                    None
                },
                precision: if field_defn.precision() > 0 {
                    Some(field_defn.precision())
                } else {
                    None
                },
                is_nullable: field_defn.is_nullable(),
            };

            raw_fields.push(raw_field);
        }

        let geometry_type_name = gdal::vector::geometry_type_to_name(layer_defn.geometry_type());

        Ok::<RawLayerSchema, Box<dyn std::error::Error + Send + Sync>>(RawLayerSchema {
            layer_name,
            geometry_type: geometry_type_name,
            srid,
            fields: raw_fields,
            feature_count: feature_count.try_into().unwrap(),
        })
    })
    .await??;

    // Map field types using connector
    let mut mapped_fields = Vec::new();
    for raw_field in raw_schema.fields {
        let field_def = FieldDefinition {
            name: raw_field.name,
            field_type: connector.map_gdal_field_type(&raw_field.gdal_field_type),
            width: raw_field.width,
            precision: raw_field.precision,
            is_nullable: raw_field.is_nullable,
        };
        mapped_fields.push(field_def);
    }

    Ok(LayerSchema {
        layer_name: raw_schema.layer_name,
        geometry_type: raw_schema.geometry_type,
        srid: raw_schema.srid,
        fields: mapped_fields,
        feature_count: raw_schema.feature_count,
    })
}
