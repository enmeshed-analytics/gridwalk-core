use crate::file::LayerSchema;
use crate::{ConnectorBase, GeometryType, VectorConnector};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use gdal::vector::{Defn, Feature, FieldValue};
use sqlx::PgPool;
use std::any::Any;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

/// Validates that an identifier is safe to use in SQL (no injection risk)
fn validate_sql_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty() {
        return Err(anyhow!("Identifier cannot be empty"));
    }

    // Check for basic SQL injection patterns and dangerous characters
    if identifier.contains(&[';', '\'', '"', '\\', '\0', '\n', '\r'][..])
        || identifier.to_uppercase().contains("DROP")
        || identifier.to_uppercase().contains("DELETE")
        || identifier.to_uppercase().contains("INSERT")
        || identifier.to_uppercase().contains("UPDATE")
    {
        return Err(anyhow!("Invalid identifier: contains unsafe characters"));
    }

    Ok(())
}

/// Safely quotes a SQL identifier
fn quote_identifier(identifier: &str) -> Result<String> {
    validate_sql_identifier(identifier)?;
    Ok(format!("\"{}\"", identifier.replace("\"", "\"\"")))
}
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub database_name: String,
    pub schema: String,
    pub max_connections: u32,
    pub disable_ssl: bool,
}

#[derive(Clone, Debug)]
pub struct PostgisConnector {
    pub pool: Arc<PgPool>,
    pub schema: String,
}

impl PostgisConnector {
    pub async fn new(config: PostgresConfig) -> Result<Self> {
        let sslmode = if config.disable_ssl {
            "disable"
        } else {
            "require"
        };

        let connection_string = format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode={}",
            config.user, config.password, config.host, config.port, config.database_name, sslmode
        );

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&connection_string)
            .await
            .map_err(|e| anyhow!("Failed to create connection pool: {}", e))?;

        Ok(PostgisConnector {
            pool: Arc::new(pool),
            schema: config.schema,
        })
    }

    /// Generate a PostGIS CREATE TABLE statement from a LayerSchema
    pub fn generate_postgis_create_table_sql(&self, schema: &LayerSchema) -> String {
        let mut sql = format!(
            "CREATE TABLE \"{}\".\"{}\" (\n",
            self.schema, schema.layer_name
        );

        // Add primary key column
        sql.push_str("    id SERIAL PRIMARY KEY,\n");

        // Add attribute columns
        for field in &schema.fields {
            let nullable = if field.is_nullable { "" } else { " NOT NULL" };
            sql.push_str(&format!(
                "    \"{}\" {}{},\n",
                field.name, field.field_type, nullable
            ));
        }

        // Add geometry column
        let srid = schema.srid.unwrap_or(4326); // Default to WGS84 if no SRID

        sql.push_str(&format!(
            "    \"geometry\" geometry({}, {})\n",
            schema.geometry_type, srid
        ));

        sql.push_str(");");

        sql
    }

    pub fn feature_to_insert_statement(
        feature: &Feature,
        defn: &Defn,
        schema: &str,
        table_name: &str,
        geometry_column: Option<&str>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut column_names = Vec::new();
        let mut values = Vec::new();

        // Get field definitions from Defn and field values from Feature
        let field_defs: Vec<_> = defn.fields().collect();

        // Iterate through fields by index
        for (field_idx, field_defn) in field_defs.iter().enumerate() {
            let field_name = field_defn.name();

            // Get the field value from the feature
            if let Some(field_value) = feature.field(field_idx)? {
                column_names.push(format!("\"{}\"", field_name));
                values.push(Self::format_field_value(&field_value)?);
            }
            // Skip NULL fields or handle them explicitly if needed
        }

        // Handle geometry if present
        if let Some(geom) = feature.geometry() {
            let geom_column = geometry_column.unwrap_or("geometry");
            column_names.push(format!("\"{}\"", geom_column));

            // Convert geometry to WKT for PostGIS
            let wkt = geom.wkt()?;

            // You might need to get SRID from the layer's spatial reference
            let srid = 4326; // Or get from layer.spatial_ref()
            values.push(format!("ST_GeomFromText('{}', {})", wkt, srid));
        }

        // Build the INSERT statement
        let insert_sql = format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({});",
            schema,
            table_name,
            column_names.join(", "),
            values.join(", ")
        );

        Ok(insert_sql)
    }

    fn format_field_value(value: &FieldValue) -> Result<String, Box<dyn std::error::Error>> {
        match value {
            FieldValue::IntegerValue(i) => Ok(i.to_string()),
            FieldValue::Integer64Value(i) => Ok(i.to_string()),
            FieldValue::RealValue(f) => {
                // Handle special float values
                if f.is_nan() {
                    Ok("NULL".to_string())
                } else if f.is_infinite() {
                    Ok("NULL".to_string())
                } else {
                    Ok(f.to_string())
                }
            }
            FieldValue::StringValue(s) => {
                // Escape single quotes for SQL
                let escaped = s.replace("'", "''");
                Ok(format!("'{}'", escaped))
            }
            FieldValue::DateValue(date) => {
                // Format date for Postgres (YYYY-MM-DD)
                Ok(format!("'{}'", date.format("%Y-%m-%d")))
            }
            FieldValue::DateTimeValue(dt) => {
                // Format datetime for Postgres
                Ok(format!("'{}'", dt.to_rfc3339()))
            }
            FieldValue::IntegerListValue(list) => {
                // For array types in Postgres
                let items: Vec<String> = list.iter().map(|i| i.to_string()).collect();
                Ok(format!("ARRAY[{}]::integer[]", items.join(", ")))
            }
            FieldValue::Integer64ListValue(list) => {
                let items: Vec<String> = list.iter().map(|i| i.to_string()).collect();
                Ok(format!("ARRAY[{}]::bigint[]", items.join(", ")))
            }
            FieldValue::RealListValue(list) => {
                let items: Vec<String> = list.iter().map(|f| f.to_string()).collect();
                Ok(format!("ARRAY[{}]::double precision[]", items.join(", ")))
            }
            FieldValue::StringListValue(list) => {
                let items: Vec<String> = list
                    .iter()
                    .map(|s| format!("'{}'", s.replace("'", "''")))
                    .collect();
                Ok(format!("ARRAY[{}]::text[]", items.join(", ")))
            }
        }
    }
}

#[async_trait]
impl ConnectorBase for PostgisConnector {
    async fn connect(&mut self) -> Result<()> {
        debug!("Testing connection to PostGIS database");
        sqlx::query("SELECT 1")
            .execute(&*self.pool)
            .await
            .map_err(|e| anyhow!("Failed to execute test query: {}", e))?;
        debug!("Connection test successful");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        debug!("Disconnect called, but pool remains active for potential future use");
        Ok(())
    }

    async fn create_layer(&self, layer: &LayerSchema) -> Result<()> {
        debug!("Creating layer '{}' in PostGIS database", layer.layer_name);

        let sql = self.generate_postgis_create_table_sql(layer);
        debug!("Executing SQL: {}", sql);

        sqlx::query(&sql)
            .execute(&*self.pool)
            .await
            .map_err(|e| anyhow!("Failed to create layer '{}': {}", layer.layer_name, e))?;

        debug!("Successfully created layer '{}'", layer.layer_name);
        Ok(())
    }

    async fn list_sources(&self) -> Result<Vec<String>> {
        let query = "SELECT table_name 
                     FROM information_schema.tables 
                     WHERE table_schema = $1";

        let rows = sqlx::query_as::<_, (String,)>(query)
            .bind(&self.schema)
            .fetch_all(&*self.pool)
            .await
            .map_err(|e| anyhow!("Failed to execute query to list sources: {}", e))?;

        let sources: Vec<String> = rows.into_iter().map(|(table_name,)| table_name).collect();
        Ok(sources)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl VectorConnector for PostgisConnector {
    // Vector-specific methods
    async fn create_namespace(&self, name: &str) -> Result<()> {
        let quoted_name = quote_identifier(name)?;
        let query = format!("CREATE SCHEMA IF NOT EXISTS {}", quoted_name);
        sqlx::query(&query)
            .execute(&*self.pool)
            .await
            .map_err(|e| anyhow!("Failed to execute query to create namespace: {}", e))?;
        Ok(())
    }

    async fn get_tile(
        &self,
        source: &crate::connector::LayerSource,
        layer_name: &str,
        z: u32,
        x: u32,
        y: u32,
    ) -> Result<Vec<u8>> {
        // Extract namespace and name from LayerSource
        let (namespace, table_name, geometry_field, srid) = match source {
            crate::connector::LayerSource::Database {
                namespace,
                name,
                geometry_field,
                srid,
            } => (namespace, name, geometry_field, srid),
        };

        // Validate and quote identifiers to prevent SQL injection
        let quoted_schema = quote_identifier(namespace)?;
        let quoted_table = quote_identifier(table_name)?;
        let geom_column = quote_identifier(geometry_field)?;

        let query = format!(
            "
                WITH bounds AS (
                    SELECT ST_Transform(ST_TileEnvelope($1, $2, $3), {}) AS geom
                ),
                mvt_data AS (
                    SELECT ST_AsMVTGeom(
                        t.{geom_col},
                        bounds.geom,
                        4096,
                        256,
                        true
                    ) AS geom
                    FROM {schema}.{table} t,
                    bounds
                    WHERE ST_Intersects(t.{geom_col}, bounds.geom)
                )
                SELECT ST_AsMVT(mvt_data.*, $4) AS mvt
                FROM mvt_data;
                ",
            srid,
            schema = quoted_schema,
            table = quoted_table,
            geom_col = geom_column
        );

        let mvt_data: Vec<u8> = sqlx::query_as::<_, (Vec<u8>,)>(&query)
            .bind(z as i32)
            .bind(x as i32)
            .bind(y as i32)
            .bind(layer_name)
            .fetch_one(&*self.pool)
            .await?
            .0;
        debug!("MVT data size: {}", mvt_data.len());
        Ok(mvt_data)
    }

    async fn get_geometry_type(&self, source_id: &Uuid) -> Result<GeometryType> {
        // First check which geometry column exists
        let check_column_query = "SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = $1 AND table_schema = $2
            AND column_name IN ('geom', 'geometry', 'geoms', 'wkb_geometry')";

        // Get the geometry column name
        let geom_column: String = sqlx::query_as::<_, (String,)>(check_column_query)
            .bind(source_id.to_string())
            .bind(&self.schema)
            .fetch_one(&*self.pool)
            .await?
            .0;

        // Validate and quote identifiers to prevent SQL injection
        let quoted_schema = quote_identifier(&self.schema)?;
        let quoted_table = quote_identifier(&source_id.to_string())?;
        let quoted_geom_column = quote_identifier(&geom_column)?;

        // Query to get the geometry type using properly quoted identifiers
        let query = format!(
            "SELECT DISTINCT ST_GeometryType({}) 
            FROM {}.{} 
            LIMIT 1",
            quoted_geom_column, quoted_schema, quoted_table
        );

        let geom_type: String = sqlx::query_as::<_, (String,)>(&query)
            .fetch_one(&*self.pool)
            .await?
            .0;

        // Map PostGIS geometry type to our GeometryType enum and return the result
        match geom_type.to_uppercase().as_str() {
            "ST_POINT" => Ok(GeometryType::Point),
            "ST_LINESTRING" => Ok(GeometryType::LineString),
            "ST_POLYGON" => Ok(GeometryType::Polygon),
            "ST_MULTIPOINT" => Ok(GeometryType::MultiPoint),
            "ST_MULTILINESTRING" => Ok(GeometryType::MultiLineString),
            "ST_MULTIPOLYGON" => Ok(GeometryType::MultiPolygon),
            "ST_GEOMETRYCOLLECTION" => Ok(GeometryType::GeometryCollection),
            _ => Err(anyhow!("Unsupported geometry type: {}", geom_type)),
        }
    }

    fn map_gdal_field_type(&self, field_type_str: &str) -> String {
        match field_type_str {
            "String" => "TEXT".to_string(),
            "Integer" => "INTEGER".to_string(),
            "Integer64" => "BIGINT".to_string(),
            "Real" => "DOUBLE PRECISION".to_string(),
            "Date" => "DATE".to_string(),
            "Time" => "TIME".to_string(),
            "DateTime" => "TIMESTAMP".to_string(),
            "Binary" => "BYTEA".to_string(),
            "StringList" => "TEXT[]".to_string(),
            "IntegerList" => "INTEGER[]".to_string(),
            "Integer64List" => "BIGINT[]".to_string(),
            "RealList" => "DOUBLE PRECISION[]".to_string(),
            _ => "TEXT".to_string(), // Default fallback
        }
    }
}
