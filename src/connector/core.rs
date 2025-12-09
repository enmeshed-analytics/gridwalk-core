use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::any::Any;
use uuid::Uuid;

/// Base trait with common functionality for all connectors
#[async_trait]
pub trait ConnectorBase: Send + Sync {
    /// Establish a connection to the data source.
    async fn connect(&mut self) -> Result<()>;

    /// Disconnect from the data source.
    async fn disconnect(&mut self) -> Result<()>;

    // Create Layer in the data source
    async fn create_layer(&self, layer: &crate::file::LayerSchema) -> Result<()>;

    /// List data sources, optionally filtered.
    async fn list_sources(&self) -> Result<Vec<String>>;

    /// Returns a reference to self as a `dyn Any` to support downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Test the connection to the data source
    async fn test_connection(&mut self) -> Result<()> {
        self.connect().await?;
        self.disconnect().await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum LayerLocation {
    Database { namespace: String, name: String },
    // CloudObject { bucket: String, key: String },
    // Uuid(Uuid),
}

/// Trait for all vector-based geospatial data sources
#[async_trait]
pub trait VectorConnector: ConnectorBase {
    async fn get_geometry_type(&self, source_id: &Uuid) -> Result<GeometryType>;
    async fn create_namespace(&self, name: &str) -> Result<()>;
    async fn get_tile(&self, source: &LayerLocation, z: u32, x: u32, y: u32) -> Result<Vec<u8>>;
    fn map_gdal_field_type(&self, field_type_str: &str) -> String;
}

/// Trait for all raster-based geospatial data sources
#[async_trait]
pub trait RasterConnector: ConnectorBase {
    async fn get_raster_info(&self, source_id: &Uuid) -> Result<RasterInfo>;
    async fn get_raster_tile(&self, source_id: &Uuid, z: u32, x: u32, y: u32) -> Result<Vec<u8>>;
}

/// Trait for connectors that support both vector and raster data
#[async_trait]
pub trait HybridConnector: VectorConnector + RasterConnector {}

/// Main connector enum that wraps either vector or raster connectors
pub enum Connector {
    Vector(Box<dyn VectorConnector>),
    Raster(Box<dyn RasterConnector>),
    Hybrid(Box<dyn HybridConnector>),
}

impl Connector {
    /// Create a new vector connector
    pub fn new_vector(connector: Box<dyn VectorConnector>) -> Self {
        Connector::Vector(connector)
    }

    /// Create a new raster connector
    pub fn new_raster(connector: Box<dyn RasterConnector>) -> Self {
        Connector::Raster(connector)
    }

    /// Create a new hybrid connector
    pub fn new_hybrid(connector: Box<dyn HybridConnector>) -> Self {
        Connector::Hybrid(connector)
    }

    /// Get reference to vector connector if this is a vector connector
    pub fn as_vector(&self) -> Option<&dyn VectorConnector> {
        match self {
            Connector::Vector(v) => Some(v.as_ref()),
            Connector::Hybrid(h) => Some(h.as_ref()),
            _ => None,
        }
    }

    /// Get mutable reference to vector connector if this is a vector connector
    pub fn as_vector_mut(&mut self) -> Option<&mut dyn VectorConnector> {
        match self {
            Connector::Vector(v) => Some(v.as_mut()),
            Connector::Hybrid(h) => Some(h.as_mut()),
            _ => None,
        }
    }

    /// Get reference to raster connector if this is a raster connector
    pub fn as_raster(&self) -> Option<&dyn RasterConnector> {
        match self {
            Connector::Raster(r) => Some(r.as_ref()),
            Connector::Hybrid(h) => Some(h.as_ref()),
            _ => None,
        }
    }

    /// Get mutable reference to raster connector if this is a raster connector
    pub fn as_raster_mut(&mut self) -> Option<&mut dyn RasterConnector> {
        match self {
            Connector::Raster(r) => Some(r.as_mut()),
            Connector::Hybrid(h) => Some(h.as_mut()),
            _ => None,
        }
    }

    /// Common connect method
    pub async fn connect(&mut self) -> Result<()> {
        match self {
            Connector::Vector(v) => v.connect().await,
            Connector::Raster(r) => r.connect().await,
            Connector::Hybrid(h) => h.connect().await,
        }
    }

    /// Common disconnect method
    pub async fn disconnect(&mut self) -> Result<()> {
        match self {
            Connector::Vector(v) => v.disconnect().await,
            Connector::Raster(r) => r.disconnect().await,
            Connector::Hybrid(h) => h.disconnect().await,
        }
    }

    /// Common list_sources method
    pub async fn list_sources(&self) -> Result<Vec<String>> {
        match self {
            Connector::Vector(v) => v.list_sources().await,
            Connector::Raster(r) => r.list_sources().await,
            Connector::Hybrid(h) => h.list_sources().await,
        }
    }

    /// Common test_connection method
    pub async fn test_connection(&mut self) -> Result<()> {
        match self {
            Connector::Vector(v) => v.test_connection().await,
            Connector::Raster(r) => r.test_connection().await,
            Connector::Hybrid(h) => h.test_connection().await,
        }
    }

    /// Check if this is a vector connector
    pub fn is_vector(&self) -> bool {
        matches!(self, Connector::Vector(_) | Connector::Hybrid(_))
    }

    /// Check if this is a raster connector
    pub fn is_raster(&self) -> bool {
        matches!(self, Connector::Raster(_) | Connector::Hybrid(_))
    }

    /// Check if this is a hybrid connector
    pub fn is_hybrid(&self) -> bool {
        matches!(self, Connector::Hybrid(_))
    }

    /// Get reference to hybrid connector if this is a hybrid connector
    pub fn as_hybrid(&self) -> Option<&dyn HybridConnector> {
        match self {
            Connector::Hybrid(h) => Some(h.as_ref()),
            _ => None,
        }
    }

    /// Get mutable reference to hybrid connector if this is a hybrid connector
    pub fn as_hybrid_mut(&mut self) -> Option<&mut dyn HybridConnector> {
        match self {
            Connector::Hybrid(h) => Some(h.as_mut()),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RasterInfo {
    pub width: u32,
    pub height: u32,
    pub bands: u32,
    pub data_type: String,
    pub no_data_value: Option<f64>,
}
