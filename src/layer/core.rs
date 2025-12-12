use anyhow::Result;
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use uuid::Uuid;

#[derive(Clone, Debug, Display, Serialize, Deserialize, EnumString, PartialEq)]
pub enum Srid {
    #[serde(rename = "3857")]
    #[strum(serialize = "3857")]
    EPSG3857,

    #[serde(rename = "4326")]
    #[strum(serialize = "4326")]
    EPSG4326,
}

#[derive(Clone, Debug, Display, Serialize, Deserialize, EnumString)]
#[serde(rename_all = "lowercase")]
pub enum LayerStatus {
    Uploading,
    Processing,
    Ready,
    Error,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct LayerSummary {
    pub id: Uuid,
    pub name: String,
    pub status: LayerStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Core trait that all layer types must implement
pub trait LayerCore: Sized {
    fn save<'e, E>(&self, executor: E) -> impl std::future::Future<Output = Result<()>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>;

    fn list<'e, E>(
        limit: u64,
        offset: u64,
        executor: E,
    ) -> impl std::future::Future<Output = Result<Vec<Self>>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>;

    fn get<'e, E>(id: Uuid, executor: E) -> impl std::future::Future<Output = Result<Self>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>;

    fn exists<'e, E>(
        id: Uuid,
        executor: E,
    ) -> impl std::future::Future<Output = Result<bool>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>;
}
