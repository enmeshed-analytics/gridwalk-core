use anyhow::Result;
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use uuid::Uuid;

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
}
