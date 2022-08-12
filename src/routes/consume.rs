use std::sync::Arc;

use axum::{extract, http::StatusCode, Extension, Json};
use serde::Deserialize;

use crate::record::{Log, Record};

#[derive(Deserialize)]
pub struct Offset {
    offset: usize,
}

pub async fn consume(
    extract::Json(payload): extract::Json<Offset>,
    Extension(log): Extension<Arc<Log>>,
) -> Result<Json<Record>, (StatusCode, String)> {
    let record = log
        .read(payload.offset)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("{}", e)))?;

    Ok(Json(record))
}
