use std::sync::Arc;

use axum::{extract, http::StatusCode, Extension, Json};

use crate::record::{Log, Record};

pub async fn produce(
    extract::Json(payload): extract::Json<Record>,
    Extension(log): Extension<Arc<Log>>,
) -> Result<Json<u64>, (StatusCode, String)> {
    let offset = log
        .append(payload)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("{}", e)))?;

    Ok(Json(offset))
}
