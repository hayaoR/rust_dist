use std::sync::Arc;

use axum::{
    routing::get,
    Extension, Router,
};
use proglog::{record::Log, routes::{consume::consume, produce::produce}};

#[tokio::main]
async fn main() {
    let shared_log = Arc::new(Log::new());

    let app = Router::new()
        .route("/", get(consume).post(produce))
        .layer(Extension(shared_log));

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
