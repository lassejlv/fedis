use tracing_subscriber::EnvFilter;

pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    let filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(default_filter()))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

    Ok(())
}

fn default_filter() -> String {
    std::env::var("FEDIS_LOG").unwrap_or_else(|_| "info".to_string())
}
