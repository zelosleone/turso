#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    TursoError(turso::Error),
    #[error("database tape error: {0}")]
    DatabaseTapeError(String),
}

impl From<turso::Error> for Error {
    fn from(value: turso::Error) -> Self {
        Self::TursoError(value)
    }
}

impl From<turso_core::LimboError> for Error {
    fn from(value: turso_core::LimboError) -> Self {
        Self::TursoError(value.into())
    }
}
