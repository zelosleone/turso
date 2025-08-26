#[derive(Debug, Clone)]
pub struct CreateTableProfile {
    enable: bool,
}

impl Default for CreateTableProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct CreateIndexProfile {
    enable: bool,
}

impl Default for CreateIndexProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct InsertProfile {
    enable: bool,
}

impl Default for InsertProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct UpdateProfile {
    enable: bool,
}

impl Default for UpdateProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct DeleteProfile {
    enable: bool,
}

impl Default for DeleteProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct DropTableProfile {
    enable: bool,
}

impl Default for DropTableProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}
