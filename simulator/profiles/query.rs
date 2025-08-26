#[derive(Debug, Default, Clone)]
pub struct QueryProfile {
    pub create_table: CreateTableProfile,
    pub create_index: CreateIndexProfile,
    pub insert: InsertProfile,
    pub update: UpdateProfile,
    pub delete: DeleteProfile,
    pub drop_table: DropTableProfile,
}

#[derive(Debug, Clone)]
pub struct CreateTableProfile {
    pub enable: bool,
}

impl Default for CreateTableProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct CreateIndexProfile {
    pub enable: bool,
}

impl Default for CreateIndexProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct InsertProfile {
    pub enable: bool,
}

impl Default for InsertProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct UpdateProfile {
    pub enable: bool,
}

impl Default for UpdateProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct DeleteProfile {
    pub enable: bool,
}

impl Default for DeleteProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[derive(Debug, Clone)]
pub struct DropTableProfile {
    pub enable: bool,
}

impl Default for DropTableProfile {
    fn default() -> Self {
        Self { enable: true }
    }
}
