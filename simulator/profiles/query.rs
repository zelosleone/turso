#[derive(Debug, Default, Clone)]
pub struct CreateTableProfile {
    enable: bool,
}

#[derive(Debug, Default, Clone)]
pub struct CreateIndexProfile {
    enable: bool,
}

#[derive(Debug, Default, Clone)]
pub struct InsertProfile {
    enable: bool,
}

#[derive(Debug, Default, Clone)]
pub struct UpdateProfile {
    enable: bool,
}

#[derive(Debug, Default, Clone)]
pub struct DeleteProfile {
    enable: bool,
}

#[derive(Debug, Default, Clone)]
pub struct DropTableProfile {
    enable: bool,
}
