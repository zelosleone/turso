#[derive(Debug, Clone)]
pub struct IOProfile {
    pub enable: bool,
    pub latency: LatencyProfile,
}

impl Default for IOProfile {
    fn default() -> Self {
        Self {
            enable: true,
            latency: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencyProfile {
    pub enable: bool,
    /// Added IO latency probability
    pub latency_probability: usize,
    /// Minimum tick time in microseconds for simulated time
    pub min_tick: u64,
    /// Maximum tick time in microseconds for simulated time
    pub max_tick: u64,
}

impl Default for LatencyProfile {
    fn default() -> Self {
        Self {
            enable: true,
            latency_probability: 1,
            min_tick: 1,
            max_tick: 30,
        }
    }
}
