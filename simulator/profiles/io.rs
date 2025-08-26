#[derive(Debug, Clone)]
pub struct IOProfile {
    enable: bool,
    latency: LatencyProfile,
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
    enable: bool,
    /// Added IO latency probability
    latency_probability: usize,
    /// Minimum tick time in microseconds for simulated time
    min_tick: u64,
    /// Maximum tick time in microseconds for simulated time
    max_tick: u64,
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
