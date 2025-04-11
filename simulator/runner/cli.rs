use clap::{command, Parser};
use serde::{Deserialize, Serialize};

#[derive(Parser, Clone, Serialize, Deserialize)]
#[command(name = "limbo-simulator")]
#[command(author, version, about, long_about = None)]
pub struct SimulatorCLI {
    #[clap(short, long, help = "set seed for reproducible runs", default_value = None)]
    pub seed: Option<u64>,
    #[clap(
        short,
        long,
        help = "enable doublechecking, run the simulator with the plan twice and check output equality"
    )]
    pub doublecheck: bool,
    #[clap(
        short = 'n',
        long,
        help = "change the maximum size of the randomly generated sequence of interactions",
        default_value_t = 5000
    )]
    pub maximum_size: usize,
    #[clap(
        short = 'k',
        long,
        help = "change the minimum size of the randomly generated sequence of interactions",
        default_value_t = 1000
    )]
    pub minimum_size: usize,
    #[clap(
        short = 't',
        long,
        help = "change the maximum time of the simulation(in seconds)",
        default_value_t = 60 * 60 // default to 1 hour
    )]
    pub maximum_time: usize,
    #[clap(short = 'l', long, help = "load plan from the bug base")]
    pub load: Option<String>,
    #[clap(
        short = 'w',
        long,
        help = "enable watch mode that reruns the simulation on file changes"
    )]
    pub watch: bool,
    #[clap(long, help = "run differential testing between sqlite and Limbo")]
    pub differential: bool,
}

impl SimulatorCLI {
    pub fn validate(&self) -> Result<(), String> {
        if self.minimum_size < 1 {
            return Err("minimum size must be at least 1".to_string());
        }
        if self.maximum_size < 1 {
            return Err("maximum size must be at least 1".to_string());
        }
        // todo: fix an issue here where if minimum size is not defined, it prevents setting low maximum sizes.
        if self.minimum_size > self.maximum_size {
            return Err("Minimum size cannot be greater than maximum size".to_string());
        }

        if self.seed.is_some() && self.load.is_some() {
            return Err("Cannot set seed and load plan at the same time".to_string());
        }

        Ok(())
    }
}
