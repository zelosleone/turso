use clap::{command, Parser};

#[derive(Parser)]
#[command(name = "limbo_stress")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    /// Number of threads to run
    #[clap(short = 't', long, help = "the number of threads", default_value_t = 8)]
    pub nr_threads: usize,

    /// Number of iterations per thread
    #[clap(
        short = 'i',
        long,
        help = "the number of iterations",
        default_value_t = 100000
    )]
    pub nr_iterations: usize,

    /// Log file for SQL statements
    #[clap(
        short = 'l',
        long,
        help = "log file for SQL statements",
        default_value = "limbostress.log"
    )]
    pub log_file: String,
}
