use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[clap(
    version = "0.1.0",
    author = "Arnau Diaz <arnaudiaz@duck.com>",
    about = "Rust async patterns"
)]
pub struct Cli {
    /// Sets logging to "debug" level, defaults to "info"
    #[clap(short, long, global = true)]
    pub verbose: bool,

    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Parallel mode
    Parallel {
        /// sets the number of jobs
        #[clap(short, long)]
        jobs: usize,
        /// sets the number of workers
        #[clap(short, long)]
        workers: usize,
    },
}
