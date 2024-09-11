use std::{collections::HashSet, env};

use anyhow::Result;
use clap::Parser;
use log::{debug, error, info};
use thiserror;
use tokio::{select, sync::mpsc};

mod cli;

async fn process(
    worker_id: usize,
    input_rx: async_channel::Receiver<JobRequest>,
    output_tx: async_channel::Sender<Result<JobResponse>>,
) -> Result<()> {
    while let Ok(job) = input_rx.recv().await {
        info!(
            "Worker {} processing job {} with request {}",
            worker_id, job.job_id, job.request
        );

        // Simulate some work and logs
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let response = if job.request % 2 != 0 {
            info!("Job {} is odd", job.job_id);
            Err(JobError::Odd(job.job_id).into())
        } else {
            Ok(JobResponse {
                job_id: job.job_id,
                result: job.request * 2,
            })
        };

        output_tx.send(response).await?;
    }
    Ok(())
}

#[derive(thiserror::Error, Debug)]
enum JobError {
    #[error("Job ID {0} is odd")]
    Odd(usize),
}

#[derive(Debug, Clone)]
struct JobRequest {
    job_id: usize,
    request: i32,
}

impl JobRequest {
    fn new(job_id: usize, request: i32) -> Self {
        Self { job_id, request }
    }
}

#[derive(Debug, Clone)]
struct JobResponse {
    job_id: usize,
    result: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::Cli::parse();
    if args.verbose {
        env::set_var("RUST_LOG", "debug")
    } else {
        env::set_var("RUST_LOG", "info")
    }
    env_logger::init();

    let result: i32 = match args.command {
        cli::Commands::Parallel { jobs, workers } => {
            let job_requests = (0..jobs)
                .map(|job_id| JobRequest::new(job_id, job_id as i32))
                .collect::<Vec<JobRequest>>();

            let (input_tx, input_rx) = async_channel::unbounded();
            let (output_tx, output_rx) = async_channel::unbounded();

            for id in 0..workers {
                let input_rx = input_rx.clone();
                let output_tx = output_tx.clone();
                tokio::spawn(async move { process(id, input_rx, output_tx).await });
            }

            for job in job_requests {
                let input_tx = input_tx.clone();
                tokio::spawn(async move {
                    input_tx.send(job).await.unwrap();
                });
            }

            let mut total: i32 = 0;
            let mut jobs_completed = 0;
            while let Ok(job) = output_rx.recv().await {
                match job {
                    Ok(job_response) => {
                        info!(
                            "Received result from worker {}: {}",
                            job_response.job_id, job_response.result
                        );
                        total += job_response.result;
                        jobs_completed += 1;
                        if jobs_completed == jobs {
                            break;
                        }
                    }
                    Err(e) => match e.downcast::<JobError>() {
                        Ok(JobError::Odd(job_id)) => {
                            let retry_job = JobRequest::new(job_id, job_id as i32 + 1);
                            info!("Retrying job {}", retry_job.job_id);
                            let input_tx = input_tx.clone();
                            tokio::spawn(async move {
                                input_tx.send(retry_job).await.unwrap();
                            });
                        }
                        Err(e) => panic!("Unexpected error: {}", e),
                    },
                }
            }
            anyhow::Ok(total)
        }
        cli::Commands::Sequential { jobs } => Ok(10),
    }?;

    info!("Total: {}", result);
    Ok(())
}
