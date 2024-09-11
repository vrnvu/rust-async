use std::{collections::HashSet, env};

use anyhow::Result;
use clap::Parser;
use log::{debug, error, info};
use thiserror;
use tokio::{select, sync::mpsc};

mod cli;

async fn process(
    worker_id: usize,
    mut input_rx: mpsc::Receiver<JobRequest>,
    output_tx: mpsc::Sender<Result<JobResponse>>,
) -> Result<()> {
    while let Some(job) = input_rx.recv().await {
        info!(
            "Worker {} processing job {} with request {}",
            worker_id, job.job_id, job.request
        );

        // Simulate some work and logs
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if job.request % 2 != 0 {
            info!("Job {} is odd", job.job_id);
        }

        let response = if job.request % 2 != 0 {
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
        cli::Commands::Parallel { jobs } => {
            let job_requests = (0..jobs)
                .map(|job_id| JobRequest::new(job_id, job_id as i32))
                .collect::<Vec<JobRequest>>();

            let mut worker_input_tx: Vec<mpsc::Sender<JobRequest>> = vec![];
            let mut worker_input_rx: Vec<mpsc::Receiver<JobRequest>> = vec![];
            let mut worker_output_tx: Vec<mpsc::Sender<Result<JobResponse>>> = vec![];
            let mut worker_output_rx: Vec<mpsc::Receiver<Result<JobResponse>>> = vec![];

            for _ in 0..2 {
                let (tx_input, rx_input) = mpsc::channel(1);
                let (tx_output, rx_output) = mpsc::channel(1);

                worker_input_tx.push(tx_input);
                worker_input_rx.push(rx_input);
                worker_output_tx.push(tx_output);
                worker_output_rx.push(rx_output);
            }

            for id in 0..2 {
                // We can safely take ownership of the channels here because we are not going to use them anymore
                let worker_output_tx = worker_output_tx.remove(0);
                let worker_input_rx = worker_input_rx.remove(0);
                tokio::spawn(async move { process(id, worker_input_rx, worker_output_tx).await });
            }

            for job in job_requests {
                let idx = job.job_id % 2;
                let tx = worker_input_tx[idx].clone();
                // We need to send them async or the main will block with a buffered channel of size 1
                // This is because we are reading the results later in the main thread with a select!
                tokio::spawn(async move {
                    tx.send(job).await.unwrap();
                });
            }

            let mut worker_output_rx_0 = worker_output_rx.remove(0);
            let mut worker_output_rx_1 = worker_output_rx.remove(0);
            let mut total: i32 = 0;
            let mut jobs_completed = HashSet::new();
            loop {
                select! {
                    res = worker_output_rx_0.recv() => {
                        if let Some(res) = res {
                            match res {
                                Ok(job_response) => {
                                    info!("Received result from worker 0: {}", job_response.result);
                                    total += job_response.result;
                                    jobs_completed.insert(job_response.job_id);
                                    if jobs_completed.len() == jobs {
                                        break;
                                    }
                                },
                                Err(e) => match e.downcast::<JobError>() {
                                    Ok(JobError::Odd(job_id)) => {
                                        let retry_job = JobRequest::new(job_id, job_id as i32 + 1);
                                        let tx = worker_input_tx[job_id % 2].clone();
                                        info!("Retrying job {}", retry_job.job_id);
                                        tokio::spawn(async move {
                                            tx.send(retry_job).await.unwrap();
                                        });
                                    }
                                    Err(e) => panic!("Unexpected error: {}", e),
                                },
                            }
                        }
                    },
                    res = worker_output_rx_1.recv() => {
                        if let Some(res) = res {
                            match res {
                                Ok(job_response) => {
                                    info!("Received result from worker 0: {}", job_response.result);
                                    total += job_response.result;
                                    jobs_completed.insert(job_response.job_id);
                                    if jobs_completed.len() == jobs {
                                        break;
                                    }
                                },
                                Err(e) => match e.downcast::<JobError>() {
                                    Ok(JobError::Odd(job_id)) => {
                                        let retry_job = JobRequest::new(job_id, job_id as i32 + 1);
                                        let tx = worker_input_tx[job_id % 2].clone();
                                        info!("Retrying job {}", retry_job.job_id);
                                        tokio::spawn(async move {
                                            tx.send(retry_job).await.unwrap();
                                        });
                                    }
                                    Err(e) => panic!("Unexpected error: {}", e),
                                },
                            }
                        }
                    }
                }
            }

            anyhow::Ok(total)
        }
        cli::Commands::Sequential { jobs } => Ok(10),
    }?;

    info!("Total: {}", result);
    Ok(())
}
