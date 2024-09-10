use std::env;

use anyhow::Result;
use clap::Parser;
use log::{debug, error, info};
use thiserror;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

mod cli;

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

async fn process_job(job: JobRequest) -> Result<JobResponse> {
    debug!("Worker received job: {:?}", job);
    sleep(Duration::from_secs(1)).await; // Simulate some work
    let result = job.request * 2;

    // If the job is odd, fail and retry
    if job.request % 2 != 0 {
        let error = JobError::Odd(job.job_id).into();
        debug!("Job failed: {}", error);
        return Err(error);
    }

    debug!("Worker processed job {}: result {}", job.job_id, result);
    Ok(JobResponse {
        job_id: job.job_id,
        result,
    })
}

async fn spawn_jobs(job_requests: Vec<JobRequest>) -> JoinSet<Result<JobResponse>> {
    let mut set: JoinSet<Result<JobResponse>> = JoinSet::new();
    for job in job_requests {
        set.spawn(process_job(job));
    }
    set
}

async fn join_all_with_retry(set: &mut JoinSet<Result<JobResponse>>) -> Result<i32> {
    let mut total = 0;
    while let Some(res) = set.join_next().await {
        let out = res?;
        match out {
            Ok(job_response) => {
                debug!(
                    "Job {} completed successfully with result {}",
                    job_response.job_id, job_response.result
                );
                total += job_response.result;
            }
            Err(e) => match e.downcast::<JobError>() {
                Ok(JobError::Odd(job_id)) => {
                    let retry_job = JobRequest::new(job_id, job_id as i32 + 1);
                    debug!("Retrying job {}", retry_job.job_id);
                    set.spawn(process_job(retry_job));
                }
                Err(e) => panic!("Unexpected error: {}", e),
            },
        }
    }
    Ok(total)
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

    let result = match args.command {
        cli::Commands::Parallel { jobs } => {
            let jobs: Vec<usize> = (0..jobs).collect();
            let job_requests = jobs
                .into_iter()
                .map(|job_id| JobRequest::new(job_id, job_id as i32))
                .collect::<Vec<JobRequest>>();

            let mut set = spawn_jobs(job_requests).await;
            let total = join_all_with_retry(&mut set).await?;
            Ok(total)
        }
        cli::Commands::Sequential => Err(anyhow::anyhow!("Not implemented")),
    }?;
    info!("Total: {}", result);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_jobs(num_jobs: usize) -> Vec<JobRequest> {
        let jobs: Vec<usize> = (0..num_jobs).collect();
        jobs.into_iter()
            .map(|job_id| JobRequest::new(job_id, job_id as i32))
            .collect::<Vec<JobRequest>>()
    }

    #[tokio::test]
    async fn test_process_job() {
        let job = JobRequest::new(0, 2);
        let result = process_job(job).await.unwrap();
        assert_eq!(result.job_id, 0);
        assert_eq!(result.result, 4);
    }

    #[tokio::test]
    async fn test_process_job_odd() {
        let job = JobRequest::new(1, 3);
        let result = process_job(job).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Job ID 1 is odd");
    }

    #[tokio::test]
    async fn test_spawn_jobs() {
        let jobs = dummy_jobs(10);
        let mut set = spawn_jobs(jobs).await;
        let total = join_all_with_retry(&mut set).await.unwrap();
        assert_eq!(set.len(), 0);
        assert_eq!(total, 100);
    }
}
