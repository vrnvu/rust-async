use std::env;

use anyhow::Result;
use log::{debug, error, info};
use thiserror;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

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

#[tokio::main]
async fn main() -> Result<()> {
    let debug = env::var("DEBUG").is_ok();
    if debug {
        env::set_var("RUST_LOG", "debug")
    } else {
        env::set_var("RUST_LOG", "info")
    }
    env_logger::init();

    // TODO randomize job spawn for cpu-branching prediction
    // TODO benchmark

    const NUM_JOBS: usize = 10;
    let jobs: Vec<usize> = (0..NUM_JOBS).collect();
    let job_requests = jobs
        .into_iter()
        .map(|job_id| JobRequest::new(job_id, job_id as i32))
        .collect::<Vec<JobRequest>>();

    let mut set: JoinSet<Result<JobResponse>> = JoinSet::new();
    for job in job_requests {
        set.spawn(process_job(job));
    }

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

    info!("Total: {}", total);

    Ok(())
}
