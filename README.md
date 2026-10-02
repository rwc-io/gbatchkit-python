# gbatchkit-python
Google Cloud Platform Batch Kit (GBK)

## Why GBatchKit?

GCP Batch is a powerful serverless computing platform. `gbatchkit` provides lightweight tools to use Batch more effectively.

Key features:

* Create a straightforward job easily. This will run `my-script.py` in the given container on Batch.

  ```python
  container_uri = "container_uri"
  runnable = ContainerRunnable(
    image_uri=container_uri,
    entrypoint="python",
    commands=["my-script.py"]
  )
  task_count = 1
  job = create_standard_job(region, compute_config, task_count, [runnable])
  submit_job(job, "my_unique_job_id", region)
  ```

  * Also supports:
    * Customize the `service_account` and `network_interface` used to run the job.
    * Allocate a persistent disk for temp files.
    * Add dependency to other jobs (GCP Batch public preview feature).

* Create multi-task jobs (for one or multiple runnables).

  * Uses Google Cloud Storage to store task arguments.

  ```python
  task_arguments = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
  job = create_standard_job(region, compute_config, len(task_arguments), runnable)
  working_directory = "gs://a-bucket/a-directory/jobs"
  prepare_multitask_job(job, working_directory, tasks=task_arguments)
  submit_job(job, "my_unique_job_id", region)
  ```

  * Supports different arguments per runnable. In this example, the 1st runnable python script takes arguments `a` and `b` and the second runnable python script takes arguments `c` and `d`. Note that Batch requires all runnables to have the same number of tasks.

    ```python
    container_uri = "container_uri"
    runnable1 = ContainerRunnable(
      image_uri=container_uri,
      entrypoint="python",
      commands=["script1.py"]
    )
    runnable2 = ContainerRunnable(
      image_uri=container_uri,
      entrypoint="python",
      commands=["script2.py"]
    )
    task_arguments = [
      [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
      [{"c": 1, "d": 2}, {"c": 3, "d": 4}],
    ]
    
    job = create_standard_job(region, compute_config, task_count=2, runnables=[runnable1, runnable2])
    working_directory = "gs://a-bucket/a-directory/jobs"
    prepare_multitask_job(job, working_directory, tasks=task_arguments)
    
    submit_job(job, "my_unique_job_id", region)
    ```

## Authentication & In-Memory Credentials

By default, functions in `gbatchkit` fall back to Application Default Credentials (ADC).

For environments where credentials are held in memory (such as web servers handling per-session credentials), you can pass an explicit `credentials` argument to `submit_job`, `prepare_multitask_job`, `write_tasks`, or `get_task_arguments`.

The `credentials` parameter accepts either a `google.auth.credentials.Credentials` object or a raw parsed JSON dictionary (e.g. from a service account key file).

```python
# In-memory service account credentials JSON dict
credentials_json = {
    "type": "service_account",
    "project_id": "my-project",
    "private_key_id": "...",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
    "client_email": "service-account@my-project.iam.gserviceaccount.com",
    "client_id": "...",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}

# The credential drives both GCP Batch job submission and GCS task-file I/O
prepare_multitask_job(
    job,
    working_directory="gs://a-bucket/jobs",
    tasks=task_arguments,
    credentials=credentials_json,
)

submit_job(
    job,
    job_id="my_unique_job_id",
    region=region,
    credentials=credentials_json,
)
```
