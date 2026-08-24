import json
import subprocess
import tempfile
from typing import List, Optional, TypeVar, Union

import math
import smart_open

from gbatchkit.types import (
    ComputeConfig,
    NetworkInterfaceConfig,
    ServiceAccountConfig,
    Runnable,
    ContainerRunnable,
)

TaskArgsType = TypeVar("TaskArgsType")


def submit_job(
    job: dict, job_id: str, region: str, project: Optional[str] = None
) -> None:
    """
    Submit a job to the Batch service.
    """
    if not job:
        raise ValueError("Job definition is empty")
    if not job_id or len(job_id) > 64:
        raise ValueError("Job ID must be 1-64 characters")
    if not region:
        raise ValueError("Region is required")

    with tempfile.NamedTemporaryFile() as job_json_file:
        with smart_open.open(job_json_file.name, "w") as f:
            # separated for ease of testing
            job_json_str = json.dumps(job)
            f.write(job_json_str)

        cmd = ["gcloud"]
        if job.get("dependencies"):
            cmd.append("alpha")
        cmd.extend(
            [
                "batch",
                "jobs",
                "submit",
                job_id,
                "--location",
                region,
                "--config",
                job_json_file.name,
            ]
        )
        if project:
            cmd.extend(["--project", project])

        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        if result.returncode != 0:
            raise RuntimeError(f"Failed to submit job: {result.stderr.decode('utf-8')}")


def prepare_multitask_job(
    job: dict,
    working_directory: str,
    tasks: List[Union[dict, TaskArgsType]] = None,
    runnable_tasks: List[List[Union[dict, TaskArgsType]]] = None,
):
    """
    Prepare a multitask job by assigning tasks, or per-runnable tasks to the job,
    configuring the necessary environment variables, and serializing the tasks
    to JSON files.

    :param dict job: The job being prepared.
    :param str working_directory: The directory where JSON tasks will be stored.
    :param tasks: Tasks, shared by all runnables. This is mutually exclusive
        with `runnable_tasks`.
    :type tasks: list[Union[dict, TaskArgsType]], optional
    :param runnable_tasks: A list of tasks per runnable. Must have a task
        list for each runnable. This is mutually exclusive with `tasks`.
    :type runnable_tasks: list[list[Union[dict, TaskArgsType]]], optional
    :return: None
    :raises ValueError: If both `tasks` and `runnable_tasks` are specified, if the number
        of `runnable_tasks` does not match the number of runnables in the job, if the
        number of tasks in any `runnable_tasks` does not match the required task count,
        or if no tasks or runnable tasks are provided.
    :raises ValueError: If there are no runnables provided in the job definition.
    """
    if tasks and runnable_tasks:
        raise ValueError("Specify tasks or runnable_tasks, not both")

    runnables = job["taskGroups"][0]["taskSpec"].setdefault("runnables", [])
    if not runnables:
        raise ValueError("No runnables found in the job definition")

    num_runnables = len(runnables)
    num_tasks = job["taskGroups"][0]["taskCount"]

    if runnable_tasks:
        if len(runnable_tasks) != num_runnables:
            raise ValueError(
                f"Need tasks for each of {num_runnables} runnables, got {len(runnable_tasks)}"
            )

        for i, (runnable, tasks) in enumerate(zip(runnables, runnable_tasks)):
            if len(tasks) != num_tasks:
                raise ValueError(
                    f"Need {num_tasks} tasks for runnable {i}, got {len(tasks)}"
                )
            runnable_tasks_path = f"{working_directory}/runnable_{i}_tasks.json"
            set_runnable_environment_variable(
                runnable, "GBATCHKIT_ARGS_PATH", runnable_tasks_path
            )
            write_tasks(tasks, runnable_tasks_path)
    elif tasks:
        if len(tasks) != num_tasks:
            raise ValueError(f"Need {num_tasks} tasks, got {len(tasks)}")

        runnable_tasks_path = f"{working_directory}/tasks.json"
        set_job_environment_variable(job, "GBATCHKIT_ARGS_PATH", runnable_tasks_path)
        write_tasks(tasks, runnable_tasks_path)
    else:
        raise ValueError("Need to specify either tasks or runnable_tasks")


def write_tasks(tasks: List[Union[dict, TaskArgsType]], tasks_path: str):
    """
    Write tasks to a JSON file.
    """
    with smart_open.open(tasks_path, "w") as f:
        # str then write separated for ease of testing :-/
        json_str = json.dumps(
            [
                task.model_dump() if hasattr(task, "model_dump") else task
                for task in tasks
            ]
        )
        f.write(json_str)


def create_standard_job(
    region: str,
    compute_config: ComputeConfig,
    task_count: int,
    runnables: List[Runnable],
    parallelism: int = 1,
    task_count_per_node: int = 1,
    tmp_dir: str = None,
    tmp_dir_size_gb: int = None,
    network_interface: NetworkInterfaceConfig = None,
    service_account: ServiceAccountConfig = None,
    depends_on_job_ids: List[str] = None,
) -> dict:
    job = create_job_base(
        task_count, task_count_per_node=task_count_per_node, parallelism=parallelism
    )
    for runnable in runnables:
        add_runnable(job, runnable)

    apply_allocation_policy(job, region, compute_config)
    apply_cloud_log_policy(job)

    if tmp_dir:
        add_tmp_dir(job, tmp_dir, tmp_dir_size_gb)

    if network_interface:
        add_networking_interface(job, network_interface)

    if service_account:
        add_service_account(job, service_account)

    if depends_on_job_ids:
        add_job_dependencies(job, depends_on_job_ids)

    return job


def create_job_base(
    task_count: int,
    task_count_per_node: int,
    parallelism: int,
    preemption_retry_count: int = 3,
) -> dict:
    parallelism = parallelism or 1
    return {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [],
                    "maxRetryCount": preemption_retry_count,
                    "lifecyclePolicies": [
                        {
                            "action": "RETRY_TASK",
                            "actionCondition": {"exitCodes": [50001]},
                        }
                    ],
                },
                "taskCount": task_count,
                "taskCountPerNode": task_count_per_node,
                "parallelism": parallelism,
            }
        ]
    }


def add_runnable(job: dict, runnable: Runnable) -> None:
    """
    Add a runnable to the job definition.
    """
    runnables = job["taskGroups"][0]["taskSpec"].setdefault("runnables", [])

    if isinstance(runnable, ContainerRunnable):
        runnables.append({"container": runnable.model_dump()})
    else:
        raise TypeError("Unsupported runnable type: {}".format(type(runnable)))


def apply_allocation_policy(
    job: dict,
    region: str,
    compute_config: ComputeConfig,
) -> None:
    """
    Apply an allocation policy to the job definition: machine type, provisioning model, and GPU.
    """
    if (compute_config.accelerator_type and not compute_config.accelerator_count) or (
        compute_config.accelerator_count and not compute_config.accelerator_type
    ):
        raise ValueError("GPU type and GPU count must be set together")
    if compute_config.provisioning_model not in ["SPOT", "STANDARD"]:
        raise ValueError("Provisioning model must be either SPOT or STANDARD")

    job["allocationPolicy"] = {
        "instances": [
            {
                "policy": {
                    "machineType": compute_config.machine_type,
                    "provisioningModel": compute_config.provisioning_model,
                },
            }
        ],
        "location": {"allowedLocations": [f"regions/{region}"]},
    }

    if compute_config.accelerator_type:
        job["allocationPolicy"]["instances"][0]["installGpuDrivers"] = True
        job["allocationPolicy"]["instances"][0]["policy"]["accelerators"] = [
            {
                "type": compute_config.accelerator_type,
                "count": compute_config.accelerator_count,
            }
        ]


def apply_cloud_log_policy(job: dict) -> None:
    """
    Apply a cloud logging policy to the job definition.
    """
    job["logsPolicy"] = {
        "destination": "CLOUD_LOGGING",
    }


import posixpath
from pathlib import PurePosixPath

def add_tmp_dir(
    job: dict,
    tmp_dir: str,
    tmp_dir_size_gb: int,
) -> None:
    """
    Add a temporary directory to the job definition.
    """
    # Validation for attached disks being mounted as volumes located in /mnt/disks/
    # Raises ValueError if tmp_dir is outside /mnt/disks or is not a single name (no subdirectories).
    normalized_path = posixpath.normpath(tmp_dir)
    parts = PurePosixPath(normalized_path).parts

    if len(parts) != 4 or parts[0] != "/" or parts[1] != "mnt" or parts[2] != "disks":
        raise ValueError(
            "tmp_dir must be located in /mnt/disks/ and consist of a single name (e.g., /mnt/disks/workspace)."
        )

    volume_name = "job-workspace"
    add_attached_disk(job, volume_name, tmp_dir_size_gb)
    add_job_storage_volume(job, tmp_dir, volume_name)
    set_job_environment_variable(job, "TMPDIR", tmp_dir)


def add_attached_disk(
    job: dict,
    device_name: str,
    size_gb: int,
    disk_type: str = "pd-balanced",
) -> None:
    """
    Set the boot disk size for the job definition.
    """
    disks = job["allocationPolicy"]["instances"][0]["policy"].setdefault("disks", [])

    size_gb = math.ceil(size_gb)

    disks.append(
        {
            "deviceName": device_name,
            "newDisk": {
                "type": disk_type,
                "sizeGb": max(size_gb, 1),
            },
        }
    )


def add_job_storage_volume(job: dict, tmp_dir: str, volume_name: str):
    """
    Add a volume to the job definition.
    """
    volumes = job["taskGroups"][0]["taskSpec"].setdefault("volumes", [])
    volumes.append(
        {
            "mountPath": tmp_dir,
            "deviceName": volume_name,
        }
    )
    pass


def set_job_environment_variable(
    job: dict,
    key: str,
    value: str,
) -> None:
    """
    Set an environment variable for the task in the job definition.
    """
    env = job["taskGroups"][0]["taskSpec"].setdefault("environment", {})
    env_vars = env.setdefault("variables", {})
    env_vars[key] = value


def set_runnable_environment_variable(
    runnable: dict,
    key: str,
    value: str,
) -> None:
    """
    Set an environment variable for the runnable in the job definition.
    """
    env = runnable.setdefault("environment", {})
    env_vars = env.setdefault("variables", {})
    env_vars[key] = value


def add_networking_interface(job: dict, networking_interface: NetworkInterfaceConfig):
    network_interfaces = (
        job["allocationPolicy"]
        .setdefault("network", {})
        .setdefault("networkInterfaces", [])
    )
    network_interfaces.append(networking_interface.model_dump())


def add_service_account(job: dict, service_account: ServiceAccountConfig):
    job["allocationPolicy"]["serviceAccount"] = service_account.model_dump()


def add_job_dependencies(job: dict, job_ids: list[str]):
    """
    Add job dependencies to the job definition.
    """
    job_ids = list(filter(None, job_ids))
    if not job_ids:
        return

    dependencies = job.setdefault("dependencies", [{"items": {}}])[0]
    for job_id in job_ids:
        dependencies["items"][job_id] = "SUCCEEDED"
