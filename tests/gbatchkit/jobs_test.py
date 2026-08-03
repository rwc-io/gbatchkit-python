import json
from subprocess import CompletedProcess, DEVNULL, PIPE
from unittest.mock import patch, mock_open, ANY

from gbatchkit.jobs import (
    create_standard_job,
    add_job_dependencies,
    prepare_multitask_job,
    add_attached_disk,
    submit_job,
)
from gbatchkit.types import (
    ServiceAccountConfig,
    NetworkInterfaceConfig,
    ComputeConfig,
    ContainerRunnable,
)


@patch("subprocess.run")
@patch("smart_open.open", new_callable=mock_open)
def test_submit_job(mock_smart_open, mock_subprocess_run):
    job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 1,
            }
        ]
    }

    # Call the function under test
    mock_subprocess_run.return_value = CompletedProcess([], returncode=0)
    submit_job(job, job_id="test-job-id", region="us-central1")

    # Verify that the smart_open mock was used correctly
    mock_smart_open.assert_called_once_with(ANY, "w")
    handle = mock_smart_open()
    handle.write.assert_called_once_with(json.dumps(job))

    # Verify that subprocess.run was called correctly
    mock_subprocess_run.assert_called_once_with(
        [
            "gcloud",
            "batch",
            "jobs",
            "submit",
            "test-job-id",
            "--location",
            "us-central1",
            "--config",
            ANY,
        ],
        stdout=DEVNULL,
        stderr=PIPE,
    )


@patch("smart_open.open", new_callable=mock_open)
def test_prepare_multitask_job_with_single_task_list(mock_smart_open):
    job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 3,
            }
        ]
    }

    tasks = [
        {"task_id": 1, "param": "value1"},
        {"task_id": 2, "param": "value2"},
        {"task_id": 3, "param": "value3"},
    ]

    prepare_multitask_job(job=job, tasks=tasks, working_directory="/test-dir")

    mock_smart_open.assert_called_once_with("/test-dir/tasks.json", "w")
    handle = mock_smart_open()
    handle.write.assert_called_once_with(
        '[{"task_id": 1, "param": "value1"}, {"task_id": 2, "param": "value2"}, {"task_id": 3, "param": "value3"}]'
    )
    assert (
        job["taskGroups"][0]["taskSpec"]["environment"]["variables"][
            "GBATCHKIT_ARGS_PATH"
        ]
        == "/test-dir/tasks.json"
    )


@patch("smart_open.open", new_callable=mock_open)
def test_prepare_multitask_job_with_tasks_per_runnable(mock_smart_open):
    job = {
        "taskGroups": [
            {
                "taskCount": 2,
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "test-image-1",
                                "entrypoint": "test-command-1",
                            }
                        },
                        {
                            "container": {
                                "image_uri": "test-image-2",
                                "entrypoint": "test-command-2",
                            }
                        },
                    ]
                },
            }
        ]
    }

    runnable_tasks = [
        [{"task1_id": "runnable1_task1"}, {"task1_id": "runnable1_task2"}],
        [{"task2_id": "runnable2_task1"}, {"task2_id": "runnable2_task2"}],
    ]

    prepare_multitask_job(
        job=job, runnable_tasks=runnable_tasks, working_directory="/test-dir"
    )

    # Verify files and the associated calls
    expected_calls = [
        (
            ("/test-dir/runnable_0_tasks.json", "w"),
            '[{"task1_id": "runnable1_task1"}, {"task1_id": "runnable1_task2"}]',
        ),
        (
            ("/test-dir/runnable_1_tasks.json", "w"),
            '[{"task2_id": "runnable2_task1"}, {"task2_id": "runnable2_task2"}]',
        ),
    ]

    assert mock_smart_open.call_count == 2
    for call, expected in zip(mock_smart_open.call_args_list, expected_calls):
        mock_call, expected_content = expected
        assert call[0] == mock_call
        handle = mock_smart_open()
        handle.write.assert_any_call(expected_content)

    # Verify environment variables
    assert (
        job["taskGroups"][0]["taskSpec"]["runnables"][0]["environment"]["variables"][
            "GBATCHKIT_ARGS_PATH"
        ]
        == "/test-dir/runnable_0_tasks.json"
    )
    assert (
        job["taskGroups"][0]["taskSpec"]["runnables"][1]["environment"]["variables"][
            "GBATCHKIT_ARGS_PATH"
        ]
        == "/test-dir/runnable_1_tasks.json"
    )


def test_create_standard_job():
    job = create_standard_job(
        region="a-region",
        compute_config=ComputeConfig(
            machine_type="n1-standard-123",
            accelerator_type="NVIDIA_TESLA_V100",
            accelerator_count=7,
        ),
        task_count=1,
        runnables=[
            ContainerRunnable(
                image_uri="gcr.io/my-project/my-image",
                entrypoint="command",
                commands=["arg1", "arg2"],
            ),
            ContainerRunnable(
                image_uri="gcr.io/my-project/my-image-2",
                entrypoint="command-2",
                commands=["arg1-2", "arg2-2"],
            ),
        ],
        tmp_dir="/mnt/disks/tmp-workspace",
        tmp_dir_size_gb=321,
        network_interface=NetworkInterfaceConfig(
            network="projects/my-project/global/networks/my-network",
            subnetwork="projects/my-project/regions/us-central1/subnetworks/my-subnetwork",
        ),
        service_account=ServiceAccountConfig(
            email="service@account.com",
            scopes=["scope1", "scope2"],
        ),
        depends_on_job_ids=["job-id-1", "job-id-2"],
    )

    assert job == {
        "taskGroups": [
            {
                "taskSpec": {
                    "maxRetryCount": 3,
                    "lifecyclePolicies": [
                        {
                            "action": "RETRY_TASK",
                            "actionCondition": {"exitCodes": [50001]},
                        }
                    ],
                    "environment": {
                        "variables": {"TMPDIR": "/mnt/disks/tmp-workspace"},
                    },
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "gcr.io/my-project/my-image",
                                "entrypoint": "command",
                                "commands": ["arg1", "arg2"],
                            }
                        },
                        {
                            "container": {
                                "image_uri": "gcr.io/my-project/my-image-2",
                                "entrypoint": "command-2",
                                "commands": ["arg1-2", "arg2-2"],
                            }
                        },
                    ],
                    "volumes": [
                        {
                            "deviceName": "job-workspace",
                            "mountPath": "/mnt/disks/tmp-workspace",
                        }
                    ],
                },
                "taskCount": 1,
                "taskCountPerNode": 1,
                "parallelism": 1,
            }
        ],
        "allocationPolicy": {
            "instances": [
                {
                    "installGpuDrivers": True,
                    "policy": {
                        "machineType": "n1-standard-123",
                        "provisioningModel": "SPOT",
                        "accelerators": [
                            {
                                "type": "NVIDIA_TESLA_V100",
                                "count": 7,
                            }
                        ],
                        "disks": [
                            {
                                "deviceName": "job-workspace",
                                "newDisk": {
                                    "type": "pd-balanced",
                                    "sizeGb": 321,
                                },
                            }
                        ],
                    },
                }
            ],
            "location": {
                "allowedLocations": ["regions/a-region"],
            },
            "network": {
                "networkInterfaces": [
                    {
                        "network": "projects/my-project/global/networks/my-network",
                        "subnetwork": "projects/my-project/regions/us-central1/subnetworks/my-subnetwork",
                        "no_external_ip_address": False,
                    }
                ],
            },
            "serviceAccount": {
                "email": "service@account.com",
                "scopes": ["scope1", "scope2"],
            },
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
        "dependencies": [
            {
                "items": {
                    "job-id-1": "SUCCEEDED",
                    "job-id-2": "SUCCEEDED",
                }
            }
        ],
    }


def test_add_attached_disk():
    job = {
        "allocationPolicy": {
            "instances": [
                {
                    "policy": {
                        "disks": [],
                    }
                }
            ]
        }
    }

    add_attached_disk(job, "disk-1", 123.456)

    assert job["allocationPolicy"]["instances"][0]["policy"]["disks"] == [
        {
            "deviceName": "disk-1",
            "newDisk": {
                "type": "pd-balanced",
                "sizeGb": 124,
            },
        }
    ]


def test_add_dependency():
    job = {}

    add_job_dependencies(job, [])

    assert job == {}

    add_job_dependencies(job, ["job-id-1", "job-id-2"])

    assert job["dependencies"] == [
        {
            "items": {
                "job-id-1": "SUCCEEDED",
                "job-id-2": "SUCCEEDED",
            }
        }
    ]


import pytest
from gbatchkit.jobs import add_tmp_dir

def test_add_tmp_dir_validation_success():
    # Valid single name inside /mnt/disks
    job = {
        "allocationPolicy": {
            "instances": [
                {
                    "policy": {}
                }
            ]
        },
        "taskGroups": [
            {
                "taskSpec": {}
            }
        ]
    }
    # No error should be raised for valid inputs
    add_tmp_dir(job, "/mnt/disks/workspace", 10)
    assert job["taskGroups"][0]["taskSpec"]["volumes"][0]["mountPath"] == "/mnt/disks/workspace"

    # With a trailing slash (which normalizes to /mnt/disks/workspace)
    add_tmp_dir(job, "/mnt/disks/workspace/", 10)


@pytest.mark.parametrize(
    "invalid_tmp_dir",
    [
        "/mnt/disks",                    # too short / empty name
        "/mnt/disks/",                   # too short / empty name after normalization
        "/mnt/disks/workspace/sub",      # multiple path elements
        "/mnt/disks/workspace/sub/",     # multiple path elements
        "/other/mnt/disks/workspace",    # outside /mnt/disks
        "mnt/disks/workspace",           # relative path
        "/mnt/disks/..",                 # resolves to /mnt, which is outside /mnt/disks
        "/mnt/disks/workspace/..",       # resolves to /mnt/disks, which is too short
        "/mnt/disks/workspace/../..",    # resolves to /mnt, which is outside
    ]
)
def test_add_tmp_dir_validation_failure(invalid_tmp_dir):
    job = {
        "allocationPolicy": {
            "instances": [
                {
                    "policy": {}
                }
            ]
        },
        "taskGroups": [
            {
                "taskSpec": {}
            }
        ]
    }
    with pytest.raises(ValueError, match="tmp_dir must be located in /mnt/disks/ and consist of a single name"):
        add_tmp_dir(job, invalid_tmp_dir, 10)
