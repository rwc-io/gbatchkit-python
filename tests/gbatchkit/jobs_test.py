import json
from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import google.auth
from google.cloud import batch_v1, batch_v1alpha
from google.protobuf.json_format import ParseError
import pytest

from gbatchkit.jobs import (
    add_attached_disk,
    add_job_dependencies,
    add_tmp_dir,
    create_standard_job,
    prepare_multitask_job,
    submit_job,
)
from gbatchkit.types import (
    ComputeConfig,
    ContainerRunnable,
    NetworkInterfaceConfig,
    ServiceAccountConfig,
)


@patch("google.cloud.batch_v1.BatchServiceClient")
def test_submit_job_v1(mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    expected_created_job = MagicMock()
    mock_client.create_job.return_value = expected_created_job

    job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 1,
            }
        ]
    }

    result = submit_job(
        job, job_id="test-job-id", region="us-central1", project="my-test-project"
    )

    assert result == expected_created_job
    mock_client.create_job.assert_called_once()
    _, kwargs = mock_client.create_job.call_args
    request = kwargs["request"]
    assert request.parent == "projects/my-test-project/locations/us-central1"
    assert request.job_id == "test-job-id"
    assert isinstance(request.job, batch_v1.Job)


@patch("google.cloud.batch_v1alpha.BatchServiceClient")
def test_submit_job_v1alpha_with_dependencies(mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    expected_created_job = MagicMock()
    mock_client.create_job.return_value = expected_created_job

    job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 1,
            }
        ],
        "dependencies": [
            {
                "items": {
                    "job-id-1": "SUCCEEDED",
                }
            }
        ],
    }

    result = submit_job(
        job, job_id="test-job-id", region="us-central1", project="my-test-project"
    )

    assert result == expected_created_job
    mock_client.create_job.assert_called_once()
    _, kwargs = mock_client.create_job.call_args
    request = kwargs["request"]
    assert request.parent == "projects/my-test-project/locations/us-central1"
    assert request.job_id == "test-job-id"
    assert isinstance(request.job, batch_v1alpha.Job)


@patch("google.cloud.batch_v1.BatchServiceClient")
@patch("google.auth.default")
def test_submit_job_project_fallback(mock_auth_default, mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_auth_default.return_value = (MagicMock(), "adc-project")

    job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 1,
            }
        ]
    }

    submit_job(job, job_id="test-job-id", region="us-central1")

    mock_auth_default.assert_called_once()
    _, kwargs = mock_client.create_job.call_args
    request = kwargs["request"]
    assert request.parent == "projects/adc-project/locations/us-central1"


@patch("google.auth.default")
def test_submit_job_missing_project_raises_error(mock_auth_default):
    mock_auth_default.side_effect = google.auth.exceptions.DefaultCredentialsError()

    job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 1,
            }
        ]
    }

    with pytest.raises(
        ValueError,
        match="Project is required and could not be determined from environment",
    ):
        submit_job(job, job_id="test-job-id", region="us-central1")


def test_submit_job_validation():
    valid_job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 1,
            }
        ]
    }

    # Empty job
    with pytest.raises(ValueError, match="Job definition is empty"):
        submit_job({}, job_id="test-job", region="us-central1", project="proj")

    # Empty job_id
    with pytest.raises(ValueError, match="Job ID must be 1-63 characters"):
        submit_job(valid_job, job_id="", region="us-central1", project="proj")

    # Job ID too long (64 chars)
    with pytest.raises(ValueError, match="Job ID must be 1-63 characters"):
        submit_job(valid_job, job_id="a" * 64, region="us-central1", project="proj")

    # Empty region
    with pytest.raises(ValueError, match="Region is required"):
        submit_job(valid_job, job_id="test-job", region="", project="proj")


@patch("google.cloud.batch_v1.BatchServiceClient")
def test_submit_job_63_char_job_id_allowed(mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    valid_job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 1,
            }
        ]
    }

    submit_job(valid_job, job_id="a" * 63, region="us-central1", project="proj")
    mock_client.create_job.assert_called_once()


@patch("google.cloud.batch_v1.BatchServiceClient")
def test_submit_job_strict_unknown_fields_error(mock_client_cls):
    job_with_unknown_field = {
        "invalidUnknownField": "value",
        "taskGroups": [],
    }

    with pytest.raises(ParseError):
        submit_job(
            job_with_unknown_field,
            job_id="test-job",
            region="us-central1",
            project="proj",
        )


@patch("google.cloud.batch_v1.BatchServiceClient")
def test_submit_job_api_error_propagation(mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client
    mock_client.create_job.side_effect = google.api_core.exceptions.PermissionDenied(
        "Permission denied"
    )

    job = {
        "taskGroups": [
            {
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": "test-image",
                                "entrypoint": "test-command",
                            }
                        }
                    ]
                },
                "taskCount": 1,
            }
        ]
    }

    with pytest.raises(google.api_core.exceptions.PermissionDenied):
        submit_job(job, job_id="test-job-id", region="us-central1", project="proj")


@patch("smart_open.open", new_callable=pytest.MonkeyPatch)
def test_prepare_multitask_job_with_single_task_list(monkeypatch):
    mock_open_func = MagicMock()
    monkeypatch.setattr("smart_open.open", mock_open_func)

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

    mock_open_func.assert_called_once_with("/test-dir/tasks.json", "w")
    handle = mock_open_func.return_value.__enter__.return_value
    handle.write.assert_called_once_with(
        '[{"task_id": 1, "param": "value1"}, {"task_id": 2, "param": "value2"}, {"task_id": 3, "param": "value3"}]'
    )
    assert (
        job["taskGroups"][0]["taskSpec"]["environment"]["variables"][
            "GBATCHKIT_ARGS_PATH"
        ]
        == "/test-dir/tasks.json"
    )


def test_prepare_multitask_job_with_tasks_per_runnable():
    mock_files = {}

    def fake_smart_open(path, mode="r"):
        class FakeFile:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

            def write(self, content):
                mock_files[path] = content

        return FakeFile()

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

    with patch("smart_open.open", side_effect=fake_smart_open):
        prepare_multitask_job(
            job=job, runnable_tasks=runnable_tasks, working_directory="/test-dir"
        )

    assert mock_files["/test-dir/runnable_0_tasks.json"] == (
        '[{"task1_id": "runnable1_task1"}, {"task1_id": "runnable1_task2"}]'
    )
    assert mock_files["/test-dir/runnable_1_tasks.json"] == (
        '[{"task2_id": "runnable2_task1"}, {"task2_id": "runnable2_task2"}]'
    )

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


def test_add_tmp_dir_validation_success():
    job = {
        "allocationPolicy": {"instances": [{"policy": {}}]},
        "taskGroups": [{"taskSpec": {}}],
    }
    add_tmp_dir(job, "/mnt/disks/workspace", 10)
    assert (
        job["taskGroups"][0]["taskSpec"]["volumes"][0]["mountPath"]
        == "/mnt/disks/workspace"
    )

    add_tmp_dir(job, "/mnt/disks/workspace/", 10)


@pytest.mark.parametrize(
    "invalid_tmp_dir",
    [
        "/mnt/disks",
        "/mnt/disks/",
        "/mnt/disks/workspace/sub",
        "/mnt/disks/workspace/sub/",
        "/other/mnt/disks/workspace",
        "mnt/disks/workspace",
        "/mnt/disks/..",
        "/mnt/disks/workspace/..",
        "/mnt/disks/workspace/../..",
    ],
)
def test_add_tmp_dir_validation_failure(invalid_tmp_dir):
    job = {
        "allocationPolicy": {"instances": [{"policy": {}}]},
        "taskGroups": [{"taskSpec": {}}],
    }
    with pytest.raises(
        ValueError,
        match="tmp_dir must be located in /mnt/disks/ and consist of a single name",
    ):
        add_tmp_dir(job, invalid_tmp_dir, 10)
