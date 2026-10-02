import json
import os
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from gbatchkit.inputs import get_task_arguments


class TaskArgs(BaseModel):
    arg1: int
    arg2: str


@pytest.fixture
def mock_env_path(tmp_path):
    """
    Fixture for mocking GBATCHKIT_ARGS_PATH environment variable to use a temporary file.
    """
    task_data = [{"arg1": 100, "arg2": "value1"}, {"arg1": 200, "arg2": "value2"}]
    file_path = tmp_path / "task_args.json"
    with open(file_path, "w") as f:
        json.dump(task_data, f)
    os.environ["GBATCHKIT_ARGS_PATH"] = str(file_path)
    yield
    del os.environ["GBATCHKIT_ARGS_PATH"]


@pytest.fixture
def mock_task_index():
    """
    Fixture for mocking BATCH_TASK_INDEX environment variable.
    """
    os.environ["BATCH_TASK_INDEX"] = "1"
    yield
    del os.environ["BATCH_TASK_INDEX"]


def test_get_untyped_arguments_from_env(mock_env_path, mock_task_index):
    """
    Test get_task_arguments reading untyped arguments from the environment variable GBATCHKIT_ARGS_PATH.
    """
    task_args = get_task_arguments()
    assert task_args["arg1"] == 200
    assert task_args["arg2"] == "value2"


def test_get_task_arguments_from_env(mock_env_path, mock_task_index):
    """
    Test get_task_arguments reading arguments from the environment variable GBATCHKIT_ARGS_PATH.
    """
    task_args = get_task_arguments(TaskArgs)
    assert task_args.arg1 == 200
    assert task_args.arg2 == "value2"


def test_get_task_arguments_from_cmd_args():
    """
    Test get_task_arguments parsing from provided command-line arguments.
    """
    cmd_args = ["--arg1", "300", "--arg2", "value3"]
    task_args = get_task_arguments(TaskArgs, args=cmd_args)
    assert task_args.arg1 == 300
    assert task_args.arg2 == "value3"


def test_get_task_arguments_no_env_or_args():
    """
    Test get_task_arguments failure when neither GBATCHKIT_ARGS_PATH nor valid task_args_cls is provided.
    """
    with pytest.raises(
        ValueError,
        match="Need GBATCHKIT_ARGS_PATH env, or task_args_cls to read from args",
    ):
        get_task_arguments()


@patch("google.cloud.storage.Client")
@patch("smart_open.open")
def test_get_batch_indexed_task_with_credentials(
    mock_smart_open, mock_storage_client_cls, mock_task_index
):
    from unittest.mock import MagicMock
    import google.oauth2.service_account
    from gbatchkit.inputs import get_batch_indexed_task
    from tests.gbatchkit.jobs_test import make_sa_dict

    mock_storage_client = MagicMock()
    mock_storage_client_cls.return_value = mock_storage_client

    mock_file = MagicMock()
    mock_file.__enter__.return_value = mock_file
    mock_file.read.return_value = json.dumps([{"arg1": 11, "arg2": "a"}, {"arg1": 22, "arg2": "b"}])
    mock_smart_open.return_value = mock_file

    sa_dict = make_sa_dict("task-input-proj")

    result = get_batch_indexed_task("gs://bucket/tasks.json", credentials=sa_dict)

    assert result == {"arg1": 22, "arg2": "b"}
    mock_storage_client_cls.assert_called_once()
    _, storage_kwargs = mock_storage_client_cls.call_args
    assert storage_kwargs["project"] == "task-input-proj"
    assert isinstance(storage_kwargs["credentials"], google.oauth2.service_account.Credentials)

    mock_smart_open.assert_called_once_with(
        "gs://bucket/tasks.json",
        "r",
        transport_params={"client": mock_storage_client},
    )


@patch("smart_open.open")
def test_get_batch_indexed_task_without_credentials_omits_transport_params(
    mock_smart_open, mock_task_index
):
    from unittest.mock import MagicMock
    from gbatchkit.inputs import get_batch_indexed_task

    mock_file = MagicMock()
    mock_file.__enter__.return_value = mock_file
    mock_file.read.return_value = json.dumps([{"arg1": 11, "arg2": "a"}, {"arg1": 22, "arg2": "b"}])
    mock_smart_open.return_value = mock_file

    result = get_batch_indexed_task("gs://bucket/tasks.json")

    assert result == {"arg1": 22, "arg2": "b"}
    mock_smart_open.assert_called_once_with("gs://bucket/tasks.json", "r")
