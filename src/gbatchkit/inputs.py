import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Type, TypeVar, Union

import google.auth.credentials
from google.cloud import storage
import smart_open

from gbatchkit.jobs import _resolve_credentials

TaskArgsType = TypeVar("TaskArgsType")


def get_task_arguments(
    task_args_cls: Type[TaskArgsType] = None,
    args: List[str] = None,
    task_name: str = None,
    credentials: Optional[Union[google.auth.credentials.Credentials, dict]] = None,
    project: Optional[str] = None,
) -> Union[TaskArgsType, dict]:
    """
    Get task arguments from command line or environment variable.

    If arguments are provided alongside a task_args_cls, parse the arguments into an arguments
    object of the given type.

    If no arguments are provided, check the GBATCHKIT_ARGS_PATH environment
    variable. If present, treat it as a path to a json array of task arguments.
    Read the entry at the BATCH_TASK_INDEX. If provided, the task_args_cls is
    used to parse the input data.

    Lastly, if neither args nor the GBATCHKIT_ARGS_PATH environment variable are
    provided, attempt to parse from process command-line arguments. task_args_cls
    is required in this case.

    :param task_args_cls: A Pydantic model class defining the expected arguments.
        (Required if parsing from the command line.)
    :param args: Command line arguments to parse. If None, sys.argv[1:] is used.
    :param task_name: Name of the task. Used to print parsing help.
    :param credentials: Credentials object or parsed JSON dict for Google Auth.
    :param str project: GCP project ID.
    :return: Parsed arguments as a Pydantic model instance or dict.
    """
    args_path = os.environ.get("GBATCHKIT_ARGS_PATH", None)

    if args and task_args_cls:
        # First priority: explicit args
        task_args = parse_cmdline_args(task_args_cls, task_name, args)
    elif args_path:
        # Second priority: environment variable
        task_args = get_batch_indexed_task(
            args_path, credentials=credentials, project=project
        )
    elif task_args_cls:
        # Third priority:
        args = sys.argv[1:] if args is None else args
        task_args = parse_cmdline_args(task_args_cls, task_name, args)
    else:
        raise ValueError(
            "Need GBATCHKIT_ARGS_PATH env, or task_args_cls to read from args"
        )

    if task_args_cls:
        return task_args_cls(**task_args)
    else:
        return task_args


def get_batch_indexed_task(
    tasks_spec_uri: str,
    credentials: Optional[Union[google.auth.credentials.Credentials, dict]] = None,
    project: Optional[str] = None,
) -> Dict:
    open_kwargs = {}
    if credentials is not None or project is not None:
        creds, resolved_project = _resolve_credentials(
            credentials=credentials, project=project
        )
        gcs_client = storage.Client(credentials=creds, project=resolved_project)
        open_kwargs["transport_params"] = {"client": gcs_client}

    with smart_open.open(tasks_spec_uri, "r", **open_kwargs) as tasks_spec_file:
        tasks_spec = json.load(tasks_spec_file)

    task_index = int(os.environ["BATCH_TASK_INDEX"])
    return tasks_spec[task_index]


def parse_cmdline_args(
    task_args_cls: Type[TaskArgsType] = None,
    task_name: str = None,
    args: List[str] = None,
) -> TaskArgsType:
    """
    Parse command line arguments.

    :param task_args_cls: A Pydantic model class defining the expected arguments. (Required if parsing from the command line.)
    :param task_name: Name of the task. Used to print parsing help.
    :param args: Command line arguments to parse. If None, sys.argv[1:] is used.
    :return: Parsed arguments as a Pydantic model instance.
    """
    task_name = task_name or (task_args_cls and task_args_cls.__name__) or "task"

    model_fields = task_args_cls.model_fields

    parser = argparse.ArgumentParser(task_name)

    for key in model_fields.keys():
        field = model_fields[key]
        parser.add_argument(
            f"--{key}",
            help=field.description,
            type=field.annotation,
            required=field.is_required(),
        )

    parsed_args = parser.parse_args(args)
    return {k: v for k, v in vars(parsed_args).items() if v is not None}
