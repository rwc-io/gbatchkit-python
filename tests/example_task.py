#!/usr/bin/env python

# Call me like so:
# python example_task.py --a_str foo --an_int 42
# Or:
# GBATCHKIT_ARGS_PATH=example-data.json BATCH_TASK_INDEX=0 python example_task.py

from pydantic import BaseModel, Field

from gbatchkit.inputs import get_task_arguments


class TypedArgs(BaseModel):
    a_str: str = Field("default")
    an_int: int = Field(1)


def main():
    try:
        untyped_args = get_task_arguments()
    except ValueError:
        untyped_args = {}
    print("Untyped args:", untyped_args)

    typed_args = get_task_arguments(task_args_cls=TypedArgs)
    print("Typed args:", typed_args)

    parsed_args = get_task_arguments(
        task_args_cls=TypedArgs, args=["--a_str", "foo", "--an_int", "42"]
    )
    print("Parsed args:", parsed_args)


if __name__ == "__main__":
    main()
