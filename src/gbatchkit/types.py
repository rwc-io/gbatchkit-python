from enum import Enum
from typing import Mapping, Sequence, Union

from pydantic import BaseModel, Field


class DependencyType(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    FINISHED = "FINISHED"


# A list of job IDs (each must succeed), or a map of job ID -> condition.
JobDependencies = Union[Sequence[str], Mapping[str, Union[DependencyType, str]]]


class ComputeConfig(BaseModel):
    machine_type: str = Field(
        default="n1-standard-1",
        title="Machine Type",
        description="The machine type to use for the job.",
    )
    provisioning_model: str = Field(
        default="SPOT",
        title="Provisioning Model",
        description="The provisioning model to use for the job.",
    )
    accelerator_count: int = Field(
        default=0,
        title="Accelerator Count",
        description="The number of accelerators to use for the job.",
    )
    accelerator_type: str = Field(
        default="",
        title="Accelerator Type",
        description="The type of accelerator to use for the job.",
    )


# Parse strings in the format:
# machine_type:provisioning_model+accelerator_type:accelerator_count
# where the provisioning model and accelerator are optional.
#
# Defaults:
#   - provisioning_model: SPOT
#   - accelerator_type: none
#   - accelerator_count: 0 (or 1, if accelerator_type is given)
def parse_compute_config(compute_str: str) -> ComputeConfig:
    compute_parts = compute_str.split("+")

    if len(compute_parts) == 1:
        compute_parts = [compute_parts[0], ""]
    elif len(compute_parts) > 2:
        raise ValueError("Invalid compute config format")

    if compute_parts[0] == "":
        raise ValueError("Machine type is required")

    machine_parts = compute_parts[0].split(":")
    if len(machine_parts) == 1:
        machine_type = machine_parts[0]
        provisioning_model = "SPOT"
    elif len(machine_parts) == 2:
        machine_type, provisioning_model = machine_parts
    else:
        raise ValueError(f"Invalid machine type/provisioning model: {machine_parts}")

    accelerator_parts = compute_parts[1].split(":")
    if len(accelerator_parts) == 1:
        accelerator_type = accelerator_parts[0]
        if accelerator_type:
            accelerator_count = 1
        else:
            accelerator_count = 0
    elif len(accelerator_parts) == 2:
        accelerator_type = accelerator_parts[0]
        accelerator_count = int(accelerator_parts[1] or 0)

        if accelerator_count > 0 and not accelerator_type:
            raise ValueError(f"Accelerator count specified without accelerator type")
    else:
        raise ValueError(f"Invalid accelerator type/count: {accelerator_parts}")

    # TODO: validate that machine_type is valid
    # TODO: validate that accelerator_type is valid
    # TODO: validate that machine_type + accelerator_type are valid together

    return ComputeConfig(
        machine_type=machine_type,
        provisioning_model=provisioning_model or "SPOT",
        accelerator_type=accelerator_type or "",
        accelerator_count=int(accelerator_count or 0),
    )


class NetworkInterfaceConfig(BaseModel):
    network: str = Field(
        default="",
        title="Network",
        description="The network to use for the job.",
    )
    subnetwork: str = Field(
        default="",
        title="Subnetwork",
        description="The subnetwork to use for the job.",
    )
    no_external_ip_address: bool = Field(
        default=False,
        title="No External IP Address",
        description="True if there is no external IP address on the VM.",
    )


class ServiceAccountConfig(BaseModel):
    email: str = Field(
        default="",
        title="Service Account Email",
        description="The email of the service account to use for the job.",
    )
    scopes: list[str] = Field(
        default_factory=list,
        title="Service Account Scopes",
        description="The scopes to use for the service account.",
    )


class Runnable(BaseModel):
    pass


class ContainerRunnable(Runnable):
    image_uri: str = Field(
        title="Container Image URI",
        description="The URI of the container image to use for the job.",
    )
    entrypoint: str = Field(
        title="Entrypoint",
        description="The entrypoint to use for the container.",
    )
    commands: list[str] = Field(
        title="Commands",
        description="The command arguments.",
    )
