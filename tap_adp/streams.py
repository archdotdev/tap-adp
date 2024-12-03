"""Stream type classes for tap-adp."""

from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_adp.client import ADPStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class WorkersStream(ADPStream):
    """Define custom stream."""

    name = "workers"
    path = "/hr/v2/workers"
    primary_keys = ["associateOID"]
    replication_key = None
    records_jsonpath = "$.workers[*]"
    schema_filepath = SCHEMAS_DIR / "worker.json"  # noqa: ERA001
