"""Generate schemas for some ADP streams."""

import json
import os
from decimal import Decimal
from typing import Any

from dotenv import load_dotenv
from genson import SchemaBuilder
from singer_sdk.helpers.types import Context
from singer_sdk.streams import Stream

from tap_adp.streams import JobApplicationStream, JobRequisitionStream
from tap_adp.tap import TapADP


def make_nullable(schema: dict) -> dict:
    """Make all properties in the schema nullable and remove 'required'."""
    if isinstance(schema, dict):
        if "type" in schema:
            if isinstance(schema["type"], str):
                schema["type"] = [schema["type"], "null"]
            elif isinstance(schema["type"], list) and "null" not in schema["type"]:
                schema["type"].append("null")

        # Remove 'required' key if it exists
        schema.pop("required", None)

        for value in schema.values():
            make_nullable(value)

        if "properties" in schema:
            for prop in schema["properties"].values():
                make_nullable(prop)

    return schema


def generate_schema(
    stream_instance: Stream,
    context: Context,
    output_file: str,
) -> None:
    """Generate a schema for a given stream."""
    builder = SchemaBuilder()
    records = stream_instance.get_records(context=context)

    def convert_decimal(obj: Any) -> Any:  # noqa: ANN401
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, dict):
            return {k: convert_decimal(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [convert_decimal(item) for item in obj]
        return obj

    # Process all records without limit
    for item in records:
        converted_item = convert_decimal(item)
        builder.add_object(converted_item)

    schema = builder.to_schema()  # Get the schema from builder
    schema = make_nullable(schema)  # Make all fields nullable

    if output_file is not None:
        with open(output_file, "w") as f:  # noqa: PTH123
            json.dump(schema, f, indent=2)


def main() -> None:
    """Main function."""
    # Load environment variables from .env file
    load_dotenv()

    config = {
        "client_id": os.getenv("TAP_ADP_CLIENT_ID"),
        "client_secret": os.getenv("TAP_ADP_CLIENT_SECRET"),
        "cert_private": os.getenv("TAP_ADP_CERT_PRIVATE"),
        "cert_public": os.getenv("TAP_ADP_CERT_PUBLIC"),
    }

    tap = TapADP(config=config)

    # Configure this part as-needed for the streams you want to generate schemas for.
    children = {
        "job_requisition": JobRequisitionStream(tap=tap),
        "job_application": JobApplicationStream(tap=tap),
    }

    for child_name, child_stream in children.items():
        generate_schema(
            child_stream, context=None, output_file=f"tap_adp/schemas/{child_name}.json"
        )


if __name__ == "__main__":
    main()
