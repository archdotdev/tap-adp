# tap-adp

`tap-adp` is a Singer tap for ADP.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Configuration

### Accepted Config Options

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| client_id | True     | None    | The OAuth client ID for ADP API |
| client_secret | True     | None    | The OAuth client secret for ADP API |
| cert_public | False    | None    | Client certificate for ADP API |
| cert_private | False    | None    | Client private key for ADP API |
| user_agent | False    | None    | A custom User-Agent header to send with each request. Default is '<tap_name>/<tap_version>' |
| stream_maps | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False    | None    | User-defined config values to be used within map expressions. |
| faker_config | False    | None    | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False    | None    | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False    | None    | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |
| batch_config | False    | None    | Configuration for BATCH message capabilities. |
| batch_config.encoding | False    | None    | Specifies the format and compression of the batch files. |
| batch_config.encoding.format | False    | None    | Format to use for batch files. |
| batch_config.encoding.compression | False    | None    | Compression format to use for batch files. |
| batch_config.storage | False    | None    | Defines the storage layer to use when writing batch files |
| batch_config.storage.root | False    | None    | Root path to use when writing batch files. |
| batch_config.storage.prefix | False    | None    | Prefix to use when writing batch files. |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-adp --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

Provide client ID, client secret, private and public certificates in their respective config options.

#### Troubleshooting

If you get an error like the below, HTTP 403:

```json
{"confirmMessage":{"confirmMessageID":{"idValue":"1A5D7EC6-F59D-BDE1-01BD-619831CBAD70","schemeName":"confirmMessageID","schemeAgencyName":"WFN"},"createDateTime":"2024-12-11T19:51:24.011+0000","requestReceiptDateTime":"2024-12-11T19:51:24.011+0000","protocolStatusCode":{"codeValue":"403","shortName":"403"},"requestStatusCode":{"codeValue":"failed"},"requestMethodCode":{"codeValue":"GET"},"requestLink":{"href":"https://pulsar-marketplace-prod.es.oneadp.com/recruitment/metaservices/jobrequisitions/staffing/v1/job-requisitions","rel":"related","method":"GET"},"resourceMessages":[{"resourceMessageID":{"idValue":"jobrequisitions/staffing/v1/job-requisitions","schemeName":"resourceMessageID","schemeAgencyName":"WFN"},"processMessages":[{"resourceStatusCode":{"codeValue":"error"},"userMessage":{"messageTxt":"Forbidden - No access for given employee ID!!!"}}]}]},"skipMetadataEnvelope":false,"statusFailure":false,"statusSuccess":false,"requestKeys":[]}
```

or:

```json
{"confirmMessage":{"confirmMessageID":{"idValue":"DB5DC903-0258-2CC1-2027-13830CA07E87","schemeName":"confirmMessageID","schemeAgencyName":"WFN"},"createDateTime":"2024-12-11T19:52:24.659+0000","requestReceiptDateTime":"2024-12-11T19:52:24.659+0000","protocolStatusCode":{"codeValue":"403","shortName":"403"},"requestStatusCode":{"codeValue":"failed"},"requestMethodCode":{"codeValue":"GET"},"requestLink":{"href":"https://pulsar-marketplace-prod.es.oneadp.com/recruitment/metaservices/jobapplications/staffing/v2/job-applications","rel":"related","method":"GET"},"resourceMessages":[{"resourceMessageID":{"idValue":"jobapplications/staffing/v2/job-applications","schemeName":"resourceMessageID","schemeAgencyName":"WFN"},"processMessages":[{"resourceStatusCode":{"codeValue":"error"},"userMessage":{"messageTxt":"Forbidden - WFN-REC-MarketPlace-API User doesn't have access to get applications"}}]}]},"statusFailure":false,"statusSuccess":false,"requestKeys":[],"skipMetadataEnvelope":false}
```

containing an error similar to `"Forbidden - No access for given employee ID!!!"` or `"Forbidden - WFN-REC-MarketPlace-API User doesn't have access to get applications"`, this indicates a permissions issue with the specific user account being used to access the API (instead of an issue with the API project's scopes). We think the steps to resolve this would be something like:
1. In ADP Workforce Now, go to Setup > Security > Security Access > Profiles.
1. Select the API Central project profile.
1. Find and expand the appropriate section in the permissions list.
1. Enable the permissions for the endpoints that are failing.
1. Save your changes.

## Usage

You can easily run `tap-adp` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-adp --version
tap-adp --help
tap-adp --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-adp` CLI interface directly using `poetry run`:

```bash
poetry run tap-adp --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-adp
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-adp --version
# OR run a test `elt` pipeline:
meltano run tap-adp target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
