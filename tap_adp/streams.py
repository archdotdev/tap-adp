"""Stream type classes for tap-adp."""

from __future__ import annotations

import typing as t
from importlib import resources

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_adp.client import ADPStream, PaginatedADPStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"


class WorkersStream(PaginatedADPStream):
    """Define custom stream."""

    name = "workers"
    path = "/hr/v2/workers"
    primary_keys = ["associateOID"]
    records_jsonpath = "$.workers[*]"
    schema_filepath = SCHEMAS_DIR / "worker.json"

    def get_child_context(self, record, context):
        return {
            "_sdc_worker_aoid": record["associateOID"]
        }


class WorkerDemographicStream(PaginatedADPStream):
    """
    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-hr-worker-demographics-v2-worker-demographics
    """

    name = "worker_demographic"
    path = "/hr/v2/worker-demographics"
    primary_keys = ["associateOID"]
    records_jsonpath = "$.workers[*]"
    schema_filepath = SCHEMAS_DIR / "worker_demographic.json"


class PayDistributionStream(ADPStream):
    """
    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-payroll-pay-distributions-v2-pay-distributions
    """

    name = "pay_distribution"
    path = "/payroll/v2/workers/{_sdc_worker_aoid}/pay-distributions"
    primary_keys = ["itemID"]
    records_jsonpath = "$.payDistributions[*]"
    schema_filepath = SCHEMAS_DIR / "pay_distribution.json"
    parent_stream_type=WorkersStream


class PayrollInstructionStream(ADPStream):
    """
    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-payroll-worker-payroll-instructions-v1-worker-payroll-instructions
    """

    name = "payroll_instruction"
    path = "/payroll/v1/workers/{_sdc_worker_aoid}/payroll-instructions"
    primary_keys = ["payrollAgreementID"]
    records_jsonpath = "$.payrollInstructions[*]"
    schema_filepath = SCHEMAS_DIR / "payroll_instruction.json"
    parent_stream_type=WorkersStream


class PayDataInputStream(ADPStream):
    """
    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-payroll-pay-data-input-v1-pay-data-input
    """

    name = "pay_data_input"
    path = "/events/payroll/v1/pay-data-input.modify/meta"
    primary_keys = []
    records_jsonpath = "$.meta"
    schema_filepath = SCHEMAS_DIR / "pay_data_input.json"

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        params["roleCode"] = "administrator"
        return params


class USTaxProfileStream(ADPStream):
    """
    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-payroll-us-tax-profiles-v1-us-tax-profiles
    """

    name = "us_tax_profile"
    path = "/payroll/v1/workers/{_sdc_worker_aoid}/us-tax-profiles"
    primary_keys = ["itemID"]
    records_jsonpath = "$.usTaxProfiles[*]"
    schema_filepath = SCHEMAS_DIR / "us_tax_profile.json"
    parent_stream_type=WorkersStream