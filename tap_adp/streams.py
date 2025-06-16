"""Stream type classes for tap-adp."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from importlib import resources
from urllib.parse import quote

import requests

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import RetriableAPIError
from tap_adp.client import ADPStream, PaginatedADPStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"

# Made for the Payroll ACC Class to skip the error when mass processing is disabled
class SkippableAPIError(Exception):
    pass

class WorkersStream(PaginatedADPStream):
    """Define custom stream."""

    name = "workers"
    path = "/hr/v2/workers"
    primary_keys = ["associateOID"]
    records_jsonpath = "$.workers[*]"
    schema_filepath = SCHEMAS_DIR / "worker.json"

    @property
    def http_headers(self) -> dict:
        headers = super().http_headers
        headers["Accept"] = "application/json;masked=false"
        return headers

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

    def validate_response(self, response):
        try:
            response_json = response.json()
        except requests.JSONDecodeError:
            response_json = None
        if (
            response_json is not None and
            response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
            and response_json.get("confirmMessage", {}).get("resourceMessages", [{}])[0].get("processMessages", [{}])[0].get("processMessageID", {}).get("idValue") == "Exception in the requestHTTP 500 Internal Server Error"
        ):
            msg = f"No pay distribution found for path: {response.request.path_url}"
            self.logger.warning(msg)
            return
        super().validate_response(response)

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

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        if response.status_code == HTTPStatus.NOT_FOUND:
            return iter([])
        return super().parse_response(response)

    def validate_response(self, response):
        if response.status_code == HTTPStatus.NOT_FOUND:
            msg = f"No US tax profile found for path: {response.request.path_url}"
            self.logger.warning(msg)
            return
        try:
            response_json = response.json()
        except requests.JSONDecodeError:
            response_json = None
        if (
            response_json is not None and
            response.status_code == HTTPStatus.BAD_REQUEST
            and response_json.get("confirmMessage", {}).get("resourceMessages", [{}])[0].get("processMessages", [{}])[0].get("userMessage", {}).get("messageTxt") == "Records are not available,  As of Date is invalid."
        ):
            msg = f"No US tax profile found for path: {response.request.path_url}"
            self.logger.warning(msg)
            return
        super().validate_response(response)

class JobRequisitionStream(PaginatedADPStream):
    """
    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-staffing-job-requisitions-v1-job-requisitions
    """

    name = "job_requisition"
    path = "/staffing/v1/job-requisitions"
    primary_keys = ["itemID"]
    records_jsonpath = "$.jobRequisitions[*]"
    schema_filepath = SCHEMAS_DIR / "job_requisition.json"

    def get_child_context(self, record, context):
        return {
            "_sdc_requisition_id": record["itemID"]
        }

class JobApplicationStream(PaginatedADPStream):
    """
    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-staffing-job-applications-v2-job-applications
    """

    name = "job_application"
    path = "/staffing/v2/job-applications"
    primary_keys = ["itemID"]
    records_jsonpath = "$.jobApplications[*]"
    schema_filepath = SCHEMAS_DIR / "job_application.json"

class QuestionnaireStream(ADPStream):
    """
    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-staffing-recruiting-questionnaires-v3-recruiting-questionnaires
    """

    name = "questionnaire"
    path = "/staffing/v3/work-fulfillment/recruiting-questionnaires/{_sdc_requisition_id}"
    primary_keys = ["questionnaireID"]
    records_jsonpath = "$"
    schema_filepath = SCHEMAS_DIR / "questionnaire.json"
    parent_stream_type=JobRequisitionStream

class DepartmentValidationStream(PaginatedADPStream):
    name = "department"
    path = "/hcm/v1/validation-tables/departments"
    primary_keys = ["payrollGroupCode", "_sdc_namecode_code"]
    records_jsonpath = "$.listItems[*]"
    schema_filepath = SCHEMAS_DIR / "department.json"
    def post_process(
        self,
        row: dict,
        context: dict | None,
    ) -> dict | None:
        """
        To form primary key we needed a nested key, so we're adding it here.
        """
        row["_sdc_namecode_code"] = row["nameCode"]["code"]
        return row

class PayDataInputStream(ADPStream):
    name = "pay_data_input"
    path = "/payroll/v1/pay-data-input"
    primary_keys = []
    records_jsonpath = "$.payDataInput[*]"
    schema_filepath = SCHEMAS_DIR / "pay_data_input.json"

class PayrollOutputStream(ADPStream):
    name = "payroll_output"
    path = "/payroll/v2/payroll-output"
    primary_keys = ["itemID"]
    replication_key = "_sdc_modified_schedule_entry_id"
    records_jsonpath = "$.payrollOutputs[*]" #There's a root level processMessages key that has metaData about the corresponding payroll(s) might be useful, ignoring for now to move forward quickly
    schema_filepath = SCHEMAS_DIR / "payroll_output.json"

    def get_child_context(self, record, context):
        return {
            "_sdc_payroll_item_id": record["itemID"]
        }
    
    def get_url_params(  # noqa: PLR6301
        self,
        context,
        next_page_token
    ) -> dict[str, t.Any] | str:
        # Date 30 days ago
        date = self.get_starting_timestamp(context)
        date_str = date.strftime("%Y%m%d")
        self.logger.info(f"Payroll is using 'payPeriodEndDate ge {date_str}'")
        return {
            "$filter": f"payPeriodEndDate ge {date_str}"
        }
    
    def post_process(self, record, context):
        # We subtract 30 days as recent payrolls are not available to pull
        # There could be a case where a payroll completes that's more recent than payrolls that havne't been completed yet so we want to play it safe and try to get them all
        # This gives us a good chance of pulling all the most recent payrolls
        record["_sdc_modified_schedule_entry_id"] = datetime.strptime(record["payrollScheduleReference"]["scheduleEntryID"][:8], "%Y%m%d")-timedelta(days=30)
        return record

class PayrollOutputAccStream(ADPStream):
    name = "payroll_output_acc"
    path = "/payroll/v2/payroll-output"
    primary_keys = ["itemID"]
    records_jsonpath = "$.payrollOutputs[*]"
    schema_filepath = SCHEMAS_DIR / "payroll_output_acc.json"
    parent_stream_type=PayrollOutputStream

    def get_url_params(  # noqa: PLR6301
        self,
        context,
        next_page_token
    ) -> dict[str, t.Any] | str:
        # Today's date
        return {
            "level": "acc-all",
            "$filter": f"itemID eq {context['_sdc_payroll_item_id']}"
        }
    
    def validate_response(self, response: requests.Response) -> None:
        # Handle 404 errors with specific message about data loading
        if response.status_code == 404:
            response_json = response.json()
            if response_json.get("confirmMessage", {}).get("processMessages"):
                process_messages = response_json.get("confirmMessage", {}).get("processMessages")
                for process_message in process_messages:
                    dev_message= process_message.get("developerMessage", {}).get("messageTxt", "")
                    code_value = process_message.get("developerMessage", {}).get("codeValue") # Could use TURBOGEN000010 but this is such a weird code, I'm going with the message in case there's others that are close to this
                    if "still loading the acc-all payroll data" in dev_message:
                        raise RetriableAPIError(f"ADP API is still loading payroll data, will retry: {dev_message=}, {code_value=}", response)
        
        if response.status_code == 400 and response.json().get("confirmMessage", {}).get("processMessages"):
            process_messages = response.json().get("confirmMessage", {}).get("processMessages")
            for process_message in process_messages:
                dev_message = process_message["developerMessage"]["messageTxt"]
                code_value = process_message["developerMessage"]["codeValue"]
                if dev_message == "Mass Processing is currently Disabled.":
                    exception_message = "Mass Processing is currently Disabled."
                    self.logger.warning(exception_message)
                    raise SkippableAPIError(exception_message)
                if code_value == "PAYGEN00030": #The payroll job id provided was in an invalid state (EDL, DAT, PVE, NER, EER, etc).
                    exception_message = f"The payroll job id provided was in an invalid state ({dev_message})."
                    self.logger.warning(exception_message)
                    raise SkippableAPIError(exception_message)
                    # Default handling if this isn't hit
        super().validate_response(response)
    
    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        try:
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record
        # Works because this is a child stream of PayrollOutputStream and only has one record
        except SkippableAPIError:
            self.logger.warning("Mass Processing is currently Disabled.")
            return