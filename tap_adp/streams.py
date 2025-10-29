"""Stream type classes for tap-adp."""

# ruff: noqa: ERA001

from __future__ import annotations

import sys
import typing as t
from datetime import datetime, timedelta
from http import HTTPStatus

import requests

from tap_adp.client import ADPStream, PaginatedADPStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


class SkippableAPIError(Exception):
    """Exception to skip the error when mass processing is disabled."""


class WorkersStream(PaginatedADPStream):
    """Define custom stream."""

    name = "workers"
    path = "/hr/v2/workers"
    primary_keys = ("associateOID",)
    records_jsonpath = "$.workers[*]"

    @override
    @property
    def http_headers(self) -> dict:
        headers = super().http_headers
        headers["Accept"] = "application/json;masked=false"
        return headers

    @override
    def get_child_context(self, record: Record, context: Context | None) -> dict:
        return {"_sdc_worker_aoid": record["associateOID"]}


class WorkerDemographicStream(PaginatedADPStream):
    """Worker demographic stream.

    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-hr-worker-demographics-v2-worker-demographics
    """

    name = "worker_demographic"
    path = "/hr/v2/worker-demographics"
    primary_keys = ("associateOID",)
    records_jsonpath = "$.workers[*]"


class PayDistributionStream(ADPStream):
    """Pay distribution stream.

    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-payroll-pay-distributions-v2-pay-distributions
    """

    name = "pay_distribution"
    path = "/payroll/v2/workers/{_sdc_worker_aoid}/pay-distributions"
    primary_keys = ("itemID",)
    records_jsonpath = "$.payDistributions[*]"
    parent_stream_type = WorkersStream

    @override
    def validate_response(self, response: requests.Response) -> None:
        try:
            response_json = response.json()
        except requests.JSONDecodeError:
            response_json = None
        if (
            response_json is not None
            and response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
            and response_json.get("confirmMessage", {})
            .get("resourceMessages", [{}])[0]
            .get("processMessages", [{}])[0]
            .get("processMessageID", {})
            .get("idValue")
            == "Exception in the requestHTTP 500 Internal Server Error"
        ):
            msg = f"No pay distribution found for path: {response.request.path_url}"
            self.logger.warning(msg)
            return
        super().validate_response(response)


class PayrollInstructionStream(ADPStream):
    """Payroll instruction stream.

    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-payroll-worker-payroll-instructions-v1-worker-payroll-instructions
    """

    name = "payroll_instruction"
    path = "/payroll/v1/workers/{_sdc_worker_aoid}/payroll-instructions"
    primary_keys = ("payrollAgreementID",)
    records_jsonpath = "$.payrollInstructions[*]"
    parent_stream_type = WorkersStream


# class PayDataInputStream(ADPStream):
#     """Pay data input stream.

#     Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-payroll-pay-data-input-v1-pay-data-input
#     """

#     name = "pay_data_input"
#     path = "/events/payroll/v1/pay-data-input.modify/meta"
#     primary_keys = ()
#     records_jsonpath = "$.meta"
#     schema_filepath = SCHEMAS_DIR / "pay_data_input.json"

#     @override
#     def get_url_params(
#         self,
#         context: Context | None,
#         next_page_token: t.Any | None,
#     ) -> dict[str, t.Any]:
#         params = super().get_url_params(context, next_page_token)
#         params["roleCode"] = "administrator"
#         return params


class USTaxProfileStream(ADPStream):
    """USTax profile stream.

    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-payroll-us-tax-profiles-v1-us-tax-profiles
    """

    name = "us_tax_profile"
    path = "/payroll/v1/workers/{_sdc_worker_aoid}/us-tax-profiles"
    primary_keys = ("itemID",)
    records_jsonpath = "$.usTaxProfiles[*]"
    parent_stream_type = WorkersStream

    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        if response.status_code == HTTPStatus.NOT_FOUND:
            return iter([])
        return super().parse_response(response)

    @override
    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == HTTPStatus.NOT_FOUND:
            msg = f"No US tax profile found for path: {response.request.path_url}"
            self.logger.warning(msg)
            return
        try:
            response_json = response.json()
        except requests.JSONDecodeError:
            response_json = None
        if (
            response_json is not None
            and response.status_code == HTTPStatus.BAD_REQUEST
            and response_json.get("confirmMessage", {})
            .get("resourceMessages", [{}])[0]
            .get("processMessages", [{}])[0]
            .get("userMessage", {})
            .get("messageTxt")
            == "Records are not available,  As of Date is invalid."
        ):
            msg = f"No US tax profile found for path: {response.request.path_url}"
            self.logger.warning(msg)
            return
        super().validate_response(response)


class JobRequisitionStream(PaginatedADPStream):
    """Job requisition stream.

    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-staffing-job-requisitions-v1-job-requisitions
    """

    name = "job_requisition"
    path = "/staffing/v1/job-requisitions"
    primary_keys = ("itemID",)
    records_jsonpath = "$.jobRequisitions[*]"

    @override
    def get_child_context(self, record: Record, context: Context | None) -> dict:
        return {"_sdc_requisition_id": record["itemID"]}


class JobApplicationStream(PaginatedADPStream):
    """Job application stream.

    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-staffing-job-applications-v2-job-applications
    """

    name = "job_application"
    path = "/staffing/v2/job-applications"
    primary_keys = ("itemID",)
    records_jsonpath = "$.jobApplications[*]"


class QuestionnaireStream(ADPStream):
    """Questionnaire stream.

    Docs: https://developers.adp.com/build/api-explorer/hcm-offrg-wfn/hcm-offrg-wfn-staffing-recruiting-questionnaires-v3-recruiting-questionnaires
    """

    name = "questionnaire"
    path = (
        "/staffing/v3/work-fulfillment/recruiting-questionnaires/{_sdc_requisition_id}"
    )
    primary_keys = ("questionnaireID",)
    records_jsonpath = "$"
    parent_stream_type = JobRequisitionStream


class DepartmentValidationStream(PaginatedADPStream):
    """Department validation stream."""

    name = "department"
    path = "/hcm/v1/validation-tables/departments"
    primary_keys = ("payrollGroupCode", "_sdc_namecode_code")
    records_jsonpath = "$.listItems[*]"

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        """To form primary key we needed a nested key, so we're adding it here."""
        row["_sdc_namecode_code"] = row["nameCode"]["code"]
        return row


class PayDataInputStream(ADPStream):
    """Pay data input stream."""

    name = "pay_data_input"
    path = "/payroll/v1/pay-data-input"
    primary_keys = ()
    records_jsonpath = "$.payDataInput[*]"


class PayrollOutputStream(ADPStream):
    """Payroll output stream."""

    name = "payroll_output"
    path = "/payroll/v2/payroll-output"
    primary_keys = ("itemID",)
    replication_key = "_sdc_modified_schedule_entry_id"
    records_jsonpath = "$.payrollOutputs[*]"  # There's a root level processMessages key that has metaData about the corresponding payroll(s) might be useful, ignoring for now to move forward quickly  # noqa: E501

    @override
    def get_child_context(self, record: Record, context: Context | None) -> dict:
        return {"_sdc_payroll_item_id": record["itemID"]}

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any] | str:
        params = {}
        # Date 30 days ago
        if date := self.get_starting_timestamp(context):
            date_str = date.strftime("%Y%m%d")
            self.logger.info("Payroll is using 'payPeriodEndDate ge %s'", date_str)
            params["$filter"] = f"payPeriodEndDate ge {date_str}"
        return params

    @override
    def post_process(
        self,
        record: Record,
        context: Context | None = None,
    ) -> Record | None:
        # We subtract 30 days as recent payrolls are not available to pull
        # There could be a case where a payroll completes that's more recent than
        # payrolls that havne't been completed yet so we want to play it safe and try
        # to get them all.
        # This gives us a good chance of pulling all the most recent payrolls
        record["_sdc_modified_schedule_entry_id"] = (
            datetime.strptime(  # noqa: DTZ007
                record["payrollScheduleReference"]["scheduleEntryID"][:8],
                "%Y%m%d",
            )
            - timedelta(days=30)
        )
        return record


class PayrollOutputAccStream(ADPStream):
    """Payroll output ACC stream."""

    name = "payroll_output_acc"
    path = "/payroll/v2/payroll-output"
    primary_keys = ("itemID",)
    records_jsonpath = "$.payrollOutputs[*]"
    parent_stream_type = PayrollOutputStream

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any] | str:
        # Today's date
        assert context is not None  # noqa: S101

        return {
            "level": "acc-all",
            "$filter": f"itemID eq {context['_sdc_payroll_item_id']}",
        }

    @override
    def validate_response(self, response: requests.Response) -> None:
        """Validate the response."""
        # Handle 404 errors with specific message about data loading
        if response.status_code == HTTPStatus.NOT_FOUND:
            response_json = response.json()
            if response_json.get("confirmMessage", {}).get("processMessages"):
                process_messages = response_json.get("confirmMessage", {}).get(
                    "processMessages"
                )
                for process_message in process_messages:
                    dev_message = process_message.get("developerMessage", {}).get(
                        "messageTxt", ""
                    )
                    code_value = process_message.get(
                        "developerMessage", {}
                    ).get(
                        "codeValue"
                    )  # Could use TURBOGEN000010 but this is such a weird code, I'm going with the message in case there's others that are close to this  # noqa: E501
                    if "still loading the acc-all payroll data" in dev_message:
                        msg = f"ADP API is still loading payroll data, will retry: {dev_message=}, {code_value=}"  # noqa: E501
                        self.logger.warning(msg)
                        raise SkippableAPIError(
                            msg
                        )  # Even though is is a 404, this has only happened for us when the Payroll has been in a rejected status, so we're skipping it  # noqa: E501

        if response.status_code == HTTPStatus.BAD_REQUEST and response.json().get(
            "confirmMessage", {}
        ).get("processMessages"):
            process_messages = (
                response.json().get("confirmMessage", {}).get("processMessages")
            )
            for process_message in process_messages:
                dev_message = process_message["developerMessage"]["messageTxt"]
                code_value = process_message["developerMessage"]["codeValue"]
                if dev_message == "Mass Processing is currently Disabled.":
                    exception_message = "Mass Processing is currently Disabled."
                    self.logger.warning(exception_message)
                    raise SkippableAPIError(exception_message)
                if (
                    code_value == "PAYGEN00030"
                ):  # The payroll job id provided was in an invalid state (EDL, DAT, PVE, NER, EER, etc).  # noqa: E501
                    exception_message = f"The payroll job id provided was in an invalid state ({dev_message})."  # noqa: E501
                    self.logger.warning(exception_message)
                    raise SkippableAPIError(exception_message)
                    # Default handling if this isn't hit
        super().validate_response(response)

    @override
    def get_records(self, context: Context | None) -> t.Iterable[Record]:
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
        # Works because this is a child stream of PayrollOutputStream and only has one record  # noqa: E501
        except SkippableAPIError:
            self.logger.warning("Mass Processing is currently Disabled.")
            return
