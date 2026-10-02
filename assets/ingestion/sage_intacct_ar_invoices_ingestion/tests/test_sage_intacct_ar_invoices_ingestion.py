"""Tests for SageIntacctArInvoicesIngestionComponent + SageIntacctResource.

Mocks ONLY the external HTTP call (`requests.post` to Intacct's XML
gateway). Everything else -- XML envelope construction, XML->dict response
parsing, readByQuery/readMore pagination walking, date-literal conversion,
query-clause building, partitions_def construction, and asset execution /
preview metadata -- runs for real.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from dagster import DailyPartitionsDefinition, StaticPartitionsDefinition, materialize

from .conftest import load_component_module, load_resource_module

component_mod = load_component_module()
resource_mod = load_resource_module()

SageIntacctArInvoicesIngestionComponent = component_mod.SageIntacctArInvoicesIngestionComponent
_build_partitions_def = component_mod._build_partitions_def
_to_intacct_date = component_mod._to_intacct_date
_build_query_clause = component_mod._build_query_clause

SageIntacctResource = resource_mod.SageIntacctResource


# --- A realistic Intacct XML response, matching the documented envelope shape ---
# (https://developer.intacct.com/web-services/your-first-api-calls/)
SAMPLE_SUCCESS_XML_PAGE_1 = """<?xml version="1.0" encoding="UTF-8"?>
<response>
  <control>
    <status>success</status>
    <senderid>dagster_integration</senderid>
    <controlid>abc123</controlid>
    <uniqueid>false</uniqueid>
    <dtdversion>3.0</dtdversion>
  </control>
  <operation>
    <authentication>
      <status>success</status>
      <userid>api_user</userid>
      <companyid>acme-co</companyid>
      <sessiontimestamp>2026-06-01T00:00:00+00:00</sessiontimestamp>
    </authentication>
    <result>
      <status>success</status>
      <function>readByQuery</function>
      <controlid>func-1</controlid>
      <data listtype="objects" count="2" totalcount="3" numremaining="1" resultId="RESULT-XYZ">
        <ARINVOICE>
          <RECORDNO>101</RECORDNO>
          <INVOICENO>INV-1001</INVOICENO>
          <CUSTOMERID>CUST-1</CUSTOMERID>
          <CUSTOMERNAME>Acme Co</CUSTOMERNAME>
          <WHENCREATED>06/01/2026</WHENCREATED>
          <TOTALDUE>500.00</TOTALDUE>
          <STATE>Open</STATE>
        </ARINVOICE>
        <ARINVOICE>
          <RECORDNO>102</RECORDNO>
          <INVOICENO>INV-1002</INVOICENO>
          <CUSTOMERID>CUST-2</CUSTOMERID>
          <CUSTOMERNAME>Globex Corp</CUSTOMERNAME>
          <WHENCREATED>06/05/2026</WHENCREATED>
          <TOTALDUE>1200.50</TOTALDUE>
          <STATE>Open</STATE>
        </ARINVOICE>
      </data>
    </result>
  </operation>
</response>
"""

SAMPLE_SUCCESS_XML_PAGE_2 = """<?xml version="1.0" encoding="UTF-8"?>
<response>
  <control><status>success</status></control>
  <operation>
    <authentication><status>success</status></authentication>
    <result>
      <status>success</status>
      <function>readMore</function>
      <controlid>func-2</controlid>
      <data listtype="objects" count="1" totalcount="3" numremaining="0">
        <ARINVOICE>
          <RECORDNO>103</RECORDNO>
          <INVOICENO>INV-1003</INVOICENO>
          <CUSTOMERID>CUST-3</CUSTOMERID>
          <CUSTOMERNAME>Initech</CUSTOMERNAME>
          <WHENCREATED>06/10/2026</WHENCREATED>
          <TOTALDUE>75.25</TOTALDUE>
          <STATE>Paid</STATE>
        </ARINVOICE>
      </data>
    </result>
  </operation>
</response>
"""

SAMPLE_CONTROL_FAILURE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<response>
  <control>
    <status>failure</status>
  </control>
  <errormessage>
    <error>
      <errorno>XL03000006</errorno>
      <description2>Sender ID/Password is invalid.</description2>
    </error>
  </errormessage>
</response>
"""

SAMPLE_AUTH_FAILURE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<response>
  <control><status>success</status></control>
  <operation>
    <authentication>
      <status>failure</status>
    </authentication>
  </operation>
</response>
"""

SAMPLE_RESULT_FAILURE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<response>
  <control><status>success</status></control>
  <operation>
    <authentication><status>success</status></authentication>
    <result>
      <status>failure</status>
      <function>readByQuery</function>
      <controlid>func-1</controlid>
      <errormessage>
        <error>
          <description2>Invalid object name specified.</description2>
          <correction>Check the object name and try again.</correction>
        </error>
      </errormessage>
    </result>
  </operation>
</response>
"""


def _mock_response(xml_text, status_code=200):
    resp = MagicMock()
    resp.text = xml_text
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture
def intacct_resource(monkeypatch):
    monkeypatch.setenv("INTACCT_SENDER_PW", "sender-secret")
    monkeypatch.setenv("INTACCT_USER_PW", "user-secret")
    return SageIntacctResource(
        sender_id="dagster_integration",
        sender_password_env_var="INTACCT_SENDER_PW",
        company_id="acme-co",
        user_id="api_user",
        user_password_env_var="INTACCT_USER_PW",
    )


# --- 1. XML response parsing against a realistic sample response ------------

def test_read_by_query_parses_realistic_xml_response(intacct_resource):
    with patch("requests.post", return_value=_mock_response(SAMPLE_SUCCESS_XML_PAGE_1)) as mock_post:
        result = intacct_resource.read_by_query(
            object_name="ARINVOICE",
            fields=["RECORDNO", "INVOICENO", "CUSTOMERNAME", "TOTALDUE", "STATE"],
            query="WHENCREATED >= '06/01/2026'",
            pagesize=100,
        )
    assert mock_post.called
    assert result["count"] == 2
    assert result["numremaining"] == 1
    assert result["resultid"] == "RESULT-XYZ"
    assert len(result["records"]) == 2
    assert result["records"][0]["RECORDNO"] == "101"
    assert result["records"][0]["CUSTOMERNAME"] == "Acme Co"
    assert result["records"][1]["INVOICENO"] == "INV-1002"


def test_read_by_query_sends_correct_xml_envelope_shape(intacct_resource):
    with patch("requests.post", return_value=_mock_response(SAMPLE_SUCCESS_XML_PAGE_1)) as mock_post:
        intacct_resource.read_by_query(object_name="ARINVOICE", fields=["RECORDNO"], query="STATE = 'Open'")
    _, kwargs = mock_post.call_args
    sent_xml = kwargs["data"].decode("utf-8")
    # Two independent credential layers must both be present.
    assert "<senderid>dagster_integration</senderid>" in sent_xml
    assert "<password>sender-secret</password>" in sent_xml
    assert "<userid>api_user</userid>" in sent_xml
    assert "<companyid>acme-co</companyid>" in sent_xml
    assert "<password>user-secret</password>" in sent_xml
    assert "<object>ARINVOICE</object>" in sent_xml
    assert "<readByQuery>" in sent_xml
    assert kwargs["headers"]["Content-Type"] == "application/xml"


def test_read_more_continues_pagination(intacct_resource):
    with patch("requests.post", return_value=_mock_response(SAMPLE_SUCCESS_XML_PAGE_2)) as mock_post:
        result = intacct_resource.read_more("RESULT-XYZ")
    sent_xml = mock_post.call_args.kwargs["data"].decode("utf-8")
    assert "<resultId>RESULT-XYZ</resultId>" in sent_xml
    assert result["numremaining"] == 0
    assert len(result["records"]) == 1
    assert result["records"][0]["RECORDNO"] == "103"


def test_control_level_failure_raises(intacct_resource):
    with patch("requests.post", return_value=_mock_response(SAMPLE_CONTROL_FAILURE_XML)):
        with pytest.raises(RuntimeError, match="control-level failure"):
            intacct_resource.read_by_query("ARINVOICE", ["RECORDNO"])


def test_authentication_failure_raises(intacct_resource):
    with patch("requests.post", return_value=_mock_response(SAMPLE_AUTH_FAILURE_XML)):
        with pytest.raises(RuntimeError, match="authentication failed"):
            intacct_resource.read_by_query("ARINVOICE", ["RECORDNO"])


def test_result_level_failure_raises_with_error_detail(intacct_resource):
    with patch("requests.post", return_value=_mock_response(SAMPLE_RESULT_FAILURE_XML)):
        with pytest.raises(RuntimeError, match="Invalid object name"):
            intacct_resource.read_by_query("BADOBJECT", ["RECORDNO"])


def test_missing_sender_password_env_var_raises(monkeypatch):
    monkeypatch.delenv("MISSING_SENDER_PW", raising=False)
    resource = SageIntacctResource(
        sender_id="x",
        sender_password_env_var="MISSING_SENDER_PW",
        company_id="co",
        user_id="u",
        user_password_env_var="MISSING_USER_PW",
    )
    with pytest.raises(RuntimeError, match="MISSING_SENDER_PW"):
        resource.read_by_query("ARINVOICE", ["RECORDNO"])


# --- 2. Date conversion + query clause building ------------------------------

def test_to_intacct_date_converts_iso_date():
    assert _to_intacct_date("2026-06-01") == "06/01/2026"


def test_to_intacct_date_converts_iso_datetime():
    assert _to_intacct_date("2026-06-01T12:30:00") == "06/01/2026"


def test_to_intacct_date_rejects_garbage():
    with pytest.raises(ValueError):
        _to_intacct_date("not-a-date")


def test_build_query_clause_combines_from_to_and_extra():
    clause = _build_query_clause("WHENCREATED", "2026-06-01", "2026-07-01", "STATE = 'Open'")
    assert clause == "WHENCREATED >= '06/01/2026' AND WHENCREATED <= '07/01/2026' AND (STATE = 'Open')"


def test_build_query_clause_returns_none_when_all_empty():
    assert _build_query_clause("WHENCREATED", None, None, None) is None


def test_build_query_clause_from_only():
    clause = _build_query_clause("WHENCREATED", "2026-06-01", None, None)
    assert clause == "WHENCREATED >= '06/01/2026'"


# --- 3. Partitions helper -----------------------------------------------------

def test_build_partitions_def_none_when_unset():
    assert _build_partitions_def(None, None, None, None, None) is None


def test_build_partitions_def_daily():
    pdef = _build_partitions_def("daily", "2026-01-01", None, None, None)
    assert isinstance(pdef, DailyPartitionsDefinition)


def test_build_partitions_def_static():
    pdef = _build_partitions_def("static", None, "us,eu", None, None)
    assert isinstance(pdef, StaticPartitionsDefinition)
    assert set(pdef.get_partition_keys()) == {"us", "eu"}


def test_build_partitions_def_daily_without_start_raises():
    with pytest.raises(ValueError, match="requires partition_start"):
        _build_partitions_def("daily", None, None, None, None)


# --- 4. Full asset execution (fake resource client, no real HTTP) -----------

class _FakeSageIntacctClient:
    """Stands in for the resource at `context.resources.<resource_name>` --
    the asset calls `client.read_by_query` / `client.read_more` directly, so
    this fake just needs those two methods."""

    def __init__(self, pages):
        self._pages = list(pages)
        self.calls = []

    def read_by_query(self, **kwargs):
        self.calls.append(("read_by_query", kwargs))
        return self._pages.pop(0)

    def read_more(self, resultid):
        self.calls.append(("read_more", resultid))
        return self._pages.pop(0)


def test_asset_execution_builds_dataframe_and_paginates():
    component = SageIntacctArInvoicesIngestionComponent(
        asset_name="sage_intacct_ar_invoices",
        from_date="2026-06-01",
        to_date="2026-07-01",
        limit=1000,
    )
    defs = component.build_defs(context=MagicMock())
    asset_def = list(defs.assets)[0]

    fake_client = _FakeSageIntacctClient(
        pages=[
            {
                "records": [{"RECORDNO": "101", "TOTALDUE": "500.00"}],
                "count": 1,
                "numremaining": 1,
                "resultid": "R1",
            },
            {
                "records": [{"RECORDNO": "102", "TOTALDUE": "75.00"}],
                "count": 1,
                "numremaining": 0,
                "resultid": None,
            },
        ]
    )

    result = materialize(
        [asset_def],
        resources={"sage_intacct_resource": fake_client},
    )
    assert result.success
    mat_events = result.get_asset_materialization_events()
    metadata = mat_events[0].materialization.metadata
    assert metadata["row_count"].value == 2
    assert "preview" in metadata
    assert fake_client.calls[0][0] == "read_by_query"
    assert fake_client.calls[1] == ("read_more", "R1")


def test_asset_execution_empty_result_set():
    component = SageIntacctArInvoicesIngestionComponent(
        asset_name="sage_intacct_ar_invoices_empty",
        from_date="2026-06-01",
        to_date="2026-07-01",
    )
    defs = component.build_defs(context=MagicMock())
    asset_def = list(defs.assets)[0]

    fake_client = _FakeSageIntacctClient(
        pages=[{"records": [], "count": 0, "numremaining": 0, "resultid": None}]
    )
    result = materialize([asset_def], resources={"sage_intacct_resource": fake_client})
    assert result.success
    metadata = result.get_asset_materialization_events()[0].materialization.metadata
    assert metadata["row_count"].value == 0


def test_asset_requires_date_window_or_extra_query():
    component = SageIntacctArInvoicesIngestionComponent(asset_name="sage_intacct_ar_invoices_no_window")
    defs = component.build_defs(context=MagicMock())
    asset_def = list(defs.assets)[0]
    fake_client = _FakeSageIntacctClient(pages=[])
    result = materialize(
        [asset_def],
        resources={"sage_intacct_resource": fake_client},
        raise_on_error=False,
    )
    assert not result.success
