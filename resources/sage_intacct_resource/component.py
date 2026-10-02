"""Sage Intacct Resource.

Wraps Sage Intacct's legacy XML Web Services gateway (the "xmlgateway"):

    POST https://api.intacct.com/ia/xml/xmlgw.phtml

This is NOT a JSON REST API. Every request is a hand-built XML envelope
with two independent layers of credentials:

  1. Web Services credentials -- a `senderid` + `password` pair registered
     with Sage support and explicitly whitelisted per company (Company >
     Company Info > Security). These go in the outer `<control>` block.
  2. Company/user credentials -- `userid` + `companyid` + `password` (plus
     an optional `locationid` for multi-entity companies). These go in the
     `<operation><authentication><login>` block.

Every response is XML with a `<control><status>` (did the *sender*
credentials pass?) and a separate `<operation><result><status>` (did the
*function call itself* succeed?) -- both must be checked independently; a
control-level failure means the request never even reached company-level
auth.

Query semantics: this resource drives the legacy `readByQuery` function
(object name + comma-separated field list + a single SQL-like `query`
where-clause string), since its pagination model -- call `readByQuery` once,
then loop `readMore` with the `resultId` Intacct hands back until
`numremaining` is `0` -- is simpler to drive generically than the newer
structured `query` function's nested `<filter>`/`<select>` element tree.
See https://developer.intacct.com/web-services/queries/ for both shapes.

No XML is built with string concatenation of untrusted structure -- only
leaf text values are escaped and interpolated into a fixed envelope shape.
Parsing uses stdlib `xml.etree.ElementTree` only; no SDK dependency.
"""
import os
import uuid
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


def _xml_escape(value: Any) -> str:
    """Escape a leaf value for safe inclusion as XML element text."""
    if value is None:
        return ""
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _extract_errors(errormessage_el: Optional[ET.Element]) -> str:
    """Flatten Intacct's <errormessage><error><description2>...</error></errormessage>
    block into a single human-readable string."""
    if errormessage_el is None:
        return ""
    parts = []
    for error_el in errormessage_el.findall("error"):
        desc = error_el.findtext("description2") or error_el.findtext("description") or ""
        correction = error_el.findtext("correction") or ""
        msg = desc if not correction else f"{desc} ({correction})"
        if msg:
            parts.append(msg)
    return "; ".join(parts) if parts else "unknown Intacct error"


class SageIntacctResource(ConfigurableResource):
    """Dagster resource wrapping the Sage Intacct XML Web Services gateway.

    Call ``read_by_query(...)`` to run an initial `readByQuery`, and
    ``read_more(resultid)`` to walk subsequent pages until
    ``numremaining == 0``.
    """

    sender_id: str = Field(description="Web Services sender ID registered with Sage support")
    sender_password_env_var: str = Field(
        description="Env var holding the Web Services sender password"
    )
    company_id: str = Field(description="Intacct company ID")
    user_id: str = Field(description="Intacct user ID")
    user_password_env_var: str = Field(
        description="Env var holding the Intacct user (login) password"
    )
    entity_id: Optional[str] = Field(
        default=None,
        description="Entity/location ID for multi-entity companies (omit for single-entity companies)",
    )
    endpoint_url: str = Field(
        default="https://api.intacct.com/ia/xml/xmlgw.phtml",
        description="Intacct XML gateway endpoint",
    )
    dtdversion: str = Field(default="3.0", description="Intacct XML DTD version")
    timeout: int = Field(default=60, description="HTTP timeout in seconds")

    def _control_block(self, control_id: str) -> str:
        sender_password = os.environ.get(self.sender_password_env_var)
        if not sender_password:
            raise RuntimeError(
                f"Missing env var {self.sender_password_env_var!r} for Sage Intacct sender password"
            )
        return (
            "<control>"
            f"<senderid>{_xml_escape(self.sender_id)}</senderid>"
            f"<password>{_xml_escape(sender_password)}</password>"
            f"<controlid>{_xml_escape(control_id)}</controlid>"
            "<uniqueid>false</uniqueid>"
            f"<dtdversion>{_xml_escape(self.dtdversion)}</dtdversion>"
            "<includewhitespace>false</includewhitespace>"
            "</control>"
        )

    def _authentication_block(self) -> str:
        user_password = os.environ.get(self.user_password_env_var)
        if not user_password:
            raise RuntimeError(
                f"Missing env var {self.user_password_env_var!r} for Sage Intacct user password"
            )
        location_el = (
            f"<locationid>{_xml_escape(self.entity_id)}</locationid>" if self.entity_id else ""
        )
        return (
            "<authentication><login>"
            f"<userid>{_xml_escape(self.user_id)}</userid>"
            f"<companyid>{_xml_escape(self.company_id)}</companyid>"
            f"<password>{_xml_escape(user_password)}</password>"
            f"{location_el}"
            "</login></authentication>"
        )

    def _post(self, function_body: str) -> ET.Element:
        """POST one XML envelope wrapping `function_body` and return the parsed
        `<result>` element, raising RuntimeError on any control/auth/result
        level failure."""
        import requests

        control_id = uuid.uuid4().hex
        function_control_id = uuid.uuid4().hex
        envelope = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            "<request>"
            f"{self._control_block(control_id)}"
            '<operation transaction="false">'
            f"{self._authentication_block()}"
            "<content>"
            f'<function controlid="{function_control_id}">{function_body}</function>'
            "</content>"
            "</operation>"
            "</request>"
        )

        resp = requests.post(
            self.endpoint_url,
            data=envelope.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=self.timeout,
        )
        resp.raise_for_status()

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as e:
            raise RuntimeError(f"Sage Intacct returned unparseable XML: {e}: {resp.text[:500]}")

        control_status = (root.findtext("control/status") or "").strip()
        if control_status != "success":
            errors = _extract_errors(root.find("errormessage"))
            raise RuntimeError(f"Sage Intacct control-level failure: {errors}")

        operation = root.find("operation")
        if operation is None:
            raise RuntimeError(f"Sage Intacct response missing <operation>: {resp.text[:500]}")

        auth_status = (operation.findtext("authentication/status") or "").strip()
        if auth_status != "success":
            raise RuntimeError(
                f"Sage Intacct authentication failed for user={self.user_id!r} company={self.company_id!r}"
            )

        result = operation.find("result")
        if result is None:
            raise RuntimeError(f"Sage Intacct response missing <result>: {resp.text[:500]}")

        result_status = (result.findtext("status") or "").strip()
        if result_status != "success":
            errors = _extract_errors(result.find("errormessage"))
            raise RuntimeError(f"Sage Intacct function call failed: {errors}")

        return result

    @staticmethod
    def _parse_records(result: ET.Element) -> Dict[str, Any]:
        """Flatten a <result><data listtype="objects" count="N" numremaining="M"
        resultId="...">...<OBJECTNAME><FIELD>val</FIELD>...</OBJECTNAME>...</data>
        block into {"records": [...], "count": N, "numremaining": M, "resultid": "..."}."""
        data = result.find("data")
        records: List[Dict[str, str]] = []
        if data is not None:
            for record_el in data:
                records.append({child.tag: (child.text or "") for child in record_el})
        count = int(data.get("count", len(records))) if data is not None else len(records)
        numremaining = int(data.get("numremaining", 0) or 0) if data is not None else 0
        resultid = data.get("resultId") if data is not None else None
        return {
            "records": records,
            "count": count,
            "numremaining": numremaining,
            "resultid": resultid,
        }

    def read_by_query(
        self,
        object_name: str,
        fields: List[str],
        query: Optional[str] = None,
        pagesize: int = 100,
    ) -> Dict[str, Any]:
        """Run `readByQuery` for `object_name`, returning the first page."""
        fields_str = ",".join(fields)
        query_el = f"<query>{_xml_escape(query)}</query>" if query else ""
        function_body = (
            "<readByQuery>"
            f"<object>{_xml_escape(object_name)}</object>"
            f"<fields>{_xml_escape(fields_str)}</fields>"
            f"{query_el}"
            f"<pagesize>{int(pagesize)}</pagesize>"
            "<returnFormat>xml</returnFormat>"
            "</readByQuery>"
        )
        result = self._post(function_body)
        return self._parse_records(result)

    def read_more(self, resultid: str) -> Dict[str, Any]:
        """Continue a prior `readByQuery` via its `resultId`."""
        function_body = f"<readMore><resultId>{_xml_escape(resultid)}</resultId></readMore>"
        result = self._post(function_body)
        return self._parse_records(result)


class SageIntacctResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SageIntacctResource wrapping Sage Intacct's XML Web Services gateway."""

    resource_key: str = Field(default="sage_intacct_resource", description="Dagster resource key")
    sender_id: str = Field(description="Web Services sender ID registered with Sage support")
    sender_password_env_var: str = Field(
        description="Env var holding the Web Services sender password"
    )
    company_id: str = Field(description="Intacct company ID")
    user_id: str = Field(description="Intacct user ID")
    user_password_env_var: str = Field(
        description="Env var holding the Intacct user (login) password"
    )
    entity_id: Optional[str] = Field(
        default=None,
        description="Entity/location ID for multi-entity companies (omit for single-entity companies)",
    )
    endpoint_url: str = Field(
        default="https://api.intacct.com/ia/xml/xmlgw.phtml",
        description="Intacct XML gateway endpoint",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(
            resources={
                self.resource_key: SageIntacctResource(
                    sender_id=self.sender_id,
                    sender_password_env_var=self.sender_password_env_var,
                    company_id=self.company_id,
                    user_id=self.user_id,
                    user_password_env_var=self.user_password_env_var,
                    entity_id=self.entity_id,
                    endpoint_url=self.endpoint_url,
                )
            }
        )
