"""Shared test helpers for GoogleAdsCustomerMatchUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `google-ads` SDK is never
installed or imported in these tests -- a minimal fake client/service
stands in for the one external, paid-API boundary
(OfflineUserDataJobService), matching the convention of mocking only the
external call while exercising all of this component's own logic (dual
source resolution, hashing, validation, chunking, metadata) for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "google_ads_customer_match_upsert_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    """A fresh @asset closure per call -- needed because materializing the
    same function object twice under different DataFrames in one test file
    would otherwise share the same closed-over `df`."""
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


# ─── Fake Google Ads SDK -- mimics proto-plus message/service shape ───────


class _FakeUserIdentifier:
    def __init__(self):
        self.hashed_email = None
        self.hashed_phone_number = None


class _FakeUserData:
    def __init__(self):
        self.user_identifiers = []


class _FakeOfflineUserDataJobOperation:
    def __init__(self):
        self.create = _FakeUserData()
        self.remove = _FakeUserData()


class _FakeCustomerMatchUserListMetadata:
    def __init__(self):
        self.user_list = None


class _FakeOfflineUserDataJob:
    def __init__(self):
        self.type_ = None
        self.customer_match_user_list_metadata = _FakeCustomerMatchUserListMetadata()


class _FakeAddOfflineUserDataJobOperationsRequest:
    def __init__(self):
        self.resource_name = None
        self.operations = []
        self.enable_partial_failure = None


class _FakeCreateJobResponse:
    def __init__(self, resource_name: str):
        self.resource_name = resource_name


class FakeOfflineUserDataJobService:
    def __init__(self):
        self.create_calls = []
        self.add_calls = []
        self.run_calls = []
        self._job_counter = 0

    def create_offline_user_data_job(self, customer_id, job):
        self._job_counter += 1
        job_resource_name = f"customers/{customer_id}/offlineUserDataJobs/{self._job_counter}"
        self.create_calls.append({"customer_id": customer_id, "job": job, "resource_name": job_resource_name})
        return _FakeCreateJobResponse(job_resource_name)

    def add_offline_user_data_job_operations(self, request):
        self.add_calls.append(request)

    def run_offline_user_data_job(self, resource_name):
        self.run_calls.append(resource_name)


class _FakeOfflineUserDataJobTypeEnum:
    CUSTOMER_MATCH_USER_LIST = "CUSTOMER_MATCH_USER_LIST"


class _FakeEnums:
    OfflineUserDataJobTypeEnum = _FakeOfflineUserDataJobTypeEnum


_FAKE_TYPE_REGISTRY = {
    "OfflineUserDataJob": _FakeOfflineUserDataJob,
    "UserIdentifier": _FakeUserIdentifier,
    "OfflineUserDataJobOperation": _FakeOfflineUserDataJobOperation,
    "AddOfflineUserDataJobOperationsRequest": _FakeAddOfflineUserDataJobOperationsRequest,
}


class FakeGoogleAdsClient:
    def __init__(self):
        self.enums = _FakeEnums()
        self.offline_user_data_job_service = FakeOfflineUserDataJobService()

    def get_service(self, name):
        assert name == "OfflineUserDataJobService", f"unexpected service: {name}"
        return self.offline_user_data_job_service

    def get_type(self, name):
        try:
            return _FAKE_TYPE_REGISTRY[name]()
        except KeyError:
            raise AssertionError(f"unexpected get_type({name!r})") from None


class FakeGoogleAdsResource:
    """Stands in for the real GoogleAdsResource -- `.get_client()` returns
    the fake client above instead of a real authenticated GoogleAdsClient."""

    def __init__(self, customer_id: str = "1112223333"):
        self.customer_id = customer_id
        self.client = FakeGoogleAdsClient()

    def get_client(self):
        return self.client
