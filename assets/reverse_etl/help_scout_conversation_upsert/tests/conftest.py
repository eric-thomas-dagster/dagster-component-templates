"""Shared test helpers for HelpScoutConversationUpsertComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. A FakeHelpScoutResource stands in for the real
`help_scout_resource` -- the one external-call boundary -- so tests exercise
all of this component's own logic (dual source resolution, create-vs-update
branching, tag parsing, body construction, metadata) for real, without ever
touching `requests` or Help Scout's OAuth flow.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "help_scout_conversation_upsert_component", component_py
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


class FakeHelpScoutResource:
    """In-memory stand-in for `HelpScoutResource`.

    Conversations are stored in a dict keyed by int conversation id, shaped
    like `{"customer_email":..., "subject":..., "body":..., "tags": [...],
    "notes": [...], "status":...}`. Call-tracking lists let tests assert on
    exactly what was sent to each method.
    """

    def __init__(self):
        self._conversations: dict = {}
        self._id_counter = 1000

        self.create_calls: list = []
        self.update_tags_calls: list = []
        self.add_note_calls: list = []
        self.patch_status_calls: list = []

    def seed_conversation(self, conversation_id: int, **fields) -> dict:
        """Test helper -- pre-populate an existing conversation."""
        conv = {"tags": [], "notes": [], "status": "active"}
        conv.update(fields)
        self._conversations[conversation_id] = conv
        return conv

    # ----------------------------------------------------------------- writes

    def create_conversation(
        self,
        mailbox_id,
        customer_email,
        subject,
        body_text,
        type_="email",
        status="active",
        tags=None,
        customer_first_name=None,
        customer_last_name=None,
    ) -> int:
        self._id_counter += 1
        conv_id = self._id_counter
        self.create_calls.append({
            "mailbox_id": mailbox_id,
            "customer_email": customer_email,
            "subject": subject,
            "body_text": body_text,
            "type_": type_,
            "status": status,
            "tags": tags,
            "customer_first_name": customer_first_name,
            "customer_last_name": customer_last_name,
        })
        self._conversations[conv_id] = {
            "customer_email": customer_email,
            "subject": subject,
            "body": body_text,
            "tags": list(tags or []),
            "notes": [],
            "status": status,
        }
        return conv_id

    def update_tags(self, conversation_id: int, tags: list) -> None:
        self.update_tags_calls.append({"conversation_id": conversation_id, "tags": tags})
        if conversation_id in self._conversations:
            self._conversations[conversation_id]["tags"] = list(tags)

    def add_note(self, conversation_id: int, text: str) -> None:
        self.add_note_calls.append({"conversation_id": conversation_id, "text": text})
        if conversation_id in self._conversations:
            self._conversations[conversation_id]["notes"].append(text)

    def patch_status(self, conversation_id: int, status: str) -> None:
        self.patch_status_calls.append({"conversation_id": conversation_id, "status": status})
        if conversation_id in self._conversations:
            self._conversations[conversation_id]["status"] = status
