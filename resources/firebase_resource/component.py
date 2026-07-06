"""Firebase Resource component.

Wraps the Firebase Admin SDK for use by other components. Handles service
account authentication (either a file path or an inline JSON blob, both via
environment variables) and idempotent app initialization — Firebase Admin
only allows one `initialize_app` call per app name.
"""

import json
import os
import threading
from typing import Any, Optional

import dagster as dg
from pydantic import Field


# Firebase Admin's initialize_app is not idempotent (raises if the app name
# already exists), so we cache resolved apps per component instance keyed by
# the resource name to survive re-imports / re-instantiations at load time.
_APP_CACHE: dict = {}
_APP_CACHE_LOCK = threading.Lock()


class FirebaseResource(dg.ConfigurableResource):
    """Dagster resource providing a Firebase Admin `App` and typed helpers.

    Authentication uses a Google service account: either a JSON key file
    (`credentials_path_env_var`) OR the raw JSON contents inline
    (`credentials_json_env_var`). The two are mutually exclusive; validation
    happens in the Component's `build_defs` before this class is ever
    instantiated so misconfiguration surfaces at load time, not run time.
    """

    project_id: str = Field(description="Firebase / GCP project id")
    credentials_path: Optional[str] = Field(
        default=None,
        description="Filesystem path to a service account JSON key file.",
    )
    credentials_json: Optional[str] = Field(
        default=None,
        description="Raw JSON contents of a service account key.",
    )
    app_name: str = Field(
        default="firebase_resource",
        description="Firebase Admin `App` name — used as the idempotency key.",
    )
    storage_bucket: Optional[str] = Field(
        default=None,
        description="Default Cloud Storage bucket (e.g. '<project>.appspot.com'). "
                    "Falls back to '<project_id>.appspot.com' when unset.",
    )
    database_url: Optional[str] = Field(
        default=None,
        description="Realtime Database URL (e.g. 'https://<project>-default-rtdb.firebaseio.com').",
    )

    def _resolve_credential(self):
        from firebase_admin import credentials  # type: ignore

        if self.credentials_path:
            return credentials.Certificate(self.credentials_path)
        if self.credentials_json:
            return credentials.Certificate(json.loads(self.credentials_json))
        # Application Default Credentials — supports Workload Identity, GCE, etc.
        return credentials.ApplicationDefault()

    def get_app(self):
        """Return the Firebase Admin `App`, initializing once per app name."""
        import firebase_admin  # type: ignore

        with _APP_CACHE_LOCK:
            cached = _APP_CACHE.get(self.app_name)
            if cached is not None:
                return cached

            options: dict = {"projectId": self.project_id}
            if self.storage_bucket:
                options["storageBucket"] = self.storage_bucket
            else:
                options["storageBucket"] = f"{self.project_id}.appspot.com"
            if self.database_url:
                options["databaseURL"] = self.database_url

            try:
                app = firebase_admin.get_app(self.app_name)
            except ValueError:
                app = firebase_admin.initialize_app(
                    self._resolve_credential(), options, name=self.app_name
                )
            _APP_CACHE[self.app_name] = app
            return app

    def get_firestore(self):
        """Return a Firestore `Client` bound to this app."""
        from firebase_admin import firestore  # type: ignore

        return firestore.client(self.get_app())

    def get_storage(self, bucket_name: Optional[str] = None):
        """Return a Cloud Storage `Bucket` (default bucket unless `bucket_name` set)."""
        from firebase_admin import storage  # type: ignore

        if bucket_name:
            return storage.bucket(bucket_name, app=self.get_app())
        return storage.bucket(app=self.get_app())

    def get_realtime_db(self, path: str = "/"):
        """Return a Realtime Database `Reference` at `path`."""
        from firebase_admin import db  # type: ignore

        return db.reference(path, app=self.get_app())


class FirebaseResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a `FirebaseResource` (Firebase Admin SDK wrapper) for use by other components.

    Auth: point at a Google service account JSON via either
    `credentials_path_env_var` (env var containing a filesystem path) OR
    `credentials_json_env_var` (env var containing the raw JSON). Exactly one
    must be set. `project_id_env_var` names the env var holding the project id.

    Other components reference this resource by the value of `resource_key`.
    """

    resource_key: str = Field(
        default="firebase_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    project_id_env_var: str = Field(
        default="FIREBASE_PROJECT_ID",
        description="Environment variable holding the Firebase project id.",
    )
    credentials_path_env_var: Optional[str] = Field(
        default="FIREBASE_CREDENTIALS_PATH",
        description="Environment variable holding a path to the service account JSON file. "
                    "Mutually exclusive with credentials_json_env_var.",
    )
    credentials_json_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable holding the raw service account JSON. "
                    "Mutually exclusive with credentials_path_env_var.",
    )
    storage_bucket_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var holding the default Cloud Storage bucket name "
                    "(e.g. '<project>.appspot.com'). If unset, defaults to <project_id>.appspot.com.",
    )
    database_url_env_var: Optional[str] = Field(
        default=None,
        description="Optional env var holding the Realtime Database URL.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Validate mutual exclusivity — path xor json, exactly one.
        path_set = bool(self.credentials_path_env_var)
        json_set = bool(self.credentials_json_env_var)
        if path_set and json_set:
            raise ValueError(
                "FirebaseResourceComponent: credentials_path_env_var and "
                "credentials_json_env_var are mutually exclusive; set exactly one."
            )
        if not path_set and not json_set:
            raise ValueError(
                "FirebaseResourceComponent: one of credentials_path_env_var or "
                "credentials_json_env_var must be set."
            )

        resource = FirebaseResource(
            project_id=dg.EnvVar(self.project_id_env_var),
            credentials_path=(
                dg.EnvVar(self.credentials_path_env_var) if path_set else None
            ),
            credentials_json=(
                dg.EnvVar(self.credentials_json_env_var) if json_set else None
            ),
            app_name=self.resource_key,
            storage_bucket=(
                dg.EnvVar(self.storage_bucket_env_var)
                if self.storage_bucket_env_var else None
            ),
            database_url=(
                dg.EnvVar(self.database_url_env_var)
                if self.database_url_env_var else None
            ),
        )
        return dg.Definitions(resources={self.resource_key: resource})
