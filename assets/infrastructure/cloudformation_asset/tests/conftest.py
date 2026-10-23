"""Test helpers for CloudFormationAssetComponent: loads the real component.py by path (no installed package needed)."""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("cloudformation_asset_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCfClient:
    """Minimal boto3 CloudFormation client stand-in: the stack doesn't exist yet (first describe_stacks
    raises, routing to create_stack), then reports CREATE_COMPLETE immediately on the post-create poll."""
    create_stack_kwargs = None

    def __init__(self):
        self._describe_calls = 0

    def describe_stacks(self, StackName):
        self._describe_calls += 1
        if self._describe_calls == 1:
            raise Exception(f"Stack with id {StackName} does not exist")
        return {"Stacks": [{"StackStatus": "CREATE_COMPLETE", "Outputs": []}]}

    def create_stack(self, **kwargs):
        FakeCfClient.create_stack_kwargs = kwargs
        return {"StackId": "arn:aws:cloudformation:us-east-1:123:stack/x"}


class FakeSession:
    def __init__(self, **kwargs):
        pass

    def client(self, service_name, **kwargs):
        assert service_name == "cloudformation"
        return FakeCfClient()
