from .component import EnrichedDbtProjectComponent

# Backward-compat alias for the old class name — retained so any customer
# with `type: dagster_component_templates.DbtDocsEnrichedProjectComponent`
# in their defs.yaml keeps working during the transition.
from .component import DbtDocsEnrichedProjectComponent  # noqa: F401

__all__ = ["EnrichedDbtProjectComponent", "DbtDocsEnrichedProjectComponent"]
