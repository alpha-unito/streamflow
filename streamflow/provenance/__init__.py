from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.provenance import ProvenanceManager
from streamflow.provenance.run_crate import CWLRunCrateProvenanceManager

prov_classes: MutableMapping[str, MutableMapping[str, type[ProvenanceManager]]] = {
    "run_crate": {"cwl": CWLRunCrateProvenanceManager}
}
