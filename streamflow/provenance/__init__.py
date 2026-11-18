from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.provenance import ProvenanceManager
from streamflow.provenance.run_crate import CWLRunCrateProvenanceManager

provenance_manager_classes: MutableMapping[str, type[ProvenanceManager]] = {
    "cwl_run_crate": CWLRunCrateProvenanceManager
}
