from __future__ import annotations

from collections.abc import MutableMapping

from streamflow.core.provenance import ProvenanceManager

prov_classes: MutableMapping[str, MutableMapping[str, type[ProvenanceManager]]] = {
    "run_crate": {}
}
