from streamflow.cwl.provenance.run_crate import CWLRunCrateProvenanceManager
from streamflow.provenance import prov_classes

prov_classes["run_crate"]["cwl"] = CWLRunCrateProvenanceManager
