import os
from typing import Optional

from streamflow.core.persistence import PersistenceManager, Database


class DefaultPersistenceManager(PersistenceManager):

    def __init__(self,
                 db: Database,
                 output_dir: Optional[str] = os.getcwd()):
        # Create .streamflow folder
        self.folder = os.path.join(output_dir, '.streamflow')
        os.makedirs(self.folder, exist_ok=True)
        # Call superclass
        super().__init__(db)
