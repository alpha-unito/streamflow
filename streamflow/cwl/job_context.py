from __future__ import annotations

import os
import threading
from collections import MutableMapping
from typing import Optional

from streamflow.connector.connector import Connector
from streamflow.data.data_manager import DataManager
from streamflow.scheduling.scheduler import Scheduler


class LocalContext(threading.local):

    def __init__(self) -> None:
        self.name: Optional[str] = None


class SfJobContext(object):
    contexts: MutableMapping[str, SfJobContext] = {}
    thread_context: LocalContext = LocalContext()

    @classmethod
    def register_context(cls,
                         context) -> None:
        cls.contexts[context.name] = context
        cls.thread_context.name = context.name

    @classmethod
    def current_context(cls) -> Optional[SfJobContext]:
        return cls.contexts[cls.thread_context.name] if cls.thread_context.name is not None else None

    @classmethod
    def get_context(cls, job_name: str) -> Optional[SfJobContext]:
        return cls.contexts.get(job_name)

    @classmethod
    def delete_by_model_name(cls, model_name: str):
        cls.contexts = {name: context for (name, context) in cls.contexts.items() if context.model_name != model_name}

    def __init__(self,
                 name: str,
                 local_outdir: str,
                 remote_outdir: str,
                 model_name: str,
                 target_resource: str,
                 connector: Connector,
                 scheduler: Scheduler,
                 data_manager: DataManager):
        self.name: str = name
        self.local_outdir = local_outdir
        self.remote_outdir = remote_outdir
        self.model_name: str = model_name
        self.target_resource: str = target_resource
        self.connector: Connector = connector
        self.scheduler: Scheduler = scheduler
        self.data_manager = data_manager

    def get_remote_path(self,
                        local_path) -> str:
        if local_path:
            return os.path.normpath(
                os.path.join(self.remote_outdir, os.path.relpath(local_path, self.local_outdir)))
        else:
            return local_path
