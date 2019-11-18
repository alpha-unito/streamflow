class RemotePath(object):

    def __init__(self,
                 resource: str,
                 remote_path: str) -> None:
        self.resource: str = resource
        self.path: str = remote_path
