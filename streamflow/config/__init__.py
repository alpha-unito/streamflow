from importlib.resources import files

ext_schemas = [
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("base")
    .joinpath("queue_manager.json"),
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("base")
    .joinpath("ssh.json"),
]
