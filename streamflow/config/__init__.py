from importlib_resources import files

ext_schemas = [
    files("streamflow.deployment.connector")
    .joinpath("schemas")
    .joinpath("queue_manager.json")
]
