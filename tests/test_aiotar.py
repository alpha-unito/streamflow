import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.data.remotepath import StreamFlowPath
from tests.test_transfer import _compare_remote_dirs
from tests.utils.deployment import (
    get_aiotar_deployment_config,
    get_docker_deployment_config,
    get_local_deployment_config,
    get_location,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("tar_format", ["gnu"])
async def test_aiotarstream(context: StreamFlowContext, tar_format: str) -> None:
    src_deployment_config = get_aiotar_deployment_config()
    src_deployment_config.config["tar_format"] = tar_format
    await context.deployment_manager.deploy(src_deployment_config)
    src_location = await get_location(context, src_deployment_config.type)
    # dst_deployment_config = get_local_deployment_config()
    dst_deployment_config = get_docker_deployment_config()
    dst_location = await get_location(context, dst_deployment_config.type)

    src_path = StreamFlowPath(
        src_deployment_config.workdir,
        utils.random_name(),
        context=context,
        location=src_location,
    )
    dst_path = StreamFlowPath(
        dst_deployment_config.workdir,
        utils.random_name(),
        context=context,
        location=dst_location,
    )
    await dst_path.mkdir(parents=True, exist_ok=True)
    await src_path.mkdir(parents=True, exist_ok=True)
    assert await src_path.exists()
    assert await dst_path.exists()

    await (src_path / "a").mkdir(parents=True, exist_ok=True)
    await (src_path / "a" / "base1").write_text("Streamflow")
    # await (src_path / "base").hardlink_to(src_path / "a" / "base1")

    context.data_manager.register_path(
        location=src_location,
        path=str(src_path),
        relpath=str(src_path),
        data_type=DataType.PRIMARY,
    )
    await context.data_manager.transfer_data(
        src_location=src_location,
        src_path=str(src_path),
        dst_locations=[dst_location],
        dst_path=str(dst_path),
        writable=False,
    )
    await _compare_remote_dirs(context, src_location, src_path, dst_location, dst_path)
