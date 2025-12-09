import pytest

from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataType
from streamflow.data.remotepath import StreamFlowPath
from tests.utils.deployment import (
    get_aiotar_deployment_config,
    get_local_deployment_config,
    get_location,
)
from tests.utils.utils import compare_remote_dirs


def _get_content(min_length: int, text: str = "") -> str:
    content = text
    while len(content) < min_length:
        content += utils.random_name()
    return content


@pytest.mark.asyncio
@pytest.mark.parametrize("tar_format", ["gnu", "pax", "posix", "ustar", "v7"])
async def test_tar_format(context: StreamFlowContext, tar_format: str) -> None:
    src_deployment_config = get_aiotar_deployment_config()
    src_deployment_config.config["tar_format"] = tar_format
    await context.deployment_manager.deploy(src_deployment_config)
    src_location = await get_location(context, src_deployment_config.type)
    src_connector = context.deployment_manager.get_connector(src_location.deployment)
    dst_deployment_config = get_local_deployment_config()
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
    src_path = await src_path.resolve()
    assert await src_path.exists()
    dst_path = await dst_path.resolve()
    assert await dst_path.exists()

    await (src_path / "a").mkdir(parents=True, exist_ok=True)
    src_buff_size = src_connector.transferBufferSize
    await (src_path / "a" / "base1.txt").write_text(
        _get_content(text="StreamFlow base1", min_length=src_buff_size * 2)
    )
    await (src_path / "base1.1.txt").hardlink_to(src_path / "a" / "base1.txt")

    await (src_path / "b").mkdir(parents=True, exist_ok=True)
    await (src_path / "b" / "base2.txt").write_text(
        _get_content(text="StreamFlow base.2", min_length=src_buff_size * 3)
    )
    await (src_path / "base1.2.txt").symlink_to(src_path / "b" / "base2.txt")
    await (src_path / "base3.txt").write_text(
        _get_content(text="StreamFlow base.3.", min_length=src_buff_size * 2)
    )

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
        dst_path=str(dst_path / src_path.name),
        writable=False,
    )
    await compare_remote_dirs(context, src_path, dst_path / src_path.name)
