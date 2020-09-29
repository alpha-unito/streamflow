import asyncio
import logging
import time

PROFILE = 15

logging.addLevelName(PROFILE, 'PROFILE')
logger = logging.getLogger("streamflow")
defaultStreamHandler = logging.StreamHandler()
logger.addHandler(defaultStreamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False


def profile(f):
    async def caller(func, *args, **kwargs):
        return await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

    if logger.isEnabledFor(PROFILE):
        async def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            out = await caller(f, *args, **kwargs)
            end_time = time.perf_counter()
            logger.log(PROFILE, "Function {f} executed in {time:.2f}ms".format(
                f=f.__name__,
                time=(end_time - start_time) * 1000
            ))
            return out

        return wrapper
    else:
        return f
