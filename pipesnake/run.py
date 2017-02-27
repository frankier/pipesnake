import asyncio
import functools

from .graph import stage_iter_all, is_connected
from .exceptions import UnconnectedGraphException

loop = asyncio.get_event_loop()


def connect_pipeline(root_stage):
    for stage in stage_iter_all(root_stage):
        stage.ensure_sources_connected()
        stage.ensure_sinks_connected()


async def run_coroutines(coroutines):
    print('run_coroutines', coroutines)
    return await asyncio.gather(
        *(asyncio.ensure_future(cr) for cr in coroutines))


def run_pipeline_awaitables(root_stage):
    if not is_connected(root_stage):
        raise UnconnectedGraphException()
    connect_pipeline(root_stage)
    pre_coroutines = []
    coroutines = []
    for stage in stage_iter_all(root_stage):
        pre_coroutine = stage.run()
        if pre_coroutine:
            pre_coroutines.append(pre_coroutine)
        coroutines.append(stage.get_coroutine())

    print(pre_coroutines, coroutines)

    return (
        run_coroutines(pre_coroutines),
        run_coroutines(coroutines))


async def run_pipeline(root_stage):
    pre_awaitable, awaitable = run_pipeline_awaitables(root_stage)

    await pre_awaitable
    return await awaitable


def deasync(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        return loop.run_until_complete(func(*args, **kwargs))
    return inner


run_pipeline_block = deasync(run_pipeline)
