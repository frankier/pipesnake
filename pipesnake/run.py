from curio import run, gather, spawn
import functools

from .graph import stage_iter_all, is_connected
from .exceptions import UnconnectedGraphException


async def connect_pipeline(root_stage):
    for stage in stage_iter_all(root_stage):
        await stage.ensure_source_connected()
        await stage.ensure_sinks_connected()


async def start_coroutines_now(coroutines):
    tasks = []
    for cr in coroutines:
        tasks.append(await spawn(cr))
    return gather(tasks)


async def start_some_finish_some(finish, start):
    await (await start_coroutines_now(finish))
    return await start_coroutines_now(start)


def run_pipeline_awaitables(root_stage):
    if not is_connected(root_stage):
        raise UnconnectedGraphException()
    pre_coroutines = [connect_pipeline(root_stage)]
    coroutines = []
    for stage in stage_iter_all(root_stage):
        pre_coroutine = stage.run()
        if pre_coroutine:
            pre_coroutines.append(pre_coroutine)
        coroutines.append(stage.get_coroutine())

    return start_some_finish_some(pre_coroutines, coroutines)


async def run_pipeline(root_stage):
    start_awaitable = run_pipeline_awaitables(root_stage)
    run_awaitable = await start_awaitable
    return await run_awaitable


def deasync(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        return run(func(*args, **kwargs))
    return inner


run_pipeline_block = deasync(run_pipeline)
