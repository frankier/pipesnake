import os
from collections import namedtuple

from curio import spawn
from .graph import unsinked_iter, unsourced_iter
from .run import run_pipeline_awaitables
from .stages import (STDIN_SOURCE, STDOUT_SINK, OSPipeNet, PortStage,
                     SinkConnector, SinkSuppliesFileLikeNet, SourceConnector,
                     SourceSuppliesFileLikeNet, dispatch, areadable_fp_to_fl,
                     sink_unsinked, source_unsourced, awritable_fp_to_fl)


def connect_input_output(root):
    sink_unsinked(root, STDOUT_SINK, only_source_name='output')
    source_unsourced(root, STDIN_SOURCE, only_sink_name='input')


class NeedsFileLikeSourceConnector(SourceConnector):
    supported_nets = (SinkSuppliesFileLikeNet, OSPipeNet)


class NeedsFileLikeSinkConnector(SinkConnector):
    supported_nets = (SourceSuppliesFileLikeNet, OSPipeNet)


class BaseFnStage(PortStage):
    sinks_avail = ('input',)
    sources_avail = ('output',)
    sink_connectors = {
        'input': NeedsFileLikeSinkConnector('input'),
    }
    source_connectors = {
        'output': NeedsFileLikeSourceConnector('output'),
    }

    def run(self):
        pass

    def get_coroutine(self):
        return self.create_and_wait()


class LineProcFnStage(BaseFnStage):
    def __init__(self, fn):
        self.fn = fn
        super().__init__()

    async def create_and_wait(self):
        input = self.sink_ports['input']
        output = self.source_ports['output']
        async for line in input:
            out_line = self.fn(line)
            await output.write(out_line)
        await output.close()


class CoRoProcFnStage(BaseFnStage):
    def __init__(self, coro):
        self.coro = coro
        super().__init__()

    async def create_and_wait(self):
        input = self.sink_ports['input']
        output = self.source_ports['output']
        self.coro.send(None)
        async for line in input:
            out_bit = self.coro.send(line)
            if out_bit:
                await output.write(out_bit)
        for out_bit in self.coro:
            await output.write(out_bit)
        await output.close()


class WritableStage(PortStage):
    sources_avail = ('output',)
    source_connectors = {
        'output': NeedsFileLikeSourceConnector('output'),
    }

    @property
    def writable(self):
        return self.source_ports['output']


class ReadableStage(PortStage):
    sinks_avail = ('input',)
    sink_connectors = {
        'input': NeedsFileLikeSinkConnector('input'),
    }

    @property
    def readable(self):
        return self.sink_ports['input']


def setup_pipe_fls_of_pipeline(stage):
    unsourced = list(unsourced_iter(stage))
    unsinked = list(unsinked_iter(stage))
    if len(unsourced) != 1:
        print(unsourced)
        raise  #
    if len(unsinked) != 1:
        print(unsinked)
        raise  #

    readable_stage = ReadableStage()
    writable_stage = WritableStage()

    source_unsourced(stage, writable_stage)
    sink_unsinked(stage, readable_stage)

    return readable_stage, writable_stage


def get_fdinfo(fd):
    import fcntl
    symbols = dir(fcntl)
    for symbol in symbols:
        if symbol.startswith('F_GET'):
            if symbol in ('F_GETLK', 'F_GETLK64'):
                continue
            val = fcntl.fcntl(fd, getattr(fcntl, symbol))
            print(f"{symbol}: {val} {val:02x}")


class LineProcFnAugmentedStage(BaseFnStage):
    ForwardString = namedtuple('ForwardString', [])
    WriteString = namedtuple('WriteString', ['s'])
    DirectWriteString = namedtuple('DirectWriteString', ['s'])
    RestartCommand = namedtuple('RestartCommand', [])
    Finish = namedtuple('Finish', [])

    def __init__(self, augmentor, mk_stage):
        self.augmentor = augmentor
        self.mk_stage = mk_stage
        super().__init__()

    async def create_and_wait(self):
        # XXX: Possible problem - might deadlock if output buffer fills up
        # since it is only drained at the task's end. Should continually drain
        # buffer. (Spawn another coroutine to take care of this?)

        input = self.sink_ports['input']
        output = self.source_ports['output']
        stage = None
        self.augmentor.send(None)

        async def finish_inner_task():
            await writable.close()
            await inner_stage_task.join()
            r = await readable.read()
            await output.write(r)
        try:
            async for line in input:
                if stage is None:
                    stage = self.mk_stage()
                    readable_stage, writable_stage =\
                        setup_pipe_fls_of_pipeline(stage)
                    start_awaitable = run_pipeline_awaitables(stage)
                    run_awaitable = await start_awaitable
                    inner_stage_task = await spawn(run_awaitable)
                    readable = readable_stage.readable
                    writable = writable_stage.writable
                commands = self.augmentor.send(line)
                if commands is None:
                    break
                for command in commands:
                    if isinstance(command, self.ForwardString):
                        await writable.write(line)
                    elif isinstance(command, self.WriteString):
                        await writable.write(command.s)
                    elif isinstance(command, self.DirectWriteString):
                        await output.write(command.s)
                    elif isinstance(command, self.RestartCommand):
                        await finish_inner_task()
                        stage = None
                    elif isinstance(command, self.Finish):
                        return
                    else:
                        raise  # Unknown command
        finally:
            if stage is not None:
                await finish_inner_task()
            await output.close()


# XXX: Identitcal to SubprocessSourceConnector, SinkSuppliesFileNet
@dispatch(NeedsFileLikeSourceConnector, SinkSuppliesFileLikeNet, object)
async def connect_source(source_connector, net, source):
    await net.ensure_sinks_connected()
    source.connect_source(source_connector.name, net.afl)


# XXX: Identitcal to SubprocessSinkConnector, SourceSuppliesFileNet
@dispatch(NeedsFileLikeSinkConnector, SourceSuppliesFileLikeNet, object)
async def connect_sink(sink_connector, net, sink):
    await net.ensure_source_connected()
    sink.connect_sink(sink_connector.name, net.afl)


@dispatch(NeedsFileLikeSourceConnector, OSPipeNet, object)
async def connect_source(source_connector, net, source):
    net.ensure_connectable()
    readable, writable = net.pipe
    source.connect_source(source_connector.name, awritable_fp_to_fl(writable))


@dispatch(NeedsFileLikeSinkConnector, OSPipeNet, object)
async def connect_sink(sink_connector, net, sink):
    net.ensure_connectable()
    readable, writable = net.pipe
    sink.connect_sink(sink_connector.name, areadable_fp_to_fl(readable))
