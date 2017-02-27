import os
from collections import namedtuple

from .graph import unsinked_iter, unsourced_iter
from .run import run_pipeline_awaitables
from .stages import (STDIN_SOURCE, STDOUT_SINK, OSPipeNet, PortStage,
                     SinkConnector, SinkSuppliesFileLikeNet, SourceConnector,
                     SourceSuppliesFileLikeNet, dispatch, readable_fp_to_fl,
                     sink_unsinked, source_unsourced, writable_fp_to_fl)


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
        for line in input:
            out_line = self.fn(line)
            output.write(out_line)
        output.close()


class CoRoProcFnStage(BaseFnStage):
    def __init__(self, coro):
        self.coro = coro
        super().__init__()

    async def create_and_wait(self):
        input = self.sink_ports['input']
        output = self.source_ports['output']
        self.coro.send(None)
        for line in input:
            out_bit = self.coro.send(line)
            if out_bit:
                output.write(out_bit)
        for out_bit in self.coro:
            output.write(out_bit)
        output.close()


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
        # since it is only drained at the end.
        input = self.sink_ports['input']
        output = self.source_ports['output']
        stage = None
        self.augmentor.send(None)
        try:
            for line in input:
                if stage is None:
                    stage = self.mk_stage()
                    readable_stage, writable_stage =\
                        setup_pipe_fls_of_pipeline(stage)
                    start_awaitable, finish_awaitable =\
                        run_pipeline_awaitables(stage)
                    await start_awaitable
                    readable = readable_stage.readable
                    writable = writable_stage.writable
                commands = self.augmentor.send(line)
                if commands is None:
                    break
                for command in commands:
                    if isinstance(command, self.ForwardString):
                        writable.write(line)
                    elif isinstance(command, self.WriteString):
                        writable.write(command.s)
                    elif isinstance(command, self.DirectWriteString):
                        output.write(command.s)
                    elif isinstance(command, self.RestartCommand):
                        writable.close()
                        output.write(readable.read())
                        await finish_awaitable
                        stage = None
                    elif isinstance(command, self.Finish):
                        return
                    else:
                        raise  # Unknown command
        finally:
            if stage is not None:
                writable.close()
                await finish_awaitable


# XXX: Identitcal to SubprocessSourceConnector, SinkSuppliesFileNet
@dispatch(NeedsFileLikeSourceConnector, SinkSuppliesFileLikeNet, object)
def connect_source(source_connector, net, source):
    net.ensure_sinks_connected()
    source.connect_source(source_connector.name, net.fl)


# XXX: Identitcal to SubprocessSinkConnector, SourceSuppliesFileNet
@dispatch(NeedsFileLikeSinkConnector, SourceSuppliesFileLikeNet, object)
def connect_sink(sink_connector, net, sink):
    net.ensure_source_connected()
    sink.connect_sink(sink_connector.name, net.fl)


@dispatch(NeedsFileLikeSourceConnector, OSPipeNet, object)
def connect_source(source_connector, net, source):
    net.ensure_connectable()
    readable, writable = net.pipe
    source.connect_source(source_connector.name, writable_fp_to_fl(writable))


@dispatch(NeedsFileLikeSinkConnector, OSPipeNet, object)
def connect_sink(sink_connector, net, sink):
    net.ensure_connectable()
    readable, writable = net.pipe
    sink.connect_sink(sink_connector.name, readable_fp_to_fl(readable))
