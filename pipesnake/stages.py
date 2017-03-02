import atexit
import fcntl
import os
import subprocess
import sys
import tempfile
import threading
from functools import partial

from curio import subprocess
from curio.io import FileStream
from multipledispatch import dispatch

from .graph import Net, Stage, sink_unsinked, source_unsourced


# Sinking utilities
def sink_unsinked_to_devnull(root):
    sink_unsinked(root, DEVNULL_SINK)


def sink_stderr(root: Stage):
    sink_unsinked(root, STDERR_SINK, only_source_name='stderr')


def sink_unsinked_to_stdio(root: Stage):
    sink_stderr(root)
    sink_unsinked(root, STDOUT_SINK, only_source_name='stdout')


def sink_unsinked_stderr_to_stdout(root):
    sink_unsinked(root, MERGE_SINK, only_source_name='stderr')


def source_unsourced_from_stdio(root):
    source_unsourced(root, STDIN_SOURCE, only_sink_name='stdin')


def connect_stdio(root):
    sink_unsinked_to_stdio(root)
    source_unsourced_from_stdio(root)


def connect_default(root):
    source_unsourced(root, DEFAULT_SUBPROCESS_SOURCE, only_sink_name='stdin')
    sink_unsinked(root, DEFAULT_SUBPROCESS_SINK, only_source_name='stdout')
    sink_unsinked(root, DEFAULT_SUBPROCESS_SINK, only_source_name='stderr')


# File implementations
class SourceSuppliesFileIntNet(Net):
    goodness = 10


class SinkSuppliesFileIntNet(Net):
    goodness = 10


class SourceSuppliesFileLikeNet(Net):
    goodness = 5


class SinkSuppliesFileLikeNet(Net):
    goodness = 5


# Pipe implementations
F_SETPIPE_SZ = 1032
F_GETPIPE_SZ = 1032


class OSPipeNet(Net):
    goodness = 30

    def __init__(self):
        super().__init__()
        self.pipe = None

    def ensure_connectable(self):
        if self.pipe is None:
            self.pipe = os.pipe()

    def get_bufsize(self):
        self.ensure_pipe()
        readable, writable = self.pipe
        return fcntl.fcntl(writable, F_GETPIPE_SZ)

    def set_bufsize(self, value):
        self.ensure_pipe()
        readable, writable = self.pipe
        return fcntl.fcntl(writable, F_SETPIPE_SZ, value)

    def close_writable(self):
        readable, writable = self.pipe
        os.close(writable)


def _get_tempdir():
    threadlocals = threading.local()
    if not hasattr(threadlocals, '_pipesnake_tmpdir'):
        threadlocals._pipesnake_tmpdir = tempfile.TemporaryDirectory(
            prefix='__pipesnake__')
    return threadlocals._pipesnake_tmpdir.name


@atexit.register
def _rm_tempdir():
    threadlocals = threading.local()
    if hasattr(threadlocals, '_pipesnake_tmpdir'):
        threadlocals._pipesnake_tmpdir.cleanup()
        del threadlocals._pipesnake_tmpdir


class NamedPipeNet(OSPipeNet):
    def _ensure_pipe(self):
        if self.pipe is None:
            self.pipe_name = os.mkfifo(os.path.join(_get_tempdir(), id(self)))
            self.pipe = (
                os.open(self.pipe_name, os.O_RDONLY),
                os.open(self.pipe_name, os.O_WRONLY))

    def cleanup(self):
        os.unlink(self.pipe_name)

    def __del__(self):
        # Last ditch attempt at cleanup - should be cleaned up by...
        # (something else)
        self.cleanup()


# Debugging
def insert_pvs(root):
    root


# Connectors
class Connector(object):
    def __init__(self, name):
        self.name = name
        self.is_connected = False

    def mark_connected(self):
        self.is_connected = True


class SourceConnector(Connector):
    """
    Connects a source to a net.
    """
    pass


class SinkConnector(Connector):
    """
    Connects a net to a sink.
    """
    pass


class SubprocessSourceConnector(SourceConnector):
    supported_nets = (OSPipeNet, SinkSuppliesFileIntNet)


class SubprocessSinkConnector(SinkConnector):
    supported_nets = (OSPipeNet, SourceSuppliesFileIntNet)


class SubprocessArgumentSourceConnector(SourceConnector):
    supported_nets = (NamedPipeNet,)


class SubprocessArgumentSinkConnector(SinkConnector):
    supported_nets = (NamedPipeNet,)


class FileSourceConnector(SourceConnector):
    supported_nets = (SourceSuppliesFileIntNet, SourceSuppliesFileLikeNet)


class FileSinkConnector(SinkConnector):
    supported_nets = (SinkSuppliesFileIntNet, SinkSuppliesFileLikeNet)


class PortStage(Stage):
    # XXX: This should all live in connector, yeah?
    def __init__(self):
        self.source_ports = {}
        self.sink_ports = {}
        super().__init__()

    @property
    def all_ports(self):
        return {**self.source_ports, **self.sink_ports}

    def connect_source(self, port, payload):
        self.source_ports[port] = payload

    def connect_sink(self, port, payload):
        self.sink_ports[port] = payload


# Subprocess stages implementations
class SubprocessStage(PortStage):
    sinks_avail = ('stdin',)
    sources_avail = ('stdout', 'stderr')
    _default_source = 'stdout'
    sink_connectors = {
        'stdin': SubprocessSinkConnector('stdin'),
    }
    source_connectors = {
        'stdout': SubprocessSourceConnector('stdout'),
        'stderr': SubprocessSourceConnector('stderr'),
    }

    def __init__(self, *args):
        self.args = args
        self.running = False
        self.proc = None
        super().__init__()

    @property
    def is_running(self):
        return self.running

    async def run(self):
        self.running = True

        self.proc = await self.create_fn(*self.args, **self.all_ports)
        for source_name, source_net in self.sources.items():
            if hasattr(source_net, 'close_writable'):
                source_net.close_writable()

    async def create_and_wait(self):
        if self.proc is None:
            raise  # Can't get coroutine before I've been run!
        await self.proc.wait()
        #for source_name, source_port in self.source_ports.items():
            #source_port.close()

    def get_coroutine(self):
        return self.create_and_wait()

    def __repr__(self):
        from reprlib import repr
        return '{}({}{}{})'.format(
            type(self).__name__,
            ", ".join(repr(v) for v in self.args),
            ", " if self.args and self.all_ports else "",
            ", ".join("{}={}".format(k, repr(v))
                      for k, v in self.all_ports.items()))


class ShellStage(SubprocessStage):
    @staticmethod
    async def create_fn(*args, **kwargs):
        return subprocess.Popen(*args, shell=True, **kwargs)

    def short_repr(self):
        return self.args[0]


class ExecStage(SubprocessStage):
    @staticmethod
    async def create_fn(*args, **kwargs):
        return subprocess.Popen(args, **kwargs)

    def short_repr(self):
        return ' '.join(self.args)


# File stages
class FileSourceStageBase(Stage):
    sources_avail = ('read',)

    def short_repr(self):
        return str(self.fp)


class FileSinkStageBase(Stage):
    sinks_avail = ('write',)

    def short_repr(self):
        # XXX: Not dry - should use file name/path when available
        return str(self.fp)


def readable_fp_to_fl(fp):
    return open(fp, 'r', closefd=False)


def writable_fp_to_fl(fp):
    return open(fp, 'w')
    #, closefd=False)


def areadable_fp_to_fl(fp):
    return FileStream(open(fp, 'br', buffering=0, closefd=False))


def awritable_fp_to_fl(fp):
    return FileStream(open(fp, 'bw', buffering=0))


class FileSourceStage(FileSourceStageBase):
    source_connectors = {
        'read': FileSourceConnector('read'),
    }

    def __init__(self, fp):
        self.fp = fp
        super().__init__()

    @property
    def fl(self):
        return readable_fp_to_fl(self.fp)

    @property
    def afl(self):
        return areadable_fp_to_fl(self.fp)


class FileSinkStage(FileSinkStageBase):
    sink_connectors = {
        'write': FileSinkConnector('write'),
    }

    def __init__(self, fp):
        self.fp = fp
        super().__init__()

    @property
    def fl(self):
        return writable_fp_to_fl(self.fp)

    @property
    def afl(self):
        return awritable_fp_to_fl(self.fp)


# Special stages
class DevNullSinkStage(FileSinkStageBase):
    fp = subprocess.DEVNULL


class MergeStderrSinkStage(FileSinkStageBase):
    fp = subprocess.STDOUT


# StringIO sink
class StringIOSink(FileSinkStageBase):
    def __init__(self):
        pass


class CallbackSink(FileSinkStageBase):
    def __init__(self, callback):
        self.callback = callback


# Singleton stage instances
DEVNULL_SINK = DevNullSinkStage()
STDIN_SOURCE = FileSourceStage(sys.stdin.fileno())
STDOUT_SINK = FileSinkStage(sys.stdout.fileno())
STDERR_SINK = FileSinkStage(sys.stderr.fileno())
MERGE_SINK = MergeStderrSinkStage()

DEFAULT_SUBPROCESS_SOURCE = FileSourceStage(None)
DEFAULT_SUBPROCESS_SINK = FileSinkStage(None)

dispatch_namespace = {}
dispatch = partial(dispatch, namespace=dispatch_namespace)


# Connect multiple dispatch functions
@dispatch(FileSourceConnector, SourceSuppliesFileIntNet, object)
async def connect_source(source_connector, net, source):
    net.fp = source.fp


@dispatch(FileSourceConnector, SourceSuppliesFileLikeNet, object)
async def connect_source(source_connector, net, source):
    net.afl = source.afl


@dispatch(SubprocessSourceConnector, SinkSuppliesFileIntNet, object)
async def connect_source(source_connector, net, source):
    # Make sure net.fp is available
    await net.ensure_sinks_connected()
    source.connect_source(source_connector.name, net.fp)


@dispatch(SubprocessSourceConnector, OSPipeNet, object)
async def connect_source(source_connector, net, source):
    net.ensure_connectable()
    readable, writable = net.pipe
    source.connect_source(source_connector.name, writable)
    #source.close_writable()
    #net.close_writable()


@dispatch(SubprocessSinkConnector, SourceSuppliesFileIntNet, object)
async def connect_sink(sink_connector, net, sink):
    # Make sure net.fp is available
    await net.ensure_source_connected()
    sink.connect_sink(sink_connector.name, net.fp)


@dispatch(SubprocessSinkConnector, SourceSuppliesFileIntNet, object)
async def connect_sink(sink_connector, net, sink):
    # Make sure net.fp is available
    await net.ensure_source_connected()
    sink.connect_sink(sink_connector.name, net.afl)


@dispatch(FileSinkConnector, SinkSuppliesFileIntNet, object)
async def connect_sink(sink_connector, net, sink):
    net.fp = sink.fp


@dispatch(FileSinkConnector, SinkSuppliesFileLikeNet, object)
async def connect_sink(sink_connector, net, sink):
    net.afl = sink.afl


@dispatch(SubprocessSinkConnector, OSPipeNet, object)
async def connect_sink(sink_connector, net, sink):
    net.ensure_connectable()
    readable, writable = net.pipe
    sink.connect_sink(sink_connector.name, readable)
