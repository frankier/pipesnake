import atexit
import fcntl
import os
import subprocess
import sys
import tempfile
import threading
from functools import partial

from asyncio.subprocess import create_subprocess_exec, create_subprocess_shell
from multipledispatch import dispatch

from .graph import Net, Stage, sink_unsinked, source_unsourced


# Sinking utilities
def sink_unsinked_to_devnull(root):
    sink_unsinked(root, DEVNULL_SINK)


def sink_unsinked_to_stdio(root: Stage, name=None):
    sink_unsinked(root, STDOUT_SINK, only_source_name='stdout')
    sink_unsinked(root, STDERR_SINK, only_source_name='stderr')


def sink_unsinked_stderr_to_stdout(root):
    sink_unsinked(root, MERGE_SINK, only_source_name='stderr')


def source_unsourced_from_stdio(root, name=None):
    source_unsourced(root, STDIN_SOURCE, only_sink_name='stdin')


def connect_stdio(root):
    sink_unsinked_to_stdio(root)
    source_unsourced_from_stdio(root)


def connect_default(root):
    source_unsourced(root, DEFAULT_SUBPROCESS_SOURCE, only_sink_name='stdin')
    sink_unsinked(root, DEFAULT_SUBPROCESS_SINK, only_source_name='stdout')
    sink_unsinked(root, DEFAULT_SUBPROCESS_SINK, only_source_name='stderr')


# File implementations
class SourceSuppliesFileNet(Net):
    goodness = 10


class SinkSuppliesFileNet(Net):
    goodness = 10


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

    def on_source_start(self):
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
    supported_nets = (OSPipeNet, SinkSuppliesFileNet)


class SubprocessSinkConnector(SinkConnector):
    supported_nets = (OSPipeNet, SourceSuppliesFileNet)


class SubprocessArgumentSourceConnector(SourceConnector):
    supported_nets = (NamedPipeNet,)


class SubprocessArgumentSinkConnector(SinkConnector):
    supported_nets = (NamedPipeNet,)


class FileSourceConnector(SourceConnector):
    supported_nets = (SourceSuppliesFileNet,)


class FileSinkConnector(SinkConnector):
    supported_nets = (SinkSuppliesFileNet,)


# Subprocess stages implementations
class SubprocessStage(Stage):
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
        self.kwargs = {}
        self.running = False
        self.subprocess = None
        super().__init__()

    @property
    def is_running(self):
        return self.running

    def run(self):
        # XXX: Poorly named? - Doesn't actually run until awaited?
        self.running = True
        self.subprocess = self.create_fn(*self.args, **self.kwargs)

    async def create_and_wait(self):
        proc = await self.subprocess
        for net in self.sources.values():
            net.on_source_start()
        await proc.wait()

    def get_coroutine(self):
        if self.subprocess is None:
            raise  # Can't get coroutine before I've been run!
        return self.create_and_wait()

    # For use by connectors only
    def connect(self, port, payload):
        self.kwargs[port] = payload

    def __repr__(self):
        from reprlib import repr
        return '{}({}, {})'.format(
            type(self).__name__,
            ", ".join(repr(v) for v in self.args),
            ", ".join("{}={}".format(k, repr(v))
                      for k, v in self.kwargs.items()))


class ShellStage(SubprocessStage):
    create_fn = staticmethod(create_subprocess_shell)


class ExecStage(SubprocessStage):
    create_fn = staticmethod(create_subprocess_exec)


# File stages
class FileSourceStageBase(Stage):
    sources_avail = ('read',)
    source_connectors = {
        'read': FileSourceConnector('read'),
    }


class FileSinkStageBase(Stage):
    sinks_avail = ('write',)
    sink_connectors = {
        'write': FileSinkConnector('write'),
    }


class FileSourceStage(FileSourceStageBase):
    def __init__(self, fp):
        self.fp = fp
        super().__init__()


class FileSinkStage(FileSinkStageBase):
    def __init__(self, fp):
        self.fp = fp
        super().__init__()


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
@dispatch(FileSourceConnector, SourceSuppliesFileNet, object)
def connect_source(source_connector, net, source):
    net.fp = source.fp


@dispatch(SubprocessSourceConnector, SinkSuppliesFileNet, object)
def connect_source(source_connector, net, source):
    # Make sure net.fp is available
    net.ensure_sinks_connected()
    source.connect(source_connector.name, net.fp)


@dispatch(SubprocessSourceConnector, OSPipeNet, object)
def connect_source(source_connector, net, source):
    net.ensure_connectable()
    readable, writable = net.pipe
    source.connect(source_connector.name, writable)
    #os.close(writable)


@dispatch(SubprocessSinkConnector, SourceSuppliesFileNet, object)
def connect_sink(sink_connector, net, sink):
    # Make sure net.fp is available
    net.ensure_source_connected()
    sink.connect(sink_connector.name, net.fp)


@dispatch(FileSinkConnector, SinkSuppliesFileNet, object)
def connect_sink(sink_connector, net, sink):
    net.fp = sink.fp


@dispatch(SubprocessSinkConnector, OSPipeNet, object)
def connect_sink(sink_connector, net, sink):
    net.ensure_connectable()
    readable, writable = net.pipe
    sink.connect(sink_connector.name, readable)
