from .exceptions import (
    NetException, RootRequiredException, NoDefaultSourceException,
    NoDefaultSinkException)
from collections import deque
from operator import attrgetter
import collections
import itertools

# This file contains abstract dataflow graph stuff in isolation from anything
# to do with concrete sources and sinks or the actual string payloads that
# travel between them.


# Lowest layer: nets, sources/sinks and stages
class Net(object):
    goodness = 0

    def __init__(self):
        self.source = None
        self.sinks = []

    # Nets are responsible for updating pointers. Everything else is a
    # convenience method which proxies here.
    def add_source(self, source_stage, source_name=None):
        if self.source is not None:
            raise NetException("A net may not have more than one source.")
        if source_name is None:
            source_name = source_stage.default_source
        self.source = (source_stage, source_name)
        source_stage.sources[source_name] = self

    def rm_source(self):
        source_stage, source_name = self.source
        del source_stage[source_name]
        self.source = None

    def add_sink(self, sink_stage, sink_name=None):
        if sink_name is None:
            sink_name = sink_stage.default_sink
        self.sinks.append((sink_stage, sink_name))
        sink_stage.sinks[sink_name] = self

    def rm_sink(self, sink_name):
        sink_stage = self.sinks[sink_name]
        del self.sinks[sink_name]
        sink_stage.sinks.remove((sink_stage, sink_name))

    def detach(self):
        self.rm_source()
        for sink_name in self.sinks:
            self.rm_sink(sink_name)

    def ensure_source_connected(self):
        from pipesnake.stages import connect_source
        self.ensure_connectable()
        if self.source is None:
            raise NetException("Can't ensure connected on net with no source.")
        source_stage, source_name = self.source
        connector = source_stage.source_connectors[source_name]
        if connector.is_connected:
            return
        connect_source(connector, self, source_stage)

    def ensure_sinks_connected(self):
        from pipesnake.stages import connect_sink
        # XXX: Should throw exception here too maybe
        self.ensure_connectable()
        for sink_stage, sink_name in self.sinks:
            connector = sink_stage.sink_connectors[sink_name]
            if connector.is_connected:
                continue
            connect_sink(connector, self, sink_stage)

    def ensure_connectable(self):
        pass

    def on_source_start(self):
        # Should this actually use an observer pattern?
        pass


class Stage(object):
    sinks_avail = ()
    sources_avail = ()
    source_connectors = {}
    sink_connectors = {}

    def __init__(self):
        self.sinks = {}
        self.sources = {}
        self.next = None

    def iter_next(self):
        yield next

    @property
    def default_source(self):
        if len(self.sources_avail) == 1:
            return self.sources_avail[0]
        elif hasattr(self, '_default_source'):
            return self._default_source
        else:
            raise NoDefaultSourceException()

    @property
    def default_sink(self):
        #print(self, self.sinks_avail, getattr(self, '_default_sink', None))
        if len(self.sinks_avail) == 1:
            return self.sinks_avail[0]
        elif hasattr(self, '_default_sink'):
            return self._default_sink
        else:
            raise NoDefaultSinkException()

    def ensure_sources_connected(self):
        from pipesnake.stages import connect_source
        for source_name, source_connector in self.source_connectors.items():
            if source_connector.is_connected:
                continue
            self.sources[source_name].ensure_connectable()
            connect_source(source_connector, self.sources[source_name], self)

    def ensure_sinks_connected(self):
        from pipesnake.stages import connect_sink
        for sink_name, sink_connector in self.sink_connectors.items():
            if sink_connector.is_connected:
                continue
            self.sinks[sink_name].ensure_connectable()
            connect_sink(sink_connector, self.sinks[sink_name], self)

    # High level/chaining api XXX: Could remove or move to special DSL module
    def attach(self, sink_stage, source_name=None, sink_name=None,
               net_cls=None, net=None):
        attach(self, sink_stage, source_name, sink_name, net_cls, net)
        return sink_stage

    async def run(self):
        return

    async def empty_coroutine(self):
        pass

    def get_coroutine(self):
        # XXX: Should there even be a default implementation? Strictly
        # FileStages should resolve their coroutine when they close.
        return self.empty_coroutine()


def attach(source, sink, source_name=None, sink_name=None,
           net_cls=None, net=None):
    if source_name is None:
        source_name = source.default_source
    if sink_name is None:
        sink_name = sink.default_sink
    if net is not None and net is not None:
        raise  # Can't specify both
    if net is None:
        if net_cls is None:
            # Get the preferred net class for these two types of connectors
            source_connector = source.source_connectors[source_name]
            sink_connector = sink.sink_connectors[sink_name]
            available_nets = []
            for net in source_connector.supported_nets:
                if net in sink_connector.supported_nets:
                    available_nets.append(net)
            if not available_nets:
                raise  # Sorry -- can't find a net type to use
            net_cls = sorted(available_nets, key=attrgetter('goodness'))[-1]
        net = net_cls()
    net.add_source(source, source_name)
    net.add_sink(sink, sink_name)
    return net


def is_connected(root_stage):
    for stage in stage_iter(root_stage):
        for sink_name in stage.sinks_avail:
            if sink_name not in stage.sinks:
                return False
        for source_name in stage.sources_avail:
            if source_name not in stage.sources:
                return False
    return True


def check_root(root):
    if root.sinks:
        raise RootRequiredException(
            "This operation takes the root of a pipeline.")


def get_next_stages(stage):
    for net in stage.sources.values():
        for next_stage, sink_name in net.sinks:
            yield next_stage


def get_prev_stages(stage):
    for net in stage.sinks.values():
        prev_stage, source_name = net.source
        yield prev_stage


def get_connected_stages(stage):
    return itertools.chain(get_next_stages(stage), get_prev_stages(stage))


def _stage_iter(next_fn):
    def inner(roots):
        if not isinstance(roots, collections.Iterable):
            for res in inner([roots]):
                yield res
            return
        queue = deque(roots)
        seen = set(roots)
        while len(queue):
            stage = queue.popleft()
            yield stage
            for next_stage in next_fn(stage):
                if next_stage not in seen:
                    queue.append(next_stage)
                    seen.add(next_stage)
    return inner


stage_iter = _stage_iter(get_next_stages)
stage_iter_backwards = _stage_iter(get_prev_stages)
stage_iter_all = _stage_iter(get_connected_stages)


def unsinked_iter(root):
    for node in stage_iter_all(root):
        for source in node.sources_avail:
            if source not in node.sources:
                yield (node, source)


def unsourced_iter(root):
    for node in stage_iter_all(root):
        for sink in node.sinks_avail:
            if sink not in node.sinks:
                yield (node, sink)


def sink_unsinked(root: Stage, sink_node: Stage,
                  sink_name=None, only_source_name=None):
    if sink_name is None:
        sink_name = sink_node.default_sink
    for source_node, source_name in unsinked_iter(root):
        if only_source_name is not None and source_name != only_source_name:
            continue
        attach(source_node, sink_node, source_name, sink_name)


def source_unsourced(root: Stage, source_node: Stage,
                     source_name=None, only_sink_name=None):
    if source_name is None:
        source_name = source_node.default_source
    for sink_node, sink_name in unsourced_iter(root):
        if only_sink_name is not None and sink_name != only_sink_name:
            continue
        attach(source_node, sink_node, source_name, sink_name)


def has_unsinked(root):
    return len(unsinked_iter(root)) > 0


def get_roots(stage):
    result = set()
    for stage in stage_iter_all(stage):
        if len(stage.sinks) == 0:
            result.add(stage)
    return result
