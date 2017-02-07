from pipesnake.graph import stage_iter


def print_graphviz(root):
    print("digraph pipesnake {")
    for source_stage in stage_iter(root):
        print(f'n{id(source_stage)} [label="{repr(source_stage)}"];')
        for source_name, net in source_stage.sources.items():
            for sink_stage, sink_name in net.sinks:
                print(f'n{id(source_stage)} -> n{id(sink_stage)} ')
                print(f'[ label="{source_name} to {sink_name} via {type(net).__name__}" ];')
    print("}")
