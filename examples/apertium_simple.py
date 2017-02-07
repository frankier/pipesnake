import os
import sys
from xml.etree import ElementTree

from pipesnake import ExecStage, connect_default, run_pipeline_block


def parse_mode(mode_xml, mode_name):
    tree = ElementTree.parse(mode_xml)
    modes = tree.getroot()
    assert modes.tag == "modes"
    for mode in modes:
        assert mode.tag == "mode"
        if mode.attrib['name'] == mode_name:
            break
    else:
        pass  # Mode not found
    assert len(mode) == 1
    pipeline = mode[0]
    assert pipeline.tag == "pipeline"
    for program in pipeline:
        assert program.tag == "program"
        name = program.attrib['name']
        name = name.replace('$1', '-g')  # lt-proc should perform generation
        name = name.replace('$2', '')  # Nothing extra for apertium-tagger
        cmd = name.split()
        for file_tag in program:
            cmd.append(file_tag.attrib['name'])
        yield cmd


def build_pipeline(programs):
    root_stage = None
    prev_stage = None
    for program in programs:
        stage = ExecStage(*program)
        if root_stage is None:
            root_stage = stage
        if prev_stage:
            prev_stage.attach(stage)
        prev_stage = stage
    return root_stage


def main():
    if len(sys.argv) != 3:
        print("Usage: {} <modes.xml> <mode name>".format(sys.argv[0]))
        return -1
    name, mode_xml, mode_name = sys.argv
    os.chdir(os.path.dirname(mode_xml))
    pipeline = build_pipeline(parse_mode(mode_xml, mode_name))
    connect_default(pipeline)
    run_pipeline_block(pipeline)


if __name__ == '__main__':
    sys.exit(main())
