import argparse
import sys

from pipesnake import ExecStage, run_pipeline_block, connect_default

#import logging
#logging.basicConfig(level=logging.DEBUG)


# Bioinfomatics one liners -- from https://github.com/stephenturner/oneliners
def freq_col2(args):
    pipeline = ExecStage('cut', '-f2', args.textfile)
    pipeline.attach(ExecStage('sort'))\
            .attach(ExecStage('uniq', '-c'))\
            .attach(ExecStage('sort', '-k1nr'))\
            .attach(ExecStage('head'))
    connect_default(pipeline)
    # print_graphviz(pipeline)

    run_pipeline_block(pipeline)


def uniq_dup(args):
    pipeline = ExecStage('sort', args.file1, args.file2)
    pipeline.attach(ExecStage('uniq', '-d'))
    connect_default(pipeline)

    run_pipeline_block(pipeline)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = 'command'

    parser_freq_col2 = subparsers.add_parser('freq_col2')
    parser_freq_col2.add_argument(
        'textfile', metavar='TEXTFILE',
        help='a tab delimited text file with at least two columns')
    parser_freq_col2.set_defaults(func=freq_col2)

    parser_uniq_dup = subparsers.add_parser('uniq_dup')
    parser_uniq_dup.add_argument(
        'file1', metavar='FILE1',
        help='the first file')
    parser_uniq_dup.add_argument(
        'file2', metavar='FILE2',
        help='the second file')
    parser_uniq_dup.set_defaults(func=uniq_dup)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    sys.exit(main())
