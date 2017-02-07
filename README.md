# pipesnake: Less painful Unix style pipes for Python

This is still a heavy work in progress. This isn't really ready for public
consumption and may be partially broken. You probably shouldn't use this. That
said, any feedback is still welcome.

I've found using pipes . In some fields, particularly Natural Language
Processing and Bioinformatics, Unix style pipelines are still a major
workhorse. But using them from Python isn't as fantastic and smooth as it ought
to be. In fact it's an area where Python could be said to be worse than Bash.

## Design plan

Main layers:

 * Lowest layer - completely abstract dataflow representation

 * Middle layer - a strict/explicit layer

 * Upper layer - convenient layer

Features:

 * Can interleave Python and external processes

Extra stuff:

 * Instrumentation of pipelines

 * Visualisation of pipelines

## Comparison

Most comparable with: http://sarge.readthedocs.io/en/latest/

Pipelines made easy!:
https://github.com/kennethreitz/envoy
https://github.com/kennethreitz/delegator.py

Pipeline builder:
https://docs.python.org/3/library/pipes.html

Dataflow processing tools: Dask...

Other: pexpect
