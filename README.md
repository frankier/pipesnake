# pipesnake: Less painful Unix style pipes for Python

This is still a heavy work in progress. This isn't really ready for public
consumption and may be partially broken. You probably shouldn't use this. That
said, any feedback is still welcome.

In some fields, particularly Natural Language Processing and Bioinformatics,
Unix style pipelines are still a major workhorse. But using them from Python
isn't as fantastic and smooth as it ought to be. In fact, I've found using
pipes from Python both unnecessarily painful and insufficiently powerful. It's
an area where Python could be said to be worse than Bash.

![The pipesnake module's logo: a photo of a common pipe snake](logo.jpg)

[(Freely licensed photo of a common pipe snake (*Cylindrophis ruffus*) and
adoptive logo obtained from Wikimedia Commons. Thanks Wibowo
Djatmiko!)](https://commons.wikimedia.org/wiki/File:Cyl_ruffus_061212_2025_tdp.jpg)

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
