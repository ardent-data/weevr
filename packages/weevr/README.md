# weevr

Configuration-driven data shaping for Spark in Microsoft Fabric.

This is the engine wheel — see the [project README](https://github.com/ardent-data/weevr#readme) and the
[documentation](https://ardent-data.github.io/weevr/latest/) for the full picture.

The pure-Python validation, planner, and telemetry-contract surface lives in
the companion [`weevr-core`](https://pypi.org/project/weevr-core/) wheel; `weevr` declares it as a
runtime dependency, so `pip install weevr` continues to install the full
stack unchanged.

weevr 1.x requires Fabric Runtime 1.3 and doesn't install on Runtime 2.0. See
[Fabric Runtime](https://ardent-data.github.io/weevr/latest/guides/fabric-runtime/#runtime-compatibility).
