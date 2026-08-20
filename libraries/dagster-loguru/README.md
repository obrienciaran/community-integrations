# dagster-loguru

Capture [Loguru](https://github.com/Delgan/loguru) log messages in Dagster's event log and UI.

Dagster natively captures standard Python logging, including the logger returned by
`dagster.get_dagster_logger()`. This package bridges Loguru into that mechanism:
`capture_loguru_logs()` adds a Loguru sink that forwards each record to that logger,
which Dagster manages during every run. No configuration, no decorators, no
monkeypatching, and existing Loguru sinks are left untouched.

## Installation

```sh
pip install dagster-loguru
```

## Usage

Call `capture_loguru_logs()` once at code location load time:

```python
from dagster import asset, Definitions
from dagster_loguru import capture_loguru_logs
from loguru import logger

capture_loguru_logs()


@asset
def my_asset():
    logger.info("This message appears in the Dagster UI.")


defs = Definitions(assets=[my_asset])
```

Loguru's `SUCCESS` and `TRACE` levels are mapped to `INFO` and `DEBUG` respectively.
Exceptions logged with `logger.exception()` are forwarded with their tracebacks. The
returned sink id can be passed to `loguru.logger.remove()` to stop forwarding.

To forward to a different standard Python logger instead, pass it explicitly and list
its name under `python_logs.managed_python_loggers` in your `dagster.yaml`:

```python
import logging

capture_loguru_logs(python_logger=logging.getLogger("my_logger"))
```

## Test

```sh
make test
```

## Build

```sh
make build
```
