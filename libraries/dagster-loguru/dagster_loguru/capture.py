import logging
from typing import TYPE_CHECKING

from dagster import get_dagster_logger
from loguru import logger

if TYPE_CHECKING:
    import loguru

# Loguru levels with no standard library equivalent.
_LOGURU_ONLY_LEVELS = {"TRACE": logging.DEBUG, "SUCCESS": logging.INFO}


def _to_python_level(level: "loguru.RecordLevel") -> int:
    if level.name in _LOGURU_ONLY_LEVELS:
        return _LOGURU_ONLY_LEVELS[level.name]
    python_level = logging.getLevelName(level.name)
    # logging.getLevelName returns a string for unrecognized level names, in
    # which case we fall back to the numeric severity of the Loguru level.
    return python_level if isinstance(python_level, int) else level.no


def capture_loguru_logs(
    python_logger: logging.Logger | None = None, level: str | int = 0
) -> int:
    """Forward Loguru log messages to Dagster's event log and UI.

    This adds a Loguru sink that re-emits each Loguru record through a standard Python
    logger that Dagster captures. By default this is the logger returned by
    ``dagster.get_dagster_logger()``, which Dagster always manages during a run, so no
    configuration is required. Call this function once at code location load time
    (e.g. in the module defining your ``Definitions``):

    .. code-block:: python

        from dagster import asset, Definitions
        from dagster_loguru import capture_loguru_logs
        from loguru import logger

        capture_loguru_logs()


        @asset
        def my_asset():
            logger.info("This message appears in the Dagster UI.")


        defs = Definitions(assets=[my_asset])

    Loguru's ``SUCCESS`` and ``TRACE`` levels are mapped to ``INFO`` and ``DEBUG``
    respectively, and exceptions are forwarded with their tracebacks. Existing Loguru
    sinks (such as its default stderr sink) are left untouched.

    Args:
        python_logger (Optional[logging.Logger]): The standard library logger to
            forward records to. Defaults to ``get_dagster_logger("loguru")``. If you
            pass a logger that Dagster does not manage by default, list its name under
            ``python_logs.managed_python_loggers`` in your ``dagster.yaml``.
        level (Union[str, int]): Minimum Loguru level to forward. Defaults to 0
            (forward everything).

    Returns:
        int: The Loguru sink id, which can be passed to ``loguru.logger.remove()`` to
        stop forwarding.
    """
    target = (
        python_logger if python_logger is not None else get_dagster_logger("loguru")
    )

    def _forward(message: "loguru.Message") -> None:
        record = message.record
        exception = record["exception"]
        target.log(
            _to_python_level(record["level"]),
            record["message"],
            exc_info=(exception.type, exception.value, exception.traceback)
            if exception
            else None,
        )

    return logger.add(_forward, level=level, format="{message}")
