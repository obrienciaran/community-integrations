import logging

import dagster as dg
import pytest
from loguru import logger

from dagster_loguru import capture_loguru_logs


@pytest.fixture
def loguru_capture():
    sink_id = capture_loguru_logs()
    try:
        yield
    finally:
        logger.remove(sink_id)


def get_log_records(assets, instance_overrides=None):
    with dg.instance_for_test(overrides=instance_overrides) as instance:
        result = dg.materialize(assets, instance=instance)
        assert result.success
        event_records = instance.event_log_storage.get_logs_for_run(result.run_id)
    return [er for er in event_records if er.user_message]


def test_loguru_logs_captured_in_event_log_without_config(loguru_capture):
    @dg.asset
    def my_asset():
        logger.info("hello from loguru")

    records = [
        lr
        for lr in get_log_records([my_asset])
        if lr.user_message == "hello from loguru"
    ]
    assert len(records) == 1
    assert records[0].step_key == "my_asset"
    assert records[0].level == logging.INFO


def test_loguru_only_levels_mapped(loguru_capture):
    @dg.asset
    def my_asset():
        logger.success("all done")
        logger.trace("fine detail")

    records = {
        lr.user_message: lr.level
        for lr in get_log_records([my_asset])
        if lr.user_message in ("all done", "fine detail")
    }
    assert records["all done"] == logging.INFO
    assert records["fine detail"] == logging.DEBUG


def test_loguru_exception_captured(loguru_capture):
    @dg.asset
    def my_asset():
        try:
            raise ValueError("boom")
        except ValueError:
            logger.exception("something failed")

    records = [
        lr
        for lr in get_log_records([my_asset])
        if lr.user_message == "something failed"
    ]
    assert len(records) == 1
    assert records[0].level == logging.ERROR


def test_custom_python_logger_with_managed_loggers():
    custom_logger = logging.getLogger("my_loguru_bridge")
    custom_logger.setLevel(logging.DEBUG)
    sink_id = capture_loguru_logs(python_logger=custom_logger)

    @dg.asset
    def my_asset():
        logger.warning("custom logger name")

    try:
        records = [
            lr
            for lr in get_log_records(
                [my_asset],
                {"python_logs": {"managed_python_loggers": ["my_loguru_bridge"]}},
            )
            if lr.user_message == "custom logger name"
        ]
    finally:
        logger.remove(sink_id)
    assert len(records) == 1
    assert records[0].level == logging.WARNING


def test_unmanaged_custom_logger_not_captured():
    custom_logger = logging.getLogger("unmanaged_bridge")
    custom_logger.setLevel(logging.DEBUG)
    sink_id = capture_loguru_logs(python_logger=custom_logger)

    @dg.asset
    def my_asset():
        logger.info("should not be captured")

    try:
        records = [
            lr
            for lr in get_log_records([my_asset])
            if lr.user_message == "should not be captured"
        ]
    finally:
        logger.remove(sink_id)
    assert len(records) == 0


def test_sink_removal_stops_forwarding():
    sink_id = capture_loguru_logs()
    logger.remove(sink_id)

    @dg.asset
    def my_asset():
        logger.info("should not be captured")

    records = [
        lr
        for lr in get_log_records([my_asset])
        if lr.user_message == "should not be captured"
    ]
    assert len(records) == 0
