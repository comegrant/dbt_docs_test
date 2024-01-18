import io
import json
import logging
import os
import shutil
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get("timestamp"):
            # this doesn't use record.created, so it is slightly off
            now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            log_record["timestamp"] = now
        if log_record.get("level"):
            log_record["level"] = log_record["level"].upper()
        else:
            log_record["level"] = record.levelname


def setNextRollingName(defaultName):
    date = datetime.now()
    base = os.path.dirname(defaultName)
    name = os.path.basename(defaultName)
    return os.path.join(base, str(date.year), str(date.month), name)


def moveFileInsteadOfRenaming(old, new):
    os.makedirs(os.path.dirname(new), exist_ok=True)
    shutil.copy(old, new)
    open(old, "w").close()


class MemoryLogging:
    log_stream = io.StringIO()

    def __init__(self) -> None:
        pass

    def initialize_logging(self, log_level, log_name, log_name_internal="default_name"):
        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)

        logger_internal = logging.getLogger(log_name_internal)
        logger_internal.setLevel(log_level)

        if not logger.handlers:
            ch = logging.StreamHandler(self.log_stream)
            ch.setLevel(
                os.environ.get("LOG_LEVEL", "DEBUG"),
            )
            ch.setFormatter(
                CustomJsonFormatter(
                    "%(timestamp)s %(level)s %(name)s %(lineno)d %(message)s"
                )
            )
            logger.addHandler(ch)
            logger.propagate = 0

        if not logger_internal.handlers:
            ch = TimedRotatingFileHandler(
                os.environ.get("LOG_PATH", "log.txt"),
                when=os.environ.get("LOG_INTERVAL_UNIT", "D"),
                interval=int(os.environ.get("LOG_INTERVAL", "1")),
            )
            ch.rotator = moveFileInsteadOfRenaming
            ch.namer = setNextRollingName
            ch.setLevel(
                os.environ.get("LOG_LEVEL", "DEBUG"),
            )
            ch.setFormatter(
                CustomJsonFormatter("%(timestamp)s %(level)s %(name)s %(message)s")
            )
            logger_internal.addHandler(ch)
            logger_internal.propagate = 0

        return logger, logger_internal

    def retrieve_buffer(self):
        return self.log_stream.getvalue()

    def build_log(self):
        errors = [json.loads(js) for js in self.log_stream.getvalue().splitlines()]

        return errors

    def clear_buffer(self):
        self.log_stream.truncate(0)
        self.log_stream.seek(0)


log_handler = MemoryLogging()
