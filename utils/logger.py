import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class PipelineLogger:

    _configured = False

    @staticmethod
    def configure_logger(
        log_level=logging.INFO
    ):

        if PipelineLogger._configured:
            return

        # project root
        project_root = Path(__file__).resolve().parent

        # logs directory
        log_dir = project_root / "logs"

        log_dir.mkdir(parents=True, exist_ok=True)

        # log file
        log_file = log_dir / "pipeline.log"

        formatter = logging.Formatter(
            fmt=(
                "%(asctime)s | "
                "%(levelname)s | "
                "%(name)s | "
                "%(filename)s:%(lineno)d | "
                "%(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # rotating file handler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5
        )

        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()

        # avoid duplicate handlers
        if root_logger.handlers:
            PipelineLogger._configured = True
            return

        root_logger.setLevel(log_level)

        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

        PipelineLogger._configured = True

    @staticmethod
    def get_logger(name):

        PipelineLogger.configure_logger()

        return logging.getLogger(name)
