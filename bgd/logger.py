import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(config: dict) -> logging.Logger:
    log_cfg = config.get("logging", {})
    log_file = log_cfg.get("file", "logs/bgd.log")
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    max_bytes = log_cfg.get("max_size_mb", 10) * 1024 * 1024

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger("bgd")
    logger.setLevel(level)

    if not logger.handlers:
        fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S")

        fh = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=3)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger
