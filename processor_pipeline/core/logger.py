from loguru import logger

# Configure loguru for better formatting and control
def _ensure_extra_fields(record):
    # Always provide a default session_id for formatting
    record["extra"].setdefault("session_id", "-")
    # Optional suffix to distinguish different inputs within the same session
    record["extra"].setdefault("sid_suffix", "")
    # Default object_name for formatting
    record["extra"].setdefault("object_name", "UNKNOWN")
    return True


logger.remove()  # Remove default handler
logger.add(
    "logs/processor_pipeline.log",
    rotation="10 MB",
    retention="7 days",
    level="INFO",
    # Include session_id in all log lines
    format="<green>{time:YYYY-MM-DD HH:mm:ss:SSS}</green> | sid={extra[session_id]}{extra[sid_suffix]} | <level>{level: <8}</level> | <cyan>{extra[object_name]}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    backtrace=True,
    diagnose=True,
    filter=_ensure_extra_fields,
)
logger.add(
    lambda msg: print(msg, end=""),
    level="DEBUG",
    format="<green>{time:HH:mm:ss:SSS}</green> | sid={extra[session_id]}{extra[sid_suffix]} | <level>{level: <8}</level> | <cyan>{extra[object_name]}</cyan> - <level>{message}</level>",
    colorize=True,
    filter=_ensure_extra_fields,
)
