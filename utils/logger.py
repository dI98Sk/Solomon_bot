import logging

def setup_logger(log_file="pipeline.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s"
    )