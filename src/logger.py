import logging

def setup_logger():
    logger = logging.getLogger("WarehouseWatcher")
    logger.setLevel(logging.DEBUG)

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler("warehouse_watcher.log")

    # Set levels
    console_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.INFO)

    # Create formatters and add to handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

logger = setup_logger()
