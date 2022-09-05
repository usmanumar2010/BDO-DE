import logging
import os
import sys
from typing import Tuple

from pyspark.sql import SparkSession


def setup_spark(app_name: str) -> Tuple[str, str, SparkSession]:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s	%(message)s "
        "[%(process)d] %(module)s %(filename)s %(funcName)s",
        level=os.environ.get("LOGLEVEL", "INFO"),
    )
    logging.info(sys.argv)

    if len(sys.argv) < 2:
        logging.warning("Input .txt file and output path are required")
        sys.exit(1)

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark_context = spark.sparkContext
    app_name = spark_context.appName
    logging.info("Application Initialized: %s", app_name)
    return sys.argv[1], sys.argv[2], spark
