from pyspark.sql import SparkSession, DataFrame
from tx_bank.common.writer.local_writer import LocalWriter
from tx_bank.common.writer.s3_writer import S3Writer
from tx_bank.common.scd_handler import SCD_Handler
from tx_bank.helper.logger import LoggerSimple


logger = LoggerSimple.get_logger(__name__)


class S2I_Writer:

    def __init__(self, spark: SparkSession, metadata: dict, data_date: str):
        self.spark = spark
        self.metadata = metadata
        self.data_date = data_date
        self.scd_handler = SCD_Handler(spark)

    def write(self, df: DataFrame):
        logger.info("Writing transformed data...")

        output_config = self.metadata["outputs"]
        mode = output_config.get("mode")
        if mode == "local":
            writer = LocalWriter(self.spark, self.scd_handler, output_config)
        elif mode == "s3":
            writer = S3Writer(self.spark, self.scd_handler, output_config)

        writer.write(df, self.data_date)

        logger.info("Data successfully written.")
