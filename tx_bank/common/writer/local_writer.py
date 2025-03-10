from pyspark.sql import SparkSession, DataFrame
from tx_bank.common.writer.base_writer import BaseWriter
from tx_bank.common.scd_handler import SCD_Handler
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class LocalWriter(BaseWriter):

    def __init__(self, spark: SparkSession, scd_handler: SCD_Handler, scd_conf: dict = None, options: dict = None):
        super().__init__(spark, scd_handler, scd_conf, options)

    def write(self, df: DataFrame, data_date: str):
        if not self.scd_conf:
            raise ValueError("SCD Configuration is required to write data.")

        logger.info(f"Processing SCD Type: {self.scd_conf['scd']['type']}")
        self.scd_handler.process(df, self.scd_conf["scd"], data_date)

        logger.info("Data successfully written using SCD_Handler.")
