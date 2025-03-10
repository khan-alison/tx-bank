from pyspark.sql import SparkSession, DataFrame
from tx_bank.common.writer.base_writer import BaseWriter
from tx_bank.common.scd_handler import SCD_Handler
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class LocalWriter(BaseWriter):
    """
    Local Writer that handles writing data to local storage in different formats (CSV, Parquet, Delta).
    It integrates with SCD processing and automatically configures writing options.
    """

    def __init__(self, spark: SparkSession, scd_handler: SCD_Handler, scd_conf: dict = None, options: dict = None):
        """
        Initializes the LocalWriter.

        :param spark: SparkSession instance
        :param scd_handler: An instance of SCD_Handler to process and write the data.
        :param scd_conf: Dictionary containing SCD configurations.
        :param options: Dictionary containing additional options.
        """
        super().__init__(spark, scd_handler, scd_conf, options)

    def write(self, df: DataFrame, data_date: str):
        """
        Calls SCD_Handler to process and write data.

        :param df: The Spark DataFrame to be written.
        :param data_date: The data date used for partitioning (if applicable).
        """
        if not self.scd_conf:
            raise ValueError("SCD Configuration is required to write data.")

        logger.info(f"Processing SCD Type: {self.scd_conf['scd']['type']}")
        self.scd_handler.process(df, self.scd_conf["scd"], data_date)

        logger.info("Data successfully written using SCD_Handler.")
