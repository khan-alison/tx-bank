from pyspark.sql import SparkSession, DataFrame
from tx_bank.common.writer.local_writer import LocalWriter
from tx_bank.common.writer.s3_writer import S3Writer
from tx_bank.common.scd_handler import SCD_Handler
from tx_bank.helper.logger import LoggerSimple


logger = LoggerSimple.get_logger(__name__)


class S2I_Writer:
    """
    Generic Writer for all ETL jobs.
    Uses metadata configuration to determine output format and storage type.
    """

    def __init__(self, spark: SparkSession, metadata: dict, data_date: str):
        """
        Initializes the writer.

        :param spark: SparkSession instance.
        :param metadata: Dictionary containing metadata configuration.
        """
        self.spark = spark
        self.metadata = metadata
        self.data_date = data_date
        self.scd_handler = SCD_Handler(spark)

    def write(self, df: DataFrame):
        """
        Writes the transformed data using LocalWriter.

        :param df: The Spark DataFrame to be written.
        :param data_date: The data date used for partitioning.
        """
        logger.info("Writing transformed data...")

        output_config = self.metadata["outputs"]
        mode = output_config.get("mode")
        if mode == "local":
            writer = LocalWriter(self.spark, self.scd_handler, output_config)
        elif mode == "s3":
            writer = S3Writer(self.spark, self.scd_handler, output_config)

        # df_with_data_date = df.withColumn("data_date", F.lit(self.data_date))

        writer.write(df, self.data_date)

        logger.info("Data successfully written.")
