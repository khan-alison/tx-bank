import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from tx_bank.common.writer.base_writer import BaseWriter
from tx_bank.common.scd_handler import SCD_Handler
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class S3Writer(BaseWriter):

    def __init__(self, spark: SparkSession, scd_handler: SCD_Handler, scd_conf: dict = None, options: dict = None):
        super().__init__(spark, scd_handler, scd_conf, options)
        self.running_in_databricks = self._is_running_in_databricks()

        if self.running_in_databricks:
            self._load_dbx_credentials()
        else:
            self._load_local_credentials()

    def _is_running_in_databricks(self):
        return "DATABRICKS_RUNTIME_VERSION" in os.environ

    def _load_dbx_credentials(self):
        try:
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(self.spark)
            logger.info(
                "Running in Databricks. Using Databricks Secrets for AWS credentials.")
            self.aws_access_key_id = self.dbutils.secrets.get(
                "aws-secrets", "AWS_ACCESS_KEY_ID")
            self.aws_secret_access_key = self.dbutils.secrets.get(
                "aws-secrets", "AWS_SECRET_ACCESS_KEY")
            self.aws_region = self.dbutils.secrets.get(
                "aws-secrets", "AWS_REGION")
        except ImportError:
            raise RuntimeError(
                "DBUtils is not available. Ensure this is running inside Databricks.")

    def _load_local_credentials(self):
        logger.info("Running locally. Using .env file for AWS credentials.")
        load_dotenv()
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.getenv("AWS_REGION")

    def write(self, df: DataFrame, data_date: str):
        if not self.scd_conf:
            raise ValueError("SCD Configuration is required to write data.")

        logger.info(f"Processing SCD Type: {self.scd_conf['scd']['type']}")
        self.scd_handler.process(df, self.scd_conf["scd"], data_date)

        logger.info(f"Data successfully written using SCD_Handler.")
