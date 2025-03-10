import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from tx_bank.common.reader.base_reader import BaseReader
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class S3Reader(BaseReader):
    """
    S3 Reader that detects environment, fetches credentials, 
    and reads data from a full S3 path dynamically.
    """

    def __init__(self, spark: SparkSession, s3_path: str, config: dict):
        """
        Initializes the S3Reader with a given S3 path.

        :param spark: SparkSession instance
        :param s3_path: Full S3 path to the dataset.
        :param config: Dictionary containing read options (format, options, etc.).
        """
        self.spark = spark
        self.s3_path = s3_path  # Now passing the full S3 path
        self.config = config

        # Extract bucket name and prefix from the full path
        self.bucket_name, self.prefix = self._parse_s3_path(s3_path)

        self.running_in_databricks = self._is_running_in_databricks()

        if self.running_in_databricks:
            self._load_dbx_credentials()
        else:
            self._load_local_credentials()

        self.format = config.get("format", "parquet")

    def _is_running_in_databricks(self):
        """Check if the script is running inside Databricks."""
        return "DATABRICKS_RUNTIME_VERSION" in os.environ

    def _load_dbx_credentials(self):
        """Load AWS credentials from Databricks Secrets."""
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
        """Load AWS credentials from .env file for local execution."""
        logger.info("Running locally. Using .env file for AWS credentials.")
        load_dotenv()
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.getenv("AWS_REGION")

    def read(self) -> DataFrame:
        """
        Reads data from S3 using the specified format.

        :return: Spark DataFrame
        """
        options = self.config.get("option", {})
        reader = self.spark.read.format(self.format)

        for key, value in options.items():
            reader = reader.option(key, value)

        logger.info(f"Reading data from {self.s3_path}")
        return reader.load(self.s3_path)

    def list_s3_objects(self):
        """
        Lists all objects in the specified S3 folder.

        :return: List of object keys
        """
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )
        response = s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=self.prefix)
        return [obj["Key"] for obj in response.get("Contents", [])] if "Contents" in response else []

    def _parse_s3_path(self, s3_path: str):
        """Parses S3 path into bucket name and key prefix."""
        if not s3_path.startswith("s3a://"):
            raise ValueError(f"Invalid S3 path: {s3_path}")

        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket_name, prefix
