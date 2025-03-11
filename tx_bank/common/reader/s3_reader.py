import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from tx_bank.common.reader.base_reader import BaseReader
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class S3Reader(BaseReader):
    """
    Reads all files inside a folder from S3 dynamically.
    """

    def __init__(self, spark: SparkSession, s3_path: str, config: dict):
        """
        :param spark: SparkSession instance
        :param s3_path: S3 folder path (e.g., "s3a://bucket-name/folder/")
        :param config: Dictionary containing read options (format, options, etc.)
        """
        self.spark = spark
        self.s3_path = s3_path.rstrip("/")  # Ensure no trailing slash
        self.config = config

        self.bucket_name, self.prefix = self._parse_s3_path(self.s3_path)

        self.running_in_databricks = self._is_running_in_databricks()

        if self.running_in_databricks:
            self._load_dbx_credentials()
        else:
            self._load_local_credentials()

        self.format = config.get("format", "parquet")

    def _is_running_in_databricks(self):
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

        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise RuntimeError(
                "AWS credentials are missing! Check your .env file.")

    def list_s3_objects(self):
        """
        Lists all files inside the S3 folder.
        :return: List of full S3 file paths
        """
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )

        response = s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=self.prefix)
        files = [
            f"s3a://{self.bucket_name}/{obj['Key']}"
            for obj in response.get("Contents", []) if not obj["Key"].endswith("/")
        ]

        if not files:
            raise FileNotFoundError(f"No files found in {self.s3_path}")

        return files

    def read(self) -> DataFrame:
        """
        Reads all files from the specified S3 folder.
        :return: Spark DataFrame
        """
        options = self.config.get("option", {})
        reader = self.spark.read.format(self.format)

        for key, value in options.items():
            reader = reader.option(key, value)

        file_paths = self.list_s3_objects()

        logger.info(file_paths)

        logger.info(
            f"Reading data from {len(file_paths)} files inside {self.s3_path}")

        return reader.load(file_paths)

    def _parse_s3_path(self, s3_path: str):
        if not (s3_path.startswith("s3a://") or s3_path.startswith("s3://")):
            raise ValueError(f"Invalid S3 path: {s3_path}")
        
        normalized_path = s3_path.replace("s3://", "s3a://") if s3_path.startswith("s3://") else s3_path
        
        parts = normalized_path.replace("s3a://", "").split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        
        if not prefix.endswith("/"):
            prefix += "/"
            
        return bucket_name, prefix
