from pyspark.sql import SparkSession
from tx_bank.common.spark_session_manager.base_spark_session_manager import (
    BaseSparkSessionManager
)
import os
from dotenv import load_dotenv
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class LocalSparkSessionManager(BaseSparkSessionManager):

    def __init__(self, appName: str):
        super().__init__(appName, "local")

    def _create_spark_session(self) -> SparkSession:
        load_dotenv()
        builder = (
            SparkSession.builder.appName(self.appName)
            .master("local")
            .config("spark.sql.shuffle.partitions", 5)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.jars",
                "jars/delta-spark_2.12-3.2.0.jar, jars/delta-storage-3.2.0.jar,jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.316.jar, jar/hadoop-common-3.3.4.jar",
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )
        logger.info("Creating local Spark session with local jar files.")
        return builder.getOrCreate()

    @staticmethod
    def get_session(appName: str):
        instance = BaseSparkSessionManager.get_instance(appName, "local")
        if instance is None:
            instance = LocalSparkSessionManager(appName)
        return instance.spark

    @staticmethod
    def close_session(appName: str):
        BaseSparkSessionManager.close_session(appName, "local")
