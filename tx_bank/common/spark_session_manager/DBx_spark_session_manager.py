from pyspark.sql import SparkSession
from tx_bank.common.spark_session_manager.base_spark_session_manager import (
    BaseSparkSessionManager,
)
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class DBxSparkSessionManager(BaseSparkSessionManager):
    """
    Manages SparkSession for Databricks (DBX) environment.

    This class ensures that a Databricks SparkSession is managed properly,
    supporting auto-merging of Delta schemas.
    """

    def __init__(self, appName: str):
        """
        Initializes the Databricks SparkSessionManager.

        :param appName: The name of the Spark application.
        """
        self.appName = appName
        self.master = "dbx"
        self.spark = self._create_spark_session()
        BaseSparkSessionManager._instances[(appName, self.master)] = self

    def _create_spark_session(self) -> SparkSession:
        """
        Retrieves the existing Databricks SparkSession.

        Databricks automatically manages the SparkSession, so we fetch it
        using `getOrCreate()` and enable auto-merging for Delta tables.

        :return: A SparkSession instance.
        """
        logger.info("Getting existing Databricks SparkSession")
        return SparkSession.builder.config(
            "spark.databricks.delta.schema.autoMerge.enabled", "true"
        ).getOrCreate()

    @staticmethod
    def get_session(appName: str):
        """
        Retrieves or creates a Databricks SparkSession for the given application.

        :param appName: The name of the Spark application.
        :return: The SparkSession instance.
        """
        """Get a SparkSession for the application"""
        instance = BaseSparkSessionManager.get_instance(appName, "dbx")
        if instance is None:
            instance = DBxSparkSessionManager(appName)
        return instance.spark
