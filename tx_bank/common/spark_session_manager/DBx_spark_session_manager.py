from pyspark.sql import SparkSession
from tx_bank.common.spark_session_manager.base_spark_session_manager import (
    BaseSparkSessionManager,
)
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class DBxSparkSessionManager(BaseSparkSessionManager):

    def __init__(self, appName: str):
        self.appName = appName
        self.master = "dbx"
        self.spark = self._create_spark_session()
        BaseSparkSessionManager._instances[(appName, self.master)] = self

    def _create_spark_session(self) -> SparkSession:
        logger.info("Getting existing Databricks SparkSession")
        return SparkSession.builder.config(
            "spark.databricks.delta.schema.autoMerge.enabled", "true"
        ).getOrCreate()

    @staticmethod
    def get_session(appName: str):
        instance = BaseSparkSessionManager.get_instance(appName, "dbx")
        if instance is None:
            instance = DBxSparkSessionManager(appName)
        return instance.spark
