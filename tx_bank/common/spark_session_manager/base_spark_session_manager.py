from pyspark.sql import SparkSession


class BaseSparkSessionManager:
    _instances = {}

    def __init__(self, appName: str, master: str):
        self.appName = appName
        self.master = master
        self.spark = self._create_spark_session()
        BaseSparkSessionManager._instances[(appName, master)] = self

    def _create_spark_session(self) -> SparkSession:
        raise NotImplementedError(
            "Subclasses must implement _create_spark_session()")

    def get_session(self) -> SparkSession:
        return self.spark

    @classmethod
    def get_instance(cls, appName: str, master: str):
        return cls._instances.get((appName, master))

    @classmethod
    def close_session(cls, appName: str, master: str):
        instance = cls._instances.get((appName, master))
        if instance is not None:
            instance.spark.stop()
            del cls._instances[(appName, master)]
            logger.info("Spark session closed.")
