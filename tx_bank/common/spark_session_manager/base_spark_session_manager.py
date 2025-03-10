from pyspark.sql import SparkSession


class BaseSparkSessionManager:
    """
    A base class for managing SparkSession instances efficiently.

    This class ensures that SparkSessions are managed as singleton instances
    based on their `appName` and `master` configurations. It allows retrieval,
    creation, and proper cleanup of Spark sessions.
    """
    _instances = {}

    def __init__(self, appName: str, master: str):
        """
        Initializes the BaseSparkSessionManager instance and creates a Spark session.

        :param appName: The name of the Spark application.
        :param master: The master URL to connect to the Spark cluster (e.g., "local", "yarn").
        """
        self.appName = appName
        self.master = master
        self.spark = self._create_spark_session()
        BaseSparkSessionManager._instances[(appName, master)] = self

    def _create_spark_session(self) -> SparkSession:
        """
        Abstract method to create a Spark session.

        This method must be implemented in subclasses to define the Spark session creation logic.

        :return: A SparkSession instance.
        """
        raise NotImplementedError(
            "Subclasses must implement _create_spark_session()")

    def get_session(self) -> SparkSession:
        """
        Returns the Spark session associated with this instance.

        :return: The SparkSession instance.
        """
        return self.spark

    @classmethod
    def get_instance(cls, appName: str, master: str):
        """
        Retrieves an existing SparkSessionManager instance based on `appName` and `master`.

        :param appName: The name of the Spark application.
        :param master: The master URL.
        :return: The existing instance of SparkSessionManager or None if not found.
        """
        return cls._instances.get((appName, master))

    @classmethod
    def close_session(cls, appName: str, master: str):
        """
        Closes and removes the Spark session associated with the given `appName` and `master`.

        :param appName: The name of the Spark application.
        :param master: The master URL.
        """
        instance = cls._instances.get((appName, master))
        if instance is not None:
            instance.spark.stop()
            del cls._instances[(appName, master)]
            logger.info("Spark session closed.")
