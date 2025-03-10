from pyspark.sql import SparkSession
import json
from tx_bank.jobs.s2i.common.s2i_reader import S2I_Reader
from tx_bank.jobs.s2i.common.s2i_writer import S2I_Writer
from tx_bank.helper.logger import LoggerSimple
from tx_bank.common.spark_session_manager.local_spark_session_manager import LocalSparkSessionManager
from tx_bank.jobs.s2i.transformation.customer_accounts import CustomerAccountTransformer
import pyspark.sql.functions as F
logger = LoggerSimple.get_logger(__name__)


class G2I_Job:
    def __init__(self, spark: SparkSession, metadata_path: str, transformer_class, data_date: str):
        self.spark = spark
        self.metadata = self._load_metadata(metadata_path)
        self.transformer_class = transformer_class
        self.data_date = data_date

    def _load_metadata(self, metadata_path: str):
        with open(metadata_path, "r") as f:
            return json.load(f)

    def run(self):
        logger.info("Starting ETL Job...")

        reader = S2I_Reader(self.spark, self.metadata)
        dataframes = reader.read()

        transformer = self.transformer_class(dataframes)
        transformed_df = transformer.transform()

        transformed_df = transformed_df.withColumn(
            "data_date", F.lit(self.data_date))

        writer = S2I_Writer(self.spark, self.metadata, self.data_date)
        writer.write(transformed_df)

        logger.info("ETL Job completed successfully.")


def get_dbutils():
    try:
        from pyspark.dbutils import DBUtils
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except ImportError:
        logger.info("DBUtils not found. Running in local mode.")
        return None


def main():
    dbutils = get_dbutils()
    if dbutils:
        logger.info("Running in Databricks mode")
        job_name = dbutils.jobs.taskValues.get(
            taskKey="create_params", key="job_name")
        data_date = dbutils.jobs.taskValues.get(
            taskKey="create_params", key="data_date")
        skip_condition = dbutils.jobs.taskValues.get(
            taskKey="create_params", key="skip_condition")
        metadata_filepath = f"/Workspace/Shared/tx_project_metadata/{job_name}.json"
    else:
        logger.info("Running in local mode")
        metadata_filepath = "tx_training/jobs/g2i/monthly.json"
        data_date = "2024-01-05"

        spark = LocalSparkSessionManager.get_session("test")
        metadata_path = "tx_bank/job_entries/r2s/customer_account.json"

    etl_job = G2I_Job(spark, metadata_path,
                      CustomerAccountTransformer, data_date)
    etl_job.run()


if __name__ == "__main__":
    main()
