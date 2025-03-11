from pyspark.sql import SparkSession
from tx_bank.common.reader.local_reader import LocalReader
from tx_bank.common.reader.s3_reader import S3Reader
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class S2I_Reader:
    def __init__(self, spark: SparkSession, metadata: dict):
        self.spark = spark
        self.metadata = metadata
        self.dataframes = {}

    def read(self):
        logger.info("Reading input data...")
        mode = self.metadata["mode"]
        for source in self.metadata["input"]:
            format_type = source["format"]
            df_name = source["df_name"]
            file_path = source["table"]
            options = source.get("option", {})

            if mode == "local":
                reader = LocalReader(self.spark, file_path, {
                    "format": format_type, "option": options})
            elif mode == "s3":
                logger.info(f"Using S3Reader for {df_name} from {file_path}")
                reader = S3Reader(self.spark, file_path, {
                                  "format": format_type, "option": options})
            df = reader.read()
            
            
            logger.info(f"df.show(20) {df.show(20)}")
            df.show(20)

            if source.get("isCache", False):
                df = df.cache()

            self.dataframes[df_name] = df

            logger.info(f"Loaded {df_name} from {file_path}.")

        return self.dataframes
