import os
from pyspark.sql import SparkSession, DataFrame
from tx_bank.common.reader.base_reader import BaseReader
from tx_bank.helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class LocalReader(BaseReader):
    def __init__(self, spark: SparkSession, path: str, config: dict):
        super().__init__(spark, path, config)
        self.format = config.get("format", "parquet")

    def read(self) -> DataFrame:
        if self.format == "csv":
            return self._read_csv()
        elif self.format == "parquet":
            return self._read_parquet()
        elif self.format == "delta":
            return self._read_delta()
        else:
            raise ValueError(f"Unsupported format: {self.format}")

    def _read_csv(self) -> DataFrame:
        options = self.config.get("option", {})
        reader = self.spark.read.format("csv")

        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(self.path)

    def _read_parquet(self) -> DataFrame:
        return self.spark.read.parquet(self.path)

    def _read_delta(self) -> DataFrame:
        if "." in self.path and not self.path.startswith("dbfs:/"):
            return self.spark.table(self.path)
        return self.spark.read.format("delta").load(self.path)
