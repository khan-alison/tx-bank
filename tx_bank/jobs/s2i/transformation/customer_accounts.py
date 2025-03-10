from pyspark.sql import DataFrame
from tx_bank.helper.logger import LoggerSimple
import pyspark.sql.functions as F

logger = LoggerSimple.get_logger(__name__)


class CustomerAccountTransformer:
    def __init__(self, dataframes: dict):
        self.dataframes = dataframes

    def transform(self) -> DataFrame:
        logger.info(
            "Performing transformation: Joining Accounts with Customers.")

        if "accounts" not in self.dataframes or "customers" not in self.dataframes:
            raise ValueError(
                "Required DataFrames (accounts, customers) are missing.")

        accounts_df = self.dataframes["accounts"]
        customers_df = self.dataframes["customers"]

        transformed_df = accounts_df.alias("a").join(
            customers_df.alias("c"), on="CustomerID", how="inner").drop(F.col("c.AccountType"))

        transformed_df.show(20)
        logger.info("Transformation complete.")
        return transformed_df
