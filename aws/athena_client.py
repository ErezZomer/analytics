import io
import logging

import boto3
import pandas as pd


class AthenClient:
    """A class to handle query and retreival of data from aws s3.
    """
    def __init__(self, db: str, s3_results_path: str) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = boto3.client("athena")
        self._resource = boto3.resource("s3")
        self._bucket, self._folder = s3_results_path.split("//", 1)[1].split("/", 1)
        self._context_config = {"Database": db}
        self._results_config = {"OutputLocation": s3_results_path}
        self._execution_id = None
        self._details = None

    def execute(self, query: str) -> int:
        """Sends the Query to Athena and retrieves the execution id.

        Args:
            query (str): The SQL query.
        
        Returns:
            (int): The execution id (query id).
        """
        self._execution_id = None
        self._details = None
        query_execution = self._client.start_query_execution(
            QueryString=query,
            QueryExecutionContext=self._context_config,
            ResultConfiguration=self._results_config,
        )
        self._execution_id = query_execution["QueryExecutionId"]
        return self.query_id

    def update_query_details(self) -> None:
        """_summary_
        """
        self._details = self._client.get_query_execution(
            QueryExecutionId=self._execution_id
        )

    @property
    def status(self) -> str:
        self.update_query_details()
        return self._details["QueryExecution"]["Status"]["State"]

    def get_query_results(self) -> pd.DataFrame:
        """Fethces the query results from S3 and resturns them in pandas's DataFrame object.

        Returns:
            pd.DataFrame:
        """
        try:
            response = (
                self._resource.Bucket(self._bucket)
                .Object(key=self._folder + self._execution_id + ".csv")
                .get()
            )
            return pd.read_csv(io.BytesIO(response["Body"].read()), encoding="utf8")
        except Exception as e:
            self._logger.error(
                f"Got the following error when trying to fetch data from S3: {e}"
            )
            return None

    def query_id(self) -> int:
        """A property for getting execution id.

        Returns:
            (int): If succeeded returns the execution id, -1 otherwise.
        """
        if self._execution_id:
            return self._execution_id
        else:
            return -1

