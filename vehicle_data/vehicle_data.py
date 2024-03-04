import os
from logging import getLogger
import time

import pandas as pd

from analytics.aws.athena_client import AthenClient
from analytics.sql.query_builder import TrueDetectionsQuery, CountedDistancesQuery, RoundedDistanceQuery


class VehicleData:
    """The main class for querying vehicle data from athena."""

    def __init__(self, db: str, s3_results_uri) -> None:
        self._logger = getLogger(self.__class__.__name__)
        self._athena = AthenClient(db, s3_results_uri)
        self.query_builder = RoundedDistanceQuery()
        self._vehicles = set()
        self._min = 1
        self._max = 100
        self._step = 10
        self._results = None

    def exclude_vehicles(self, vehicles: set):
        """Excludes the vehicles specifeid from the results.

        Args:
            vehicles (set): a set of vehicles types.
        """
        self._vehicles |= vehicles

    def set_boundaries(
        self, min_dist: int = 1, max_dist: int = 100, step_dist: int = 10
    ):
        """Sets the boundaries for query data.

        Args:
            min_dist (int, optional): The minimum distance to query. Defaults to 1.
            max_dist (int, optional): The maximum distance to query. Defaults to 100.
            step_dist (int, optional): The size of step differentiate which will divide the gap
                between max_dist and min_dist to different bins. Defaults to 10.

        Raises:
            ValueError: In case min_dist <= 0 or in case min_dist >= max_dist
            ValueError: In case step_dist > 0 or step_dist <= (max_dist - min_dist)
        """
        if min_dist < 0 or min_dist >= max_dist:
            raise ValueError(
                "min_dist and max must qualified for: min_dist > 0 , max_dist > 0 , min_dist < max_dist"
            )
        if step_dist <= 0 or step_dist > (max_dist - min_dist):
            raise ValueError(
                "step should qualified for: step_dist > 0, step_dist <= (max_dist - min_dist)"
            )
        self._min = min_dist
        self._max = max_dist
        self._step = step_dist

    def run(self):
        """Sends the Query to athena and wait for results.
        The function uses following environment variables:
        QUERY_TIMEOUT_SECS - To determine how long to wait for query to complete.
        QUERY_STATUS_CHECK_INTERVAL_SECS - To set the interval betwees status check of the query.

        Returns:
            bool: True in case query was successfully executed, False otherwise.
        """
        res = False
        self.query_builder.build_query(min_dist=self._min, max_dist=self._max, step_dist=self._step)
        timeout = int(os.getenv("QUERY_TIMEOUT_SECS", "5"))
        interval = float(os.getenv("QUERY_STATUS_CHECK_INTERVAL_SECS", "0.1"))
        start_time = time.perf_counter()
        self._athena.execute(self.query_builder.query)
        while self._athena.status != "SUCCEEDED":
            time.sleep(interval)
            if time.perf_counter() - start_time > timeout:
                self._logger.error(
                    f"The time limit of {timeout} seconds has exceeded for this query."
                )
                break

        if self._athena.status == "SUCCEEDED":
            self._results = self._athena.get_query_results()
            self._logger.info(
                f"Successfully retrived query_id: {self._athena.query_id} results."
            )
            res = True
        else:
            self._logger.error("Failed to retrive query results.")

        return res

    @property
    def results(self) -> pd.DataFrame:
        """Return the query results in pandas DataFrame format.

        Returns:
            pd.DataFrame: A Dataframe object with the query results.
        """
        if self.results:
            return self._results
