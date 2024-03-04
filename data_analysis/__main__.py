import logging
from analytics.data_analysis.vehicle_data import VehicleData

Logger = logging.getLogger("Analytics")


def main() -> int:
    """The initial main function to launch the progeam.

    Returns:
        (int): 0 on success, -1 on failure.
    """
    try:
        analytics = VehicleData(
            "sensor_data", "s3://erez-test-bucket-me/reports/output/"
        )
        if analytics.run():
            print(analytics.results)
        else:
            Logger.error("Failsed to execute query!")
        return 0
    except Exception as e:
        Logger.error(f"While trying to query data, got the followig exception: {e} .")
        return -1


if __name__ == "__main__":
    exit(main())
