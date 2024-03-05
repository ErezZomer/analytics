# analytics

# Prerequisites
1. set environment variables:
    - AWS_ACCESS_KEY_ID=<your_access_id>
    - AWS_SECRET_ACCESS_KEY=<your_access_key>
    - AWS_DEFAULT_REGION=eu-west-3

# installation:
1. navigate to analytics directory.
2. run: python3 setup.py bdist_wheel
3. pip3 install dist/analytics-1.0.0.post0-py3-none-any.whl 

# usage:
    from analytics import VehicleData
    analytics = VehicleData("my_db_name", "my_s3_bucket_uri")
    if analytics.run():
        print(analytics.results)
