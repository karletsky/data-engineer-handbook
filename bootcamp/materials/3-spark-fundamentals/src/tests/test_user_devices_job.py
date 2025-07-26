from chispa.dataframe_comparer import *
from collections import namedtuple

from ..jobs.user_devices_job import do_user_devices_transformation

UserDevice = namedtuple("UserDevice", "user_id browser_type device_activity_datelist date")

def test_user_devices_transformation(spark):
    # Input data: yesterday’s cumulated, today’s events (simulate minimal working sample)
    yesterday_data = [
        UserDevice("u1", "Chrome", ["2023-01-30"], "2023-01-30")
    ]
    today_data = [
        UserDevice("u1", "Chrome", None, "2023-01-31"),
        UserDevice("u2", "Firefox", None, "2023-01-31"),
    ]
    # Union both sets; in practice, loaded from different sources/views.
    input_df = spark.createDataFrame(yesterday_data + today_data)

    # Apply transformation
    actual_df = do_user_devices_transformation(spark, input_df)

    # Expected output after processing
    expected_output = [
        UserDevice("u1", "Chrome", ["2023-01-31", "2023-01-30"], "2023-01-31"),
        UserDevice("u2", "Firefox", ["2023-01-31"], "2023-01-31"),
    ]
    expected_df = spark.createDataFrame(expected_output)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
