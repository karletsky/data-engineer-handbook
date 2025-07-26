from chispa.dataframe_comparer import *
from collections import namedtuple

from ..jobs.actors_scd_job import do_actor_scd_transformation

Actor = namedtuple("Actor", "actor current_year quality_class is_active")
ActorSCD = namedtuple("ActorSCD", "actor quality_class is_active start_date end_date current_year")

def test_actor_scd_transformation(spark):
    # Input data: sequence of yearly records per actor
    input_data = [
        Actor('A-1', 2018, 'A', True),
        Actor('A-1', 2019, 'A', True),
        Actor('A-1', 2020, 'B', True),
        Actor('A-2', 2018, 'C', False),
        Actor('A-2', 2019, 'C', False),
        Actor('A-2', 2020, 'C', True),
    ]
    input_df = spark.createDataFrame(input_data)

    # Apply transformation
    actual_df = do_actor_scd_transformation(spark, input_df)

    # Expected output: one row per continuous streak of same class/active status
    expected_output = [
        ActorSCD('A-1', 'A', True, 2018, 2019, 2020),
        ActorSCD('A-1', 'B', True, 2020, 2020, 2020),
        ActorSCD('A-2', 'C', False, 2018, 2019, 2020),
        ActorSCD('A-2', 'C', True, 2020, 2020, 2020),
    ]
    expected_df = spark.createDataFrame(expected_output)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
