from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
import operators
import helpers

class GitPlugin(AirflowPlugin):
    name = "git_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadDictOperator,
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
    ]
    helpers = [
        helpers.SqlQueries
    ]