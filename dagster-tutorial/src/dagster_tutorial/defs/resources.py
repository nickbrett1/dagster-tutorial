from dagster_duckdb import DuckDBResource

import dagster as dg

database_resource = DuckDBResource(database="/tmp/jaffle_shop.duckdb")


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
        }
    )
