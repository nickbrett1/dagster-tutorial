from dagster_duckdb import DuckDBResource

import dagster as dg


class ETL(dg.Model):
    url_path: str
    table: str


class Tutorial(dg.Component, dg.Model, dg.Resolvable):
    # The interface for the component
    duckdb_database: str
    etl_steps: list[ETL]

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _etl_assets = []

        for etl in self.etl_steps:

            def make_asset(current_etl: ETL):
                @dg.asset(name=current_etl.table)
                def _table(duckdb: DuckDBResource):
                    with duckdb.get_connection() as conn:
                        conn.execute(
                            f"""
                            create or replace table {current_etl.table} as (
                                select * from read_csv_auto('{current_etl.url_path}')
                            )
                            """
                        )
                return _table

            _etl_assets.append(make_asset(etl))

        return dg.Definitions(
            assets=_etl_assets,
            resources={"duckdb": DuckDBResource(
                database=self.duckdb_database)},
        )
