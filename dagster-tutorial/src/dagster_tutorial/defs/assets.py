from dagster_duckdb import DuckDBResource

import dagster as dg


@dg.asset(
    name="orders_aggregation",
    deps=["customers", "orders", "payments"],
)
def orders_aggregation_asset(duckdb: DuckDBResource):
    table_name = "orders_aggregation"

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {table_name} as (
                select
                    c.id as customer_id,
                    c.first_name,
                    c.last_name,
                    count(distinct o.id) as total_orders,
                    count(distinct p.id) as total_payments,
                    coalesce(sum(p.amount), 0) as total_amount_spent
                from customers c
                left join orders o
                    on c.id = o.user_id
                left join payments p
                    on o.id = p.order_id
                group by 1, 2, 3
            );
            """
        )


@dg.asset_check(asset="orders_aggregation")
def orders_aggregation_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    table_name = "orders_aggregation"
    with duckdb.get_connection() as conn:
        result = conn.execute(f"select count(*) from {table_name}").fetchone()
        row_count = result[0] if result else 0

    if row_count == 0:
        return dg.AssetCheckResult(
            passed=False, metadata={"message": "Order aggregation check failed"}
        )

    return dg.AssetCheckResult(
        passed=True, metadata={"message": "Order aggregation check passed"}
    )
