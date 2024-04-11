from __future__ import annotations

from harlequin import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError
from harlequin_postgres import HarlequinPostgresAdapter
from harlequin_postgres.adapter import HarlequinPostgresConnection
from psycopg2.extensions import connection

from .completion import get_completions


class HarlequinRisingwaveAdapter(HarlequinPostgresAdapter):  # type: ignore[misc]
    def connect(self) -> HarlequinRisingwaveConnection:
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide multiple connection strings to the Risingwave adapter. "
                f"{self.conn_str}"
            )
        conn = HarlequinRisingwaveConnection(self.conn_str, options=self.options)
        return conn


class HarlequinRisingwaveConnection(HarlequinPostgresConnection):  # type: ignore[misc]
    def get_catalog(self) -> Catalog:
        databases = self._get_databases()
        db_items: list[CatalogItem] = []
        for (db,) in databases:
            schemas = self._get_schemas(db)
            schema_items: list[CatalogItem] = []
            for (schema,) in schemas:
                table_catalog = CatalogItem(
                    qualified_identifier=f"{db}.{schema}.table",
                    query_name=f"{db}.{schema}.table",
                    label="table",
                    type_label="t",
                    children=self._get_table(db, schema, "BASE TABLE", "table", "t"),
                )

                view_catalog = CatalogItem(
                    qualified_identifier=f"{db}.{schema}.view",
                    query_name=f"{db}.{schema}.view",
                    label="view",
                    type_label="v",
                    children=self._get_table(db, schema, "VIEW", "view", "v"),
                )

                materialized_view_catalog = CatalogItem(
                    qualified_identifier=f"{db}.{schema}.materialized_view",
                    query_name=f"{db}.{schema}.materialized_view",
                    label="materialized_view",
                    type_label="mv",
                    children=self._get_table(
                        db, schema, "MATERIALIZED VIEW", "materialized_view", "mv"
                    ),
                )

                source_catalog = CatalogItem(
                    qualified_identifier=f"{db}.{schema}.source",
                    query_name=f"{db}.{schema}.source",
                    label="source",
                    type_label="source",
                    children=self._get_table(db, schema, "SOURCE", "source", "source"),
                )

                sink_catalog = CatalogItem(
                    qualified_identifier=f"{db}.{schema}.sink",
                    query_name=f"{db}.{schema}.sink",
                    label="sink",
                    type_label="sink",
                    children=self._get_table(db, schema, "SINK", "sink", "sink"),
                )

                schema_items.append(
                    CatalogItem(
                        qualified_identifier=f"{db}.{schema}",
                        query_name=f"{db}.{schema}",
                        label=schema,
                        type_label="s",
                        children=[
                            table_catalog,
                            view_catalog,
                            materialized_view_catalog,
                            source_catalog,
                            sink_catalog,
                        ],
                    )
                )
            db_items.append(
                CatalogItem(
                    qualified_identifier=db,
                    query_name=db,
                    label=db,
                    type_label="db",
                    children=schema_items,
                )
            )
        return Catalog(items=db_items)

    def _get_schemas(self, dbname: str) -> list[tuple[str]]:
        conn: connection = self.pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select distinct table_schema
                from information_schema.tables
                where
                    table_catalog = '{dbname}'
                    and table_schema != 'information_schema'
                    and table_schema not like 'pg_%'
                order by table_schema asc
                ;"""
            )
            results: list[tuple[str]] = cur.fetchall()

        self.pool.putconn(conn)
        return results

    def _get_table(
        self,
        dbname: str,
        schema: str,
        table_type: str,
        table_identifier: str,
        type_label: str,
    ) -> list[CatalogItem]:
        conn: connection = self.pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select table_name
                from information_schema.tables
                where
                    table_catalog = '{dbname}'
                    and table_schema = '{schema}'
                    and table_type = '{table_type}'
                order by table_name asc;
                """
            )
            table_names: list[str] = [row[0] for row in cur.fetchall()]
            tables = [
                CatalogItem(
                    qualified_identifier=f"{dbname}.{schema}.{table_name}",
                    query_name=f"{dbname}.{schema}.{table_name}",
                    label=table_name,
                    type_label=type_label,
                    children=[
                        CatalogItem(
                            qualified_identifier=f"{dbname}.{schema}.{table_name}.{col}",
                            query_name=col,
                            label=col,
                            type_label=self._get_short_type(col_type),
                        )
                        for col, col_type in self._get_columns(
                            dbname, schema, table_name
                        )
                    ],
                )
                for table_name in table_names
            ]

        self.pool.putconn(conn)
        return tables

    def get_completions(self) -> list[HarlequinCompletion]:
        conn: connection = self.pool.getconn()
        completions = get_completions(conn)
        self.pool.putconn(conn)
        return completions
