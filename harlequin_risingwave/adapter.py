from __future__ import annotations

from typing import Any, Sequence

from harlequin import HarlequinCompletion, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError
from harlequin.options import TextOption
from harlequin_postgres import HarlequinPostgresAdapter, cli_options
from harlequin_postgres.adapter import HarlequinPostgresConnection
from psycopg2.extensions import connection

from .completion import get_completions

RISINGWAVE_OPTIONS = [
    *cli_options.POSTGRES_OPTIONS,
    TextOption(
        "timezone",
        description="Timezone to use for the connection.",
    ),
]


class HarlequinRisingwaveAdapter(HarlequinPostgresAdapter):  # type: ignore[misc]
    ADAPTER_OPTIONS = RISINGWAVE_OPTIONS

    def __init__(
        self,
        conn_str: Sequence[str],
        host: str | None = None,
        port: str | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        passfile: str | None = None,
        require_auth: str | None = None,
        channel_binding: str | None = None,
        connect_timeout: int | None = None,
        sslmode: str | None = None,
        sslcert: str | None = None,
        sslkey: str | None = None,
        timezone: str | None = None,
        **_: Any,
    ) -> None:
        super().__init__(
            conn_str,
            host,
            port,
            dbname,
            user,
            password,
            passfile,
            require_auth,
            channel_binding,
            connect_timeout,
            sslmode,
            sslcert,
            sslkey,
            **_,
        )
        self.timezone = timezone

    def connect(self) -> HarlequinRisingwaveConnection:
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide multiple connection strings to the Risingwave adapter. "
                f"{self.conn_str}"
            )
        conn = HarlequinRisingwaveConnection(
            self.conn_str, options=self.options, timezone=self.timezone
        )
        return conn


class HarlequinRisingwaveConnection(HarlequinPostgresConnection):  # type: ignore[misc]
    def __init__(
        self,
        conn_str: Sequence[str],
        *_: Any,
        init_message: str = "",
        options: dict[str, Any],
        timezone: str | None = None,
    ) -> None:
        self.timezone = timezone
        super().__init__(conn_str, *_, init_message=init_message, options=options)

    def execute(self, query: str) -> HarlequinCursor | None:
        if self.timezone:
            query = f'set timezone = "{self.timezone}";\n{query}'
        return super().execute(query)

    def get_catalog(self) -> Catalog:
        databases = self._get_databases()
        db_items: list[CatalogItem] = []
        for (db,) in databases:
            schemas = self._get_schemas(db)
            schema_items: list[CatalogItem] = []
            for (schema,) in schemas:
                schema_items.append(
                    CatalogItem(
                        qualified_identifier=f"{db}.{schema}",
                        query_name=f"{db}.{schema}",
                        label=schema,
                        type_label="s",
                        children=[
                            *self._get_table(db, schema, "BASE TABLE", "table", "t"),
                            *self._get_table(db, schema, "VIEW", "view", "v"),
                            *self._get_table(
                                db,
                                schema,
                                "MATERIALIZED VIEW",
                                "materialized_view",
                                "mv",
                            ),
                            *self._get_table(db, schema, "SOURCE", "source", "source"),
                            *self._get_table(db, schema, "SINK", "sink", "sink"),
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
