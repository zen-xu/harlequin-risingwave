from __future__ import annotations

from harlequin import HarlequinCompletion
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

    def get_completions(self) -> list[HarlequinCompletion]:
        conn: connection = self.pool.getconn()
        completions = get_completions(conn)
        self.pool.putconn(conn)
        return completions
