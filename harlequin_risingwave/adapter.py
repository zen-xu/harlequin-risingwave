from __future__ import annotations

from typing import Any, Sequence

from harlequin import HarlequinCompletion, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError
from harlequin.options import TextOption
from harlequin_postgres import HarlequinPostgresAdapter, cli_options
from harlequin_postgres.adapter import HarlequinPostgresConnection
from harlequin_postgres.loaders import register_inf_loaders
from psycopg import Connection

from .catalog import RisingwaveDatabaseCatalogItem
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
        connect_timeout: int | float | None = None,
        sslmode: str | None = None,
        sslcert: str | None = None,
        sslkey: str | None = None,
        timezone: str | None = None,
        **_: Any,
    ) -> None:
        super().__init__(
            conn_str,
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            passfile=passfile,
            require_auth=require_auth,
            channel_binding=channel_binding,
            connect_timeout=connect_timeout,
            sslmode=sslmode,
            sslcert=sslcert,
            sslkey=sslkey,
            **_,
        )
        self.timezone = timezone

    def connect(self) -> HarlequinRisingwaveConnection:
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide multiple connection strings to the Risingwave adapter. "
                f"{self.conn_str}"
            )
        register_inf_loaders()
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
        db_items: list[CatalogItem] = [
            RisingwaveDatabaseCatalogItem.from_label(label=db, connection=self)
            for (db,) in databases
        ]
        return Catalog(items=db_items)

    def get_completions(self) -> list[HarlequinCompletion]:
        conn: Connection = self.pool.getconn()
        completions = get_completions(conn)
        self.pool.putconn(conn)
        return completions
