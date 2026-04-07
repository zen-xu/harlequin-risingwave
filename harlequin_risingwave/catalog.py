from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from harlequin.catalog import InteractiveCatalogItem
from harlequin_postgres.catalog import (
    MaterializedViewCatalogItem,
    RelationCatalogItem,
    TableCatalogItem,
    ViewCatalogItem,
)

if TYPE_CHECKING:
    from harlequin_risingwave.adapter import HarlequinRisingwaveConnection


class SourceCatalogItem(RelationCatalogItem):
    @classmethod
    def from_parent(
        cls,
        parent: "RisingwaveSchemaCatalogItem",
        label: str,
    ) -> "SourceCatalogItem":
        relation_query_name = f'"{parent.label}"."{label}"'
        relation_qualified_identifier = f'{parent.qualified_identifier}."{label}"'
        return cls(
            qualified_identifier=relation_qualified_identifier,
            query_name=relation_query_name,
            label=label,
            type_label="source",
            connection=parent.connection,
            parent=parent,
        )


class SinkCatalogItem(RelationCatalogItem):
    @classmethod
    def from_parent(
        cls,
        parent: "RisingwaveSchemaCatalogItem",
        label: str,
    ) -> "SinkCatalogItem":
        relation_query_name = f'"{parent.label}"."{label}"'
        relation_qualified_identifier = f'{parent.qualified_identifier}."{label}"'
        return cls(
            qualified_identifier=relation_qualified_identifier,
            query_name=relation_query_name,
            label=label,
            type_label="sink",
            connection=parent.connection,
            parent=parent,
        )


@dataclass
class RisingwaveSchemaCatalogItem(
    InteractiveCatalogItem["HarlequinRisingwaveConnection"]
):
    parent: "RisingwaveDatabaseCatalogItem | None" = None

    @classmethod
    def from_parent(
        cls,
        parent: "RisingwaveDatabaseCatalogItem",
        label: str,
    ) -> "RisingwaveSchemaCatalogItem":
        schema_identifier = f'"{label}"'
        return cls(
            qualified_identifier=schema_identifier,
            query_name=schema_identifier,
            label=label,
            type_label="sch",
            connection=parent.connection,
            parent=parent,
        )

    def fetch_children(self) -> list[RelationCatalogItem]:
        if self.parent is None or self.connection is None:
            return []
        children: list[RelationCatalogItem] = []

        result = self.connection._get_relations(self.parent.label, self.label)
        for table_label, table_type in result:
            if table_type == "VIEW":
                children.append(
                    ViewCatalogItem.from_parent(parent=self, label=table_label)  # type: ignore[arg-type]
                )
            elif table_type == "BASE TABLE":
                children.append(
                    TableCatalogItem.from_parent(parent=self, label=table_label)  # type: ignore[arg-type]
                )
            elif table_type == "MATERIALIZED VIEW":
                children.append(
                    MaterializedViewCatalogItem.from_parent(
                        parent=self,
                        label=table_label,  # type: ignore[arg-type]
                    )
                )
            elif table_type == "SOURCE":
                children.append(
                    SourceCatalogItem.from_parent(parent=self, label=table_label)
                )
            elif table_type == "SINK":
                children.append(
                    SinkCatalogItem.from_parent(parent=self, label=table_label)
                )

        return children


class RisingwaveDatabaseCatalogItem(
    InteractiveCatalogItem["HarlequinRisingwaveConnection"]
):
    @classmethod
    def from_label(
        cls, label: str, connection: "HarlequinRisingwaveConnection"
    ) -> "RisingwaveDatabaseCatalogItem":
        database_identifier = f'"{label}"'
        return cls(
            qualified_identifier=database_identifier,
            query_name=database_identifier,
            label=label,
            type_label="db",
            connection=connection,
        )

    def fetch_children(self) -> list[RisingwaveSchemaCatalogItem]:
        if self.connection is None:
            return []
        schemas = self.connection._get_schemas(self.label)
        return [
            RisingwaveSchemaCatalogItem.from_parent(
                parent=self,
                label=schema_label,
            )
            for (schema_label,) in schemas
        ]
