"""Configuração central de catalog/schema/paths usada pelos notebooks.

Mantido fora dos notebooks para que a mesma config seja reaproveitada pelo
job de export ao BigQuery sem duplicar valores.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class CatalogConfig:
    catalog: str = "data_monetization"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    raw_volume: str = "raw_csv"

    @property
    def raw_volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.raw_volume}"

    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"


DEFAULT_CONFIG = CatalogConfig()

# Nome lógico da tabela de negócio ao longo das camadas. Troque aqui se quiser
# outro nome — os notebooks importam essa constante em vez de hardcodar.
BUSINESS_TABLE_NAME = "events"

# Contrato de colunas geográficas esperadas no CSV de origem. Ajuste para bater
# com as colunas reais do seu arquivo antes de rodar 02_silver_cleaning_geo.
GEO_COLUMNS = {
    "city": "city",
    "state": "state",       # UF, ex: "SP"
    "latitude": "latitude",
    "longitude": "longitude",
}
