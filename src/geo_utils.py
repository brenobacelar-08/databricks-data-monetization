"""Utilitários geográficos reutilizáveis entre notebooks Databricks e o export
para BigQuery. Usa PySpark (roda em cluster Databricks); se algum dia precisar
rodar fora do Spark (ex: script local), adapte para pandas usando a mesma lógica
de `UF_TO_REGION`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Mapeamento de UF -> Região (Brasil). Use como exemplo; ajuste se seus dados
# forem de outro país ou granularidade.
UF_TO_REGION = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte",
    "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste",
    "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste",
    "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}


def normalize_text(col: Column) -> Column:
    """Trim + uppercase, para padronizar cidade/UF antes de comparar/agrupar."""
    return F.upper(F.trim(col))


def with_region(df: DataFrame, state_col: str, output_col: str = "region") -> DataFrame:
    """Adiciona uma coluna de região a partir da UF, via mapeamento UF_TO_REGION."""
    mapping_expr = F.create_map([F.lit(x) for pair in UF_TO_REGION.items() for x in pair])
    return df.withColumn(output_col, mapping_expr.getItem(F.upper(F.trim(F.col(state_col)))))


def with_valid_coordinates_flag(
    df: DataFrame,
    lat_col: str,
    lon_col: str,
    output_col: str = "has_valid_coordinates",
) -> DataFrame:
    """Marca linhas com lat/lon fora do range válido ou nulas, sem descartá-las
    (decisão de filtrar ou não fica na camada que consome — Silver guarda o dado,
    só sinaliza qualidade)."""
    is_valid = (
        F.col(lat_col).isNotNull()
        & F.col(lon_col).isNotNull()
        & F.col(lat_col).between(-90, 90)
        & F.col(lon_col).between(-180, 180)
    )
    return df.withColumn(output_col, is_valid)
