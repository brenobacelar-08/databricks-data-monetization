# Mapeamento de Tipos: Delta Lake → BigQuery

| Delta / Spark      | BigQuery          | Notas                                              |
|--------------------|-------------------|----------------------------------------------------|
| `StringType`       | `STRING`          | —                                                  |
| `LongType`         | `INT64`           | —                                                  |
| `IntegerType`      | `INT64`           | BigQuery não tem INT32 nativo                       |
| `DoubleType`       | `FLOAT64`         | —                                                  |
| `FloatType`        | `FLOAT64`         | Upcast automático pelo conector                     |
| `DecimalType(p,s)` | `NUMERIC`/`BIGNUMERIC` | Precision > 29 → BIGNUMERIC                   |
| `BooleanType`      | `BOOL`            | —                                                  |
| `DateType`         | `DATE`            | —                                                  |
| `TimestampType`    | `TIMESTAMP`       | UTC no BigQuery; fuso local no Spark (cuidado!)     |
| `ArrayType`        | `REPEATED`        | Suportado, mas evite na Gold — dificulta SQL       |
| `MapType`          | Não suportado     | Exploda em colunas antes de exportar               |
| `StructType`       | `RECORD` (STRUCT) | Suportado, mas BI em cima de STRUCT é verboso      |

## Pontos de atenção

**Timestamps com fuso:** o Databricks guarda timestamps no fuso do cluster.
O BigQuery interpreta timestamps como UTC. Se suas datas aparecerem com 3h de diferença,
converta explicitamente no notebook de export:

```python
F.to_utc_timestamp(F.col("_ingested_at"), "America/Sao_Paulo")
```

**Nomes de colunas:** BigQuery não aceita colunas que começam com `_` em modo REQUIRED.
As colunas de auditoria Bronze (`_row_hash`, `_source_file`, `_ingested_at`) são
descartadas no export (a Gold não as carrega).

**Particionamento:** na Gold Delta você pode ter `PARTITIONED BY (event_year, event_month)`.
No BigQuery, ao criar a tabela, use `timePartitioning` ou `rangePartitioning` — o script
de export já inclui esse config.
