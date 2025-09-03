# KPI – Gasto mensal por departamento (+ participação %)

## Objetivo
Mensurar **quanto cada departamento gasta por mês** em benefícios e **qual a participação (%)** de cada um no total do mês. Útil para **acompanhar orçamento**, sazonalidade e **priorizar ações** (ex.: políticas/regras de uso).

## Query (BigQuery)
Arquivo: `sql/01_kpi_gasto_mensal_departamento/01_beneficios.sql`  
Tabelas usadas:  
- `chat.transacoes_beneficios` (fato)  
- `chat.colaboradores` (dim)  
- `chat.departamentos` (dim)

### Como rodar
```sql
WITH gasto_mes AS (
  SELECT
    DATE_TRUNC(t.data, MONTH) AS mes,
    d.nome_departamento,
    ROUND(SUM(t.valor)2) AS gasto_total
  FROM `chat.transacoes_beneficios` t
  JOIN `chat.colaboradores` c USING (id_colaborador)
  JOIN `chat.departamentos` d USING (id_departamento)
  GROUP BY mes, nome_departamento
)
SELECT
  mes,
  nome_departamento,
  gasto_total,
  ROUND(100 * gasto_total / SUM(gasto_total) OVER (PARTITION BY mes), 2) AS share_mes_pct
FROM gasto_mes
ORDER BY mes, gasto_total DESC;
```

## Por que está assim?
- **CTE (`WITH gasto_mes`)**: organiza a lógica em etapas e deixa a leitura/manutenção simples.  
- **Window function com `PARTITION BY`**: calcula o **% de participação dentro do mês** sem precisar novo `GROUP BY`.  
> Obs.: usei CTE e window function também para **demonstrar domínio técnico** (poderia ser resolvido de outra forma).

## Métrica gerada
- `gasto_total`: soma de `valor` por `departamento` e `mês`.  
- `share_mes_pct`: % do `gasto_total` do departamento sobre o total do mês.

## Campos de saída
- `mes` (YYYY-MM-01)  
- `nome_departamento`  
- `gasto_total`  
- `share_mes_pct`