# KPI – Média móvel de 3 meses por departamento

## Objetivo
Avaliar a **tendência de gastos de cada departamento** suavizando variações mensais.  
A média móvel de 3 meses ajuda a enxergar melhor o comportamento ao longo do tempo e detectar **mudanças estruturais de gasto**.

## Query (BigQuery)
Arquivo: `sql/03_kpi_media_movel_de_3_meses_por_departamento/03_media_movel.sql`  
Tabelas usadas:  
- `chat.transacoes_beneficios` (fato)  
- `chat.colaboradores` (dim)  
- `chat.departamentos` (dim)

### Como rodar
```sql
WITH gasto_mensal AS (
  SELECT
    DATE_TRUNC(t.data, MONTH) AS mes,
    d.nome_departamento,
    ROUND(SUM(t.valor),2) AS gasto_total
  FROM `chat.transacoes_beneficios` t
  JOIN `chat.colaboradores` c USING (id_colaborador)
  JOIN `chat.departamentos` d USING (id_departamento)
  GROUP BY mes, nome_departamento
)
SELECT
  nome_departamento,
  mes,
  gasto_total,
  ROUND(AVG(gasto_total) OVER (
    PARTITION BY nome_departamento
    ORDER BY mes
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ), 2) AS media_movel_3m
FROM gasto_mensal
ORDER BY nome_departamento, mes;
```

## Por que está assim?
- **CTE (`WITH gasto_mensal`)**: simplifica a lógica, agregando gastos mensais por depto.  
- **Window function com `PARTITION BY` + `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`**: calcula a média móvel de 3 meses para cada departamento.  
> Obs.: poderia ser feito com subquery, mas aqui usei CTE + janela para **demonstrar domínio técnico**.

## Métrica gerada
- `gasto_total`: soma mensal por departamento.  
- `media_movel_3m`: média dos últimos 3 meses (mês atual + 2 anteriores).

## Campos de saída
- `nome_departamento`  
- `mes`  
- `gasto_total`  
- `media_movel_3m`

## Exemplos de uso prático
- Detectar **crescimento consistente** de gastos em uma área.  
- Suavizar efeitos sazonais (ex.: alta em dezembro, queda em janeiro).  
- Apoiar **planejamento orçamentário** com visão de tendência.

