WITH gasto_mes AS (
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
  mes,
  nome_departamento,
  gasto_total,
  ROUND(100 * gasto_total / SUM(gasto_total) OVER (PARTITION BY mes), 2) AS share_mes_pct
FROM gasto_mes
ORDER BY mes, gasto_total DESC;