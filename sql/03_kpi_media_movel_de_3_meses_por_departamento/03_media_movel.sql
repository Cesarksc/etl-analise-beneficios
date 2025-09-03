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
