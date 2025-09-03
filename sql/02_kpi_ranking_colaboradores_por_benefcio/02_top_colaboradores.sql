WITH gasto_colab AS (
  SELECT
    b.nome_beneficio,
    c.id_colaborador,
    c.nome,
    ROUND(SUM(t.valor),2) AS gasto_total
  FROM `chat.transacoes_beneficios` t
  JOIN `chat.colaboradores` c USING (id_colaborador)
  JOIN `chat.beneficios` b USING (id_beneficio)
  GROUP BY b.nome_beneficio, c.id_colaborador, c.nome
)
SELECT *
FROM (
  SELECT
    nome_beneficio,
    id_colaborador,
    nome,
    gasto_total,
    RANK() OVER (PARTITION BY nome_beneficio ORDER BY gasto_total DESC) AS pos
  FROM gasto_colab
)
WHERE pos <= 10
ORDER BY nome_beneficio, pos;