# KPI – Ranking de colaboradores por benefício (Top 10)

## Objetivo
Identificar os **colaboradores que mais gastam em cada benefício**.  
Esse tipo de ranking ajuda a entender padrões de consumo, identificar **outliers** e apoiar **políticas internas de uso**.

## Query (BigQuery)
Arquivo: `sql/02_kpi_ranking_colaboradores_por_benefcio/02_top_colaboradores.sql`  
Tabelas usadas:  
- `chat.transacoes_beneficios` (fato)  
- `chat.colaboradores` (dim)  
- `chat.beneficios` (dim)

### Como rodar
```sql
WITH gasto_colab AS (
  SELECT
    b.nome_beneficio,
    c.id_colaborador,
    c.nome,
    ROUND(SUM(t.valor)2) AS gasto_total
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
```

## Por que está assim?
- **CTE (`WITH gasto_colab`)**: facilita a leitura consolidando o gasto por colaborador e benefício.  
- **Window function `RANK() OVER (PARTITION BY ...)`**: cria ranking **dentro de cada benefício**.  
- **Filtro `pos <= 10`**: retorna apenas o **Top 10** colaboradores em cada categoria de benefício.  
> Obs.: usei CTE + função de janela também para **demonstrar conhecimento técnico** (mesmo que houvesse outras formas).

## Métrica gerada
- `gasto_total`: soma dos valores por colaborador em cada benefício.  
- `pos`: posição no ranking (1 = colaborador que mais gastou).

## Campos de saída
- `nome_beneficio`  
- `id_colaborador`  
- `nome`  
- `gasto_total`  
- `pos`

## Exemplos de uso prático
- Identificar **superusuários** de benefícios.  
- Analisar padrões de uso por cargo/área (cruzando com tabela de colaboradores).  
- Subsidiar **políticas de limite de gasto** ou incentivos específicos.

