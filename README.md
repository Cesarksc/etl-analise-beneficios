# Projeto de Análise e Pipeline de Dados

Este repositório apresenta um pipeline ETL robusto, análise exploratória, engenharia de features e modelagem preditiva para KPIs de benefícios, simulando um desafio real de negócio.

---

## Objetivo

Construir um fluxo completo de dados, desde a extração (BigQuery ou arquivos locais), processamento, geração de indicadores e visualizações, até a exportação de resultados para uso analítico e de negócio.

## Estrutura do Projeto

```
├── src/                # Código-fonte do ETL e utilitários
├── notebooks/          # Notebooks de análise e modelagem
├── data/raw/           # Dados brutos (exportados do BigQuery)
├── data/processed/     # Dados processados (CSVs gerados pelo ETL)
├── sql/                # Queries SQL utilizadas
├── requirements.txt    # Dependências do projeto
└── README.md           # Este arquivo
```

## Pré-requisitos

- Python 3.8+
- Conta Google Cloud (opcional, para rodar com BigQuery)
- Variáveis de ambiente (.env) ou arquivos em `data/raw`

## Como Executar

1. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Configure o .env** (opcional, para rodar com BigQuery):
   ```
   PROJECT_ID=seu-projeto
   DATASET=seu-dataset
   BQ_LOCATION=US
   ```
   Se não houver `.env`, o pipeline tentará rodar usando arquivos em `data/raw`.

3. **Execute o ETL:**
   ```bash
   python src/main.py
   ```
   Os arquivos CSV processados serão gerados em `data/processed/`.

4. **Abra o notebook:**
   - Navegue até `notebooks/01_exploracao_modelo.ipynb` para análise exploratória, visualização e modelagem preditiva.

## Principais Tecnologias

- Python (pandas, numpy, matplotlib, scikit-learn)
- Google BigQuery (extração de dados)
- Jupyter Notebook
- Logging e tratamento de erros

## Funcionalidades

- Pipeline ETL robusto, modular e com logging
- Análise exploratória de KPIs de benefícios
- Engenharia de features temporais
- Modelos preditivos (baseline, regressão linear, random forest)
- Visualizações profissionais e exportação de resultados