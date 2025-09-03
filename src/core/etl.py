import logging
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery_storage
from .config import GlobalConfig

class KpiETLPipeline:
    """Executa queries de KPI no BigQuery e exporta CSVs.

    Args:
        user (str): Usuário do sistema.
        project_id (str): ID do projeto GCP.
        dataset (str): Nome do dataset BigQuery.
        bigquery_location (str): Região do BigQuery.
        project_root (Path): Caminho raiz do projeto.
    """
    def __init__(self, user: str, project_id: str, dataset: str, bigquery_location: str, project_root: Path):
        logging.info("Inicializando KpiETLPipeline...")
        self.user = user
        self.project_id = project_id
        self.dataset = dataset
        self.bigquery_location = bigquery_location
        self.project_root = project_root
        if not self.project_id:
            logging.error("PROJECT_ID não definido! Verifique seu arquivo .env ou variáveis de ambiente.")
            raise ValueError("PROJECT_ID não definido. Verifique seu arquivo .env ou variáveis de ambiente.")
        logging.info(f"PROJECT_ID carregado: {self.project_id}")
        self.bigquery_client = bigquery.Client(project=self.project_id)
        logging.info("BigQuery Client criado.")
        self.bigquery_storage_client = bigquery_storage.BigQueryReadClient()
        logging.info("BigQuery Storage Client criado.")
        self.sql_directory = self.project_root / "sql"
        self.processed_data_dir = self.project_root / "data" / "processed"
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Diretório de SQL: {self.sql_directory}")
        logging.info(f"Diretório de saída dos CSVs: {self.processed_data_dir}")

    def _read_sql_file(self, relative_path: str) -> str:
        """Lê e formata um arquivo SQL.

        Args:
            relative_path (str): Caminho relativo do arquivo SQL.

        Returns:
            str: SQL formatado.

        Raises:
            FileNotFoundError: Se o arquivo não existir.
        """
        sql_file = self.sql_directory / relative_path
        logging.info(f"Lendo arquivo SQL: {sql_file}")
        if not sql_file.exists():
            logging.error(f"Arquivo SQL não encontrado: {sql_file}")
            raise FileNotFoundError(f"Arquivo SQL não encontrado: {sql_file}")
        sql = sql_file.read_text(encoding="utf-8")
        logging.debug(f"Conteúdo do SQL: {sql[:300]}")
        sql_final = sql.format(PROJECT_ID=self.project_id, DATASET=self.dataset)
        logging.info(f"SQL lido e formatado: {sql_final[:200]}...")
        return sql_final

    def _run_bigquery(self, sql: str) -> pd.DataFrame:
        """Executa uma query SQL no BigQuery e retorna um DataFrame.

        Args:
            sql (str): Query SQL a ser executada.

        Returns:
            pd.DataFrame: Resultado da query.
        """
        logging.info(f"Executando query no BigQuery (location={self.bigquery_location})...")
        job = self.bigquery_client.query(sql, location=self.bigquery_location)
        logging.info("Query enviada. Aguardando resultado...")
        df = job.to_dataframe(bqstorage_client=self.bigquery_storage_client)
        logging.info(f"Query retornou {len(df)} linhas e {len(df.columns)} colunas.")
        logging.debug(f"Colunas retornadas: {list(df.columns)}")
        return df

    def _save_dataframe_to_csv(self, dataframe: pd.DataFrame, filename: str) -> Path:
        """Salva um DataFrame como CSV no diretório de dados processados.

        Args:
            dataframe (pd.DataFrame): Dados a serem salvos.
            filename (str): Nome do arquivo de saída.

        Returns:
            Path: Caminho do arquivo salvo.
        """
        output_path = self.processed_data_dir / filename
        logging.info(f"Salvando DataFrame em CSV: {output_path}")
        dataframe.to_csv(output_path, index=False, encoding="utf-8")
        logging.info(f"CSV salvo: {output_path} | linhas={len(dataframe)} colunas={len(dataframe.columns)}")
        return output_path

    def export_monthly_department_expense_kpi(self) -> Path:
        """Exporta o KPI de gasto mensal por departamento.

        Returns:
            Path: Caminho do arquivo CSV gerado.
        """
        logging.info("Exportando KPI: Gasto mensal por departamento...")
        sql = self._read_sql_file("01_kpi_gasto_mensal_departamento/01_beneficios.sql")
        logging.debug(f"SQL para gasto mensal: {sql[:300]}")
        df = self._run_bigquery(sql)
        logging.debug(f"Primeiras linhas do DataFrame: {df.head(3).to_dict()}")
        return self._save_dataframe_to_csv(df, "kpi_monthly_department_expense.csv")

    def export_top10_employee_by_benefit_kpi(self) -> Path:
        """Exporta o KPI de top 10 colaboradores por benefício.

        Returns:
            Path: Caminho do arquivo CSV gerado.
        """
        logging.info("Exportando KPI: Top 10 colaboradores por benefício...")
        sql = self._read_sql_file("02_kpi_ranking_colaboradores_por_benefcio/02_top_colaboradores.sql")
        logging.debug(f"SQL para top 10 colaboradores: {sql[:300]}")
        df = self._run_bigquery(sql)
        logging.debug(f"Primeiras linhas do DataFrame: {df.head(3).to_dict()}")
        return self._save_dataframe_to_csv(df, "kpi_top10_employee_by_benefit.csv")

    def export_3month_moving_avg_department_kpi(self) -> Path:
        """Exporta o KPI de média móvel de 3 meses por departamento.

        Returns:
            Path: Caminho do arquivo CSV gerado.
        """
        logging.info("Exportando KPI: Média móvel 3 meses por departamento...")
        sql = self._read_sql_file("03_kpi_media_movel_de_3_meses_por_departamento/03_media_movel.sql")
        logging.debug(f"SQL para média móvel: {sql[:300]}")
        df = self._run_bigquery(sql)
        logging.debug(f"Primeiras linhas do DataFrame: {df.head(3).to_dict()}")
        return self._save_dataframe_to_csv(df, "kpi_3month_moving_avg_department.csv")

    def run_pipeline(self) -> None:
        """Executa o pipeline ETL completo (todos os KPIs)."""
        logging.info(f"Iniciando pipeline ETL: User={self.user} | Project={self.project_id} | Dataset={self.dataset} | Location={self.bigquery_location}")
        self.export_monthly_department_expense_kpi()
        self.export_top10_employee_by_benefit_kpi()
        self.export_3month_moving_avg_department_kpi()
        logging.info("ETL pipeline finalizado com sucesso.")
