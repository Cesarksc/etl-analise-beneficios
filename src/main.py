"""
Ponto de entrada do pipeline de dados: executa ETL BigQuery ou processamento local.

Este módulo inicializa as configurações globais e executa o pipeline ETL (BigQuery) ou o processamento local dos arquivos brutos, conforme a configuração encontrada.
"""
from core.config import GlobalConfig
from core.etl import KpiETLPipeline
from core.local_pipeline import run_local_pipeline

def main() -> None:
    """Executa o pipeline ETL principal do projeto.

    Inicializa as configurações globais, carrega variáveis de ambiente (ou ativa o modo local caso o .env não seja encontrado), instancia o pipeline de ETL ou executa o fluxo local de geração dos KPIs.
    """
    config = GlobalConfig(dev_mode=False)
    user = config.get_system_user()
    bq_config = config.get_bigquery_config()
    if config.fallback_to_raw or not bq_config["PROJECT_ID"]:
        run_local_pipeline(config.project_root)
    else:
        pipeline = KpiETLPipeline(
            user=user,
            project_id=bq_config["PROJECT_ID"],
            dataset=bq_config["DATASET"],
            bigquery_location=bq_config["BQ_LOCATION"],
            project_root=config.project_root,
        )
        pipeline.run_pipeline()

if __name__ == "__main__":
    main()