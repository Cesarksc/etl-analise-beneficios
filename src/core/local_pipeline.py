import logging
from pathlib import Path
import pandas as pd

def run_local_pipeline(project_root: Path):
    """Processa arquivos brutos de data/raw e gera arquivos KPI em data/processed.

    Args:
        project_root (Path): Caminho raiz do projeto.
    """
    logging.warning("Rodando em modo local: processando arquivos brutos de data/raw para data/processed.")
    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    try:
        trans = pd.read_csv(raw_dir / "transacoes_beneficios.csv", sep=';')
        colabs = pd.read_csv(raw_dir / "colaboradores.csv", sep=';')
        deptos = pd.read_csv(raw_dir / "departamentos.csv", sep=';')
        trans_colab = trans.merge(colabs[["id_colaborador", "id_departamento"]], on="id_colaborador", how="left")
        trans_colab_dept = trans_colab.merge(deptos, on="id_departamento", how="left")
        trans_colab_dept["mes"] = pd.to_datetime(trans_colab_dept["data"], errors="coerce").dt.to_period("M").dt.to_timestamp()
        df1 = (
            trans_colab_dept.groupby(["mes", "nome_departamento"], as_index=False)["valor"].sum()
            .rename(columns={"valor": "gasto_total"})
            .sort_values(["mes", "nome_departamento"])
        )
        df1.to_csv(processed_dir / "kpi_monthly_department_expense.csv", index=False)
        logging.info("Arquivo gerado: kpi_monthly_department_expense.csv")
    except Exception as e:
        logging.error(f"Erro ao gerar kpi_monthly_department_expense.csv: {e}")
    try:
        df1 = pd.read_csv(processed_dir / "kpi_monthly_department_expense.csv")
        df1["mes"] = pd.to_datetime(df1["mes"])
        df1 = df1.sort_values(["nome_departamento", "mes"])
        df1["media_movel_3m"] = (
            df1.groupby("nome_departamento")["gasto_total"]
            .transform(lambda x: x.rolling(window=3, min_periods=1).mean())
        )
        df1.to_csv(processed_dir / "kpi_3month_moving_avg_department.csv", index=False)
        logging.info("Arquivo gerado: kpi_3month_moving_avg_department.csv")
    except Exception as e:
        logging.error(f"Erro ao gerar kpi_3month_moving_avg_department.csv: {e}")
    try:
        trans = pd.read_csv(raw_dir / "transacoes_beneficios.csv", sep=';')
        colabs = pd.read_csv(raw_dir / "colaboradores.csv", sep=';')
        benefs = pd.read_csv(raw_dir / "beneficios.csv", sep=';')
        trans_colab = trans.merge(colabs[["id_colaborador", "nome"]], on="id_colaborador", how="left")
        trans_colab_benef = trans_colab.merge(benefs, on="id_beneficio", how="left")
        df3 = (
            trans_colab_benef.groupby(["nome_beneficio", "id_colaborador", "nome"], as_index=False)["valor"].sum()
            .sort_values(["nome_beneficio", "valor"], ascending=[True, False])
        )
        df3["pos"] = df3.groupby("nome_beneficio").cumcount() + 1
        df3 = df3[df3["pos"] <= 10]
        df3.rename(columns={"nome": "nome_colaborador"}, inplace=True)
        df3.to_csv(processed_dir / "kpi_top10_employee_by_benefit.csv", index=False)
        logging.info("Arquivo gerado: kpi_top10_employee_by_benefit.csv")
    except Exception as e:
        logging.error(f"Erro ao gerar kpi_top10_employee_by_benefit.csv: {e}")
    print("[INFO] Pipeline local concluído. Arquivos KPI gerados em data/processed.")
