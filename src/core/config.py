import logging
import os
import locale
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

class GlobalConfig:
    """Gerencia configuração global do projeto (variáveis de ambiente, logging, locale, etc).

    Args:
        locale_setting (str): Locale para datas.
        dev_mode (bool): Se True, loga também no console.
        env_path (Optional[str]): Caminho customizado para .env.
        log_dir (Optional[str]): Caminho customizado para logs.
        log_file_name (str): Nome do arquivo de log.
    """
    def __init__(self, *, locale_setting: str = "pt_BR.UTF-8", dev_mode: bool = False, env_path: Optional[str] = None, log_dir: Optional[str] = None, log_file_name: str = "run.log") -> None:
        self.project_root = Path(__file__).resolve().parents[2]
        self.env_path = Path(env_path) if env_path else (self.project_root / ".env")
        self.fallback_to_raw = False
        try:
            if not self.env_path.exists():
                raise FileNotFoundError(f"Arquivo .env não encontrado em {self.env_path}")
            load_dotenv(dotenv_path=self.env_path, override=True)
        except Exception as e:
            print(f"[AVISO] Não foi possível carregar o .env: {e}\nRodando em modo local, usando arquivos de data/raw.")
            logging.warning(f"Não foi possível carregar o .env: {e}. Rodando em modo local, usando arquivos de data/raw.")
            self.fallback_to_raw = True
        self.log_dir = Path(log_dir) if log_dir else (self.project_root / "logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_file = self.log_dir / log_file_name
        handlers = [logging.FileHandler(log_file, encoding="utf-8")]
        if dev_mode:
            handlers.append(logging.StreamHandler())
        root_logger = logging.getLogger()
        if root_logger.hasHandlers():
            root_logger.handlers.clear()
        logging.basicConfig(
            level=logging.DEBUG if dev_mode else logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=handlers,
        )
        try:
            locale.setlocale(locale.LC_TIME, locale_setting)
        except locale.Error:
            logging.warning(f"Não foi possível setar locale para {locale_setting}")
        self._cfg_bq = {
            "PROJECT_ID": os.getenv("PROJECT_ID", "").strip(),
            "DATASET": os.getenv("DATASET", "").strip(),
            "BQ_LOCATION": os.getenv("BQ_LOCATION", "US").strip(),
        }
        logging.info(f"Configs carregadas. env={self.env_path} logs={self.log_dir} fallback_to_raw={self.fallback_to_raw}")

    @staticmethod
    def get_system_user() -> str:
        """Obtém o usuário do sistema operacional.

        Returns:
            str: Nome do usuário.
        """
        return os.getenv("USERNAME") or os.getenv("USER") or "unknown"

    def get_bigquery_config(self) -> dict:
        """Retorna as configurações do BigQuery carregadas.

        Returns:
            dict: Dicionário com PROJECT_ID, DATASET, BQ_LOCATION.
        """
        return self._cfg_bq.copy()
