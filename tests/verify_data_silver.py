import pandas as pd
from pathlib import Path

def verify_data_silver(file_path, records_to_display=5):
    """Verifica e exibe registros do arquivo Parquet da camada prata."""
    file_path = Path(file_path)

    if not file_path.exists():
        print(f"Arquivo {file_path} não encontrado.")
        return

    try:
        data = pd.read_parquet(file_path)
        print(f"Os primeiros {records_to_display} registros do arquivo prata:")
        print(data.head(records_to_display))
    except Exception as e:
        print(f"Erro ao carregar o arquivo Parquet: {e}")

if __name__ == "__main__":
    verify_data_silver("data/silver/breweries.parquet")
