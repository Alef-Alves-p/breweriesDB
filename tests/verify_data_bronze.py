import pytest
import json
from pathlib import Path
from tempfile import TemporaryDirectory

@pytest.fixture
def sample_json():
    """Cria um JSON de exemplo."""
    return [{"name": "Brewery A", "state": "CA"}, {"name": "Brewery B", "state": "NY"}]

def verify_data_bronze(sample_parquet, capsys):
    """Testa a verificação da camada bronze."""
    with TemporaryDirectory() as tmp_dir:
        file_path = Path(tmp_dir) / "bronze.parquet"
        sample_parquet.to_parquet(file_path)

        # Chama a função de verificação para a camada bronze (sem passar 'capsys' aqui)
        verify_data_bronze(file_path)

        # Captura a saída do console dentro do teste, não na função de verificação
        captured = capsys.readouterr()
        
        # Verifique se os dados da camada bronze estão corretos
        assert "Brewery A" in captured.out
        assert "CA" in captured.out
        assert "micro" in captured.out
