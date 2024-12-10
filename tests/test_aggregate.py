import pandas as pd
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from scripts.load import load_aggregated_data  # Ajuste o import conforme necessário

@pytest.fixture
def sample_data():
    """Cria um DataFrame de exemplo para os testes."""
    data = {
        'type': ['micro', 'micro', 'regional', 'brewpub'],
        'state': ['CA', 'CA', 'TX', 'NY'],
    }
    return pd.DataFrame(data)

def aggregate_brewery_data(sample_data):
    """Testa a agregação de dados."""
    with TemporaryDirectory() as tmp_dir:
        input_path = Path(tmp_dir) / "input.parquet"
        output_path = Path(tmp_dir) / "aggregated.csv"

        # Salvar o DataFrame de exemplo no caminho temporário
        sample_data.to_parquet(input_path, index=False)

        # Executar a função
        aggregate_brewery_data(input_path, output_path)

        # Verificar se o arquivo de saída foi criado
        assert output_path.exists(), "O arquivo agregado não foi criado."

        # Ler o arquivo de saída e validar os dados
        result = pd.read_csv(output_path)
        expected = pd.DataFrame({
            'type': ['brewpub', 'micro', 'regional'],
            'state': ['NY', 'CA', 'TX'],
            'brewery_count': [1, 2, 1],
        })
        pd.testing.assert_frame_equal(result, expected, check_like=True)
