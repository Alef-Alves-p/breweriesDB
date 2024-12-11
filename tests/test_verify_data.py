import pytest
import pandas as pd
from tabulate import tabulate
from pathlib import Path
from tempfile import TemporaryDirectory
from verify_data_bronze import verify_data_bronze
from verify_data_silver import verify_data_silver
from verify_data_gold import verify_data_gold
from test_aggregate import aggregate_brewery_data


@pytest.fixture
def sample_parquet():
    """Cria um DataFrame de exemplo."""
    data = pd.DataFrame({
        "name": ["Brewery A", "Brewery B"],
        "state": ["CA", "NY"],
        "type": ["micro", "macro"],
    })
    return data


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


def test_verify_data_silver(sample_parquet: pd.DataFrame, capsys: pytest.CaptureFixture[str]):
    """Testa a verificação da camada prata."""
    with TemporaryDirectory() as tmp_dir:
        file_path = Path(tmp_dir) / "silver.parquet"
        sample_parquet.to_parquet(file_path)

        verify_data_silver(file_path)

        captured = capsys.readouterr()
        assert "Brewery A" in captured.out
        assert "CA" in captured.out


def test_verify_data_gold(sample_parquet: pd.DataFrame, capsys: pytest.CaptureFixture[str]):
    """Testa a verificação da camada ouro."""
    with TemporaryDirectory() as tmp_dir:
        file_path = Path(tmp_dir) / "gold.parquet"
        sample_parquet.to_parquet(file_path)

        verify_data_gold(file_path)

        captured = capsys.readouterr()
        assert "Brewery B" in captured.out
        assert "NY" in captured.out


def aggregate_brewery_data(sample_data: pd.DataFrame):
    """Testa a agregação de dados."""
    sample_data = pd.DataFrame({
        "type": ["brewpub", "micro", "micro", "regional"],
        "state": ["NY", "CA", "CA", "TX"],
    })

    with TemporaryDirectory() as tmp_dir:
        input_path = Path(tmp_dir) / "input.parquet"
        output_path = Path(tmp_dir) / "aggregated.csv"

        # Salvar o DataFrame de exemplo no caminho temporário
        sample_data.to_parquet(input_path, index=False)

        # Executa a agregação de dados
        aggregate_brewery_data(input_path, output_path)

        assert output_path.exists(), "O arquivo agregado não foi criado."

        result = pd.read_csv(output_path)
        expected = pd.DataFrame({
            'type': ['brewpub', 'micro', 'regional'],
            'state': ['NY', 'CA', 'TX'],
            'brewery_count': [1, 2, 1],
        })

        # Mostrar os dados em forma de tabela
        print("\nResultado (Output):")
        print(tabulate(result, headers="keys", tablefmt="grid", showindex=False))

        print("\nEsperado (Expected):")
        print(tabulate(expected, headers="keys", tablefmt="grid", showindex=False))

        # Validar os dados
        pd.testing.assert_frame_equal(result, expected, check_like=True)
