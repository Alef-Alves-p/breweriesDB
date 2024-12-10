import pandas as pd
import logging
from pathlib import Path

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_aggregated_data(input_path, output_path, engine='pyarrow'):
    """Carrega dados da camada prata e cria agregações na camada ouro."""
    if not input_path.exists():
        logger.error(f"A pasta de entrada não foi encontrada em: {input_path}")
        return False

    try:
        # Lê todos os arquivos Parquet na camada prata e os concatena em um único DataFrame
        parquet_files = list(input_path.glob("*.parquet"))
        if not parquet_files:
            logger.warning("Nenhum arquivo Parquet encontrado na pasta de entrada.")
            return False

        # Carrega e concatena os arquivos Parquet
        data_frames = []
        for file in parquet_files:
            try:
                df = pd.read_parquet(file, engine=engine)
                if df.empty:
                    logger.warning(f"Arquivo {file} está vazio e será ignorado.")
                else:
                    data_frames.append(df)
                    logger.info(f"Arquivo {file} carregado com {len(df)} registros.")
            except Exception as e:
                logger.error(f"Erro ao carregar o arquivo {file}: {e}")
        
        if not data_frames:
            logger.error("Nenhum dado válido foi carregado dos arquivos Parquet.")
            return False

        data = pd.concat(data_frames, ignore_index=True)
        logger.info(f"Dados combinados da camada prata. Total de registros: {len(data)}")

        # Cria agregações da contagem de cervejarias por tipo e localização
        if 'brewery_type' not in data.columns or 'state' not in data.columns:
            logger.error("Colunas necessárias ('brewery_type', 'state') não estão presentes nos dados.")
            return False

        aggregated_data = (
            data.groupby(['brewery_type', 'state'])
            .size()
            .reset_index(name='count')
        )
        logger.info(f"Agregação criada. Número de registros agregados: {len(aggregated_data)}")

        # Salva os dados agregados na camada ouro
        output_path.mkdir(parents=True, exist_ok=True)
        output_file_path = output_path / "aggregated_breweries.parquet"
        aggregated_data.to_parquet(output_file_path, index=False, engine=engine)
        logger.info(f"Dados agregados salvos na camada ouro em: {output_file_path}")

        return True

    except pd.errors.EmptyDataError:
        logger.warning("Um ou mais arquivos de entrada estão vazios.")
        return False
    except Exception as e:
        logger.error(f"Erro no carregamento: {e}")
        return False

if __name__ == "__main__":
    SILVER_PATH = Path("data/silver")
    GOLD_PATH = Path("data/gold")

    if load_aggregated_data(SILVER_PATH, GOLD_PATH):
        logger.info("Carregamento de dados concluído com sucesso.")
    else:
        logger.error("O carregamento de dados falhou.")
