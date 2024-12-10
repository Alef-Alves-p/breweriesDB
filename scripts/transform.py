import pandas as pd
import logging
from pathlib import Path

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_data(input_path, output_path, engine='pyarrow'):
    """Transforma os dados da camada bronze e salva na camada prata."""
    if not input_path.exists():
        logger.error(f"Arquivo de entrada não encontrado em: {input_path}")
        return False

    try:
        # Lê os dados da camada bronze
        data = pd.read_parquet(input_path, engine=engine)
        logger.info(f"Dados lidos do arquivo de entrada: {input_path}. Número de registros: {len(data)}")

        # Remove duplicatas e registros vazios
        data.drop_duplicates(inplace=True)
        data.dropna(subset=['name', 'state'], inplace=True)
        logger.info("Registros duplicados e vazios removidos.")

        # Verifica se a coluna 'name' existe antes de transformá-la
        if 'name' in data.columns:
            data['name'] = data['name'].str.upper()
            logger.info("Nomes de cervejarias transformados para maiúsculas.")
        else:
            logger.warning("Coluna 'name' não encontrada nos dados.")

        # Verifica se todos os estados estão presentes
        unique_states = data['state'].dropna().unique()
        logger.info(f"Estados encontrados nos dados transformados: {unique_states}")

        # Cria a pasta de saída, se não existir
        output_path.mkdir(parents=True, exist_ok=True)

        # Particiona os dados por estado e salva como arquivos Parquet
        for state, df_state in data.groupby('state'):
            state_path = output_path / f"{state.replace(' ', '_')}.parquet"
            df_state.to_parquet(state_path, index=False, engine=engine)
            logger.info(f"Dados transformados e salvos em: {state_path}")

        return True

    except pd.errors.EmptyDataError:
        logger.warning(f"O arquivo {input_path} está vazio.")
        return False
    except KeyError as e:
        logger.error(f"Erro de chave ao processar os dados: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro na transformação: {e}")
        return False

if __name__ == "__main__":
    bronze_path = Path("data/bronze") / "breweries.parquet"
    silver_path = Path("data/silver")
    if transform_data(bronze_path, silver_path):
        logger.info("Transformação concluída com sucesso.")
    else:
        logger.error("A transformação falhou.")
