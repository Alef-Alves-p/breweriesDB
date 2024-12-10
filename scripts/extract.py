import requests
import pandas as pd
import logging
from pathlib import Path

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data(api_url, output_path, engine='pyarrow'):
    """Extrai dados da API Open Brewery DB e salva na camada bronze em formato Parquet."""
    try:
        # Inicializa uma sessão de requisições para melhor performance
        with requests.Session() as session:
            retries = requests.packages.urllib3.util.retry.Retry(
                total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504]
            )
            adapter = requests.adapters.HTTPAdapter(max_retries=retries)
            session.mount("https://", adapter)

            breweries = []
            page = 1
            while True:
                response = session.get(f"{api_url}?page={page}")
                response.raise_for_status()

                data = response.json()
                if not data:  # Interrompe se não houver mais dados
                    break

                breweries.extend(data)
                logger.info(f"Página {page} processada. Total de registros acumulados: {len(breweries)}")
                page += 1

        if not breweries:
            logger.warning("Nenhum dado foi extraído da API.")
            return False

        # Cria DataFrame e escreve em Parquet, com otimização de escrita
        df = pd.DataFrame(breweries).drop_duplicates()
        df.to_parquet(output_path, index=False, engine=engine)  # Uso de engine configurável

        logger.info(f"Dados extraídos e salvos em: {output_path}")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição para a API: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado na extração: {e}")
        return False

# Adiciona verificação para evitar execução indesejada em importações
if __name__ == "__main__":
    # Definindo caminho de saída e criando diretórios, se necessário
    output_path = Path("data/bronze") / "breweries.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)  # Cria diretórios pai se não existirem

    if extract_data("https://api.openbrewerydb.org/breweries", output_path):
        logger.info("Extração concluída com sucesso.")
    else:
        logger.error("A extração falhou.")
