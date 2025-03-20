import requests
import zipfile
import io
import os
import pandas as pd
import logging
from datetime import datetime

# Configuração de logs
logging.basicConfig(
    filename='logs/coleta_dados.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def baixar_dados_dnit(ano, br, caminho_base="data/raw"):
    """Baixa e organiza dados do DNIT com tratamento completo de erros."""
    
    try:
        # Validação rigorosa das entradas
        if not isinstance(ano, int) or len(str(ano)) != 4 or ano < 2000 or ano > datetime.now().year:
            raise ValueError(f"Ano inválido: {ano}. Deve ser entre 2000 e {datetime.now().year}.")
        
        if not isinstance(br, int) or br <= 0 or br > 999:
            raise ValueError(f"BR inválida: {br}. Deve ser um número entre 1 e 999.")

        # Criação de diretórios com tratamento de erros
        caminho_br = os.path.join(caminho_base, str(ano), f"BR-{br}")
        caminho_zip = os.path.join(caminho_br, "arquivos_zip")
        caminho_csv = os.path.join(caminho_br, "arquivos_csv")
        
        try:
            os.makedirs(caminho_zip, exist_ok=True)
            os.makedirs(caminho_csv, exist_ok=True)
        except PermissionError as e:
            logging.critical(f"Permissão negada para criar diretórios: {e}")
            raise RuntimeError("Erro de permissão: verifique acesso às pastas.")
        except OSError as e:
            logging.critical(f"Erro ao criar diretórios: {e}")
            raise RuntimeError("Erro crítico no sistema de arquivos.")

        # Construção da URL
        url = f"https://servicos.dnit.gov.br/dadospnct/arquivos/pnct_{ano}_{br}.zip"
        nome_zip = f"pnct_{ano}_{br}.zip"
        caminho_zip_completo = os.path.join(caminho_zip, nome_zip)

        # Download com timeout e verificação de status
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            if 'application/zip' not in response.headers.get('Content-Type', ''):
                raise ValueError("O conteúdo baixado não é um arquivo ZIP válido.")

        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logging.error(f"Arquivo não encontrado: {url}")
                raise FileNotFoundError(f"Dados para BR-{br} ({ano}) não encontrados no servidor.")
            else:
                logging.error(f"Erro HTTP {response.status_code}: {e}")
                raise
        except requests.exceptions.Timeout:
            logging.error("Timeout ao acessar o servidor.")
            raise RuntimeError("Conexão com o servidor demorou muito.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de conexão: {e}")
            raise RuntimeError("Falha na conexão com a internet.")

        # Salvamento do ZIP
        try:
            with open(caminho_zip_completo, 'wb') as f:
                f.write(response.content)
            logging.info(f"ZIP salvo em: {caminho_zip_completo}")
        except IOError as e:
            logging.error(f"Erro ao salvar ZIP: {e}")
            raise RuntimeError("Erro ao salvar arquivo no computador.")

        # Processamento do ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                if not zip_ref.namelist():
                    raise zipfile.BadZipFile("Arquivo ZIP vazio ou corrompido.")
                
                csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                if not csv_files:
                    raise ValueError("Nenhum CSV encontrado no ZIP.")
                
                csv_no_zip = csv_files[0]
                zip_ref.extract(csv_no_zip, caminho_csv)
                caminho_csv_final = os.path.join(caminho_csv, csv_no_zip)

        except zipfile.BadZipFile as e:
            logging.error(f"ZIP corrompido: {e}")
            raise RuntimeError("Arquivo baixado está corrompido.")
        except (ValueError, IndexError) as e:
            logging.error(f"Erro no conteúdo do ZIP: {e}")
            raise RuntimeError("Formato inesperado do arquivo ZIP.")

        # Leitura do CSV
        try:
            df = pd.read_csv(caminho_csv_final, sep=';', encoding='latin1', on_bad_lines='warn')
            if df.empty:
                logging.warning("CSV vazio ou sem dados válidos.")
            return df
        
        except pd.errors.ParserError as e:
            logging.error(f"Erro ao ler CSV: {e}")
            raise RuntimeError("Formato inválido do arquivo CSV.")
        except UnicodeDecodeError as e:
            logging.error(f"Erro de codificação: {e}")
            raise RuntimeError("Problema com caracteres especiais no arquivo.")
        
    except Exception as e:
        logging.exception("Erro não tratado durante a execução:")
        raise

# Interface interativa com tratamento de erros
if __name__ == "__main__":
    print("=== Coletor de Dados DNIT ===")
    try:
        ano = int(input("Digite o ano (ex: 2023): "))
        br = int(input("Digite o número da BR (ex: 101): "))
        
        df = baixar_dados_dnit(ano, br)
        
        if df is not None:
            print("\n✅ Download concluído!")
            print(f"📁 Dados salvos em: data/raw/{ano}/BR-{br}")
            print("\n📊 Visualização dos dados:")
            print(df.head())
            
    except ValueError as e:
        print(f"\n❌ Erro de entrada: {e}")
    except FileNotFoundError as e:
        print(f"\n❌ {e} Verifique no site: https://servicos.dnit.gov.br/dadospnct/mapa")
    except RuntimeError as e:
        print(f"\n❌ Erro durante a execução: {e}")
    except Exception as e:
        print(f"\n⚠️ Erro inesperado: {e} Consulte o arquivo de logs.")
    finally:
        print("\nOperação finalizada.")
