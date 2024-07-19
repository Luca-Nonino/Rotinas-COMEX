import asyncio
import aiohttp
import aiofiles
import pandas as pd
from datetime import datetime
import os  # Importa os para verificar existência de arquivo e remoção
import time  # Importa time para rastrear o tempo de execução

async def async_retry_request(url, attempts=5, delay=10):
    # Cria uma sessão HTTP assíncrona para tentar fazer requisições com re-tentativas
    async with aiohttp.ClientSession() as session:
        for i in range(2):  # Executa duas tentativas completas
            for attempt in range(attempts):
                try:
                    # Realiza a requisição HTTP GET
                    async with session.get(url, ssl=False) as response:
                        response.raise_for_status()
                        return await response.read()
                except aiohttp.ClientError as e:
                    # Gerencia erros e tenta novamente após um delay caso ainda haja tentativas restantes
                    print(f"Tentativa {attempt+1} de loop {i+1} falhou, erro: {e}")
                    if attempt < attempts - 1:
                        await asyncio.sleep(delay)
            if i == 0:  # Apenas log se não for a última tentativa entre os loops
                print(f"Reiniciando tentativas após {attempts} falhas.")
        raise ConnectionError(f"Excedido o número máximo de tentativas após {attempts * 2} tentativas.")

async def download_and_filter_data(years, ncm_codes, data_type, base_url):
    # Timer inicial para métricas de desempenho
    start_time = time.time()
    final_dfs = []
    for year in years:
        file_path = f"data/raw/{data_type}_{year}.csv"
        url = f"{base_url}{data_type}_{year}.csv"
        try:
            # Faz download dos dados usando re-tentativas
            data = await async_retry_request(url)
            # Salva os dados em arquivo CSV
            async with aiofiles.open(file_path, 'wb') as file:
                await file.write(data)
            print(f"Arquivo baixado salvo em: {file_path}")
            # Carrega o CSV para filtrar dados
            df = pd.read_csv(file_path, delimiter=';', encoding='latin1')
            df_filtered = df[df['CO_NCM'].astype(str).isin(ncm_codes)]
            final_dfs.append(df_filtered)
        except Exception as e:
            # Gerencia exceções e remove arquivos parciais
            print(f"Falha ao processar {file_path}: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
            continue

    if final_dfs:
        # Concatena todos os dataframes filtrados em um único dataframe
        final_df = pd.concat(final_dfs, ignore_index=True)
        consolidated_file_path = f"data/raw/{data_type}_final.csv"
        final_df.to_csv(consolidated_file_path, index=False)
        print(f"Dados finais para {data_type} salvos em {consolidated_file_path}")
    for year in years:
        file_path = f"data/raw/{data_type}_{year}.csv"
        if os.path.exists(file_path):
            os.remove(file_path)
    # Timer final para métricas de desempenho
    end_time = time.time()
    print(f"Tempo de execução para download e filtro de dados: {end_time - start_time} segundos")

async def process_auxiliary_tables(url, output_dir):
    # Timer inicial para métricas de desempenho
    start_time = time.time()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    temp_excel_path = "temp_aux_tables.xlsx"
    # Faz download dos dados usando re-tentativas
    data = await async_retry_request(url)
    async with aiofiles.open(temp_excel_path, 'wb') as file:
        await file.write(data)
    # Lê os dados do Excel e grava em CSV segundo diretório de saída e nome do arquivo
    df = pd.read_excel(temp_excel_path, sheet_name=None)
    for name, sheet in df.items():
        if name == 'INDEX':
            continue
        output_csv_path = f"{output_dir}/aux_{name}.csv"
        async with aiofiles.open(output_csv_path, mode='w', encoding='utf-8') as csvfile:
            await csvfile.write(sheet.to_csv(None, index=False))
    # print(f"CSV Salvo: {output_csv_path}")
    os.remove(temp_excel_path)
    # Timer final para métricas de desempenho
    end_time = time.time()
    print(f"Tempo de execução para processamento de tabelas auxiliares: {end_time - start_time} segundos")

async def main():
    # Timer geral inicial para o programa
    start_time = time.time()
    years = [str(year) for year in range(datetime.now().year - 1, datetime.now().year+1)]
    ncm_codes_to_keep = [
        '10051000', '10059010', '10059090', '12010010', '12010090',
        '12011000', '12019000', '15071000', '15079011', '15079019',
        '15079090', '23040010', '23040090', '10011100', '10011090',
        '10011900', '10019010', '10019090', '10019100', '10019900',
        '11010010', '11010020'
    ]
    base_url = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/"
    aux_url = "https://balanca.economia.gov.br/balanca/bd/tabelas/TABELAS_AUXILIARES.xlsx"
    output_dir = "data/auxiliar"
    # Executa tarefas assíncronas em paralelo
    await asyncio.gather(
        download_and_filter_data(years, ncm_codes_to_keep, 'EXP', base_url),
        download_and_filter_data(years, ncm_codes_to_keep, 'IMP', base_url),
        process_auxiliary_tables(aux_url, output_dir)
    )
    # Timer geral final para o programa
    end_time = time.time()
    print(f"Tempo total de execução do programa: {end_time - start_time} segundos")

if __name__ == "__main__":
    asyncio.run(main())