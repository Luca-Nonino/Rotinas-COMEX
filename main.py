import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import subprocess
import io
import warnings

# Suprime apenas o aviso InsecureRequestWarning
warnings.filterwarnings('ignore', category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

def check_data_update():
    with open('data/logs/update_log.json', 'r') as log_file:
        update_log = json.load(log_file)

    last_updated_month = int(update_log['LAST_UPDATED']['MONTH'])
    last_updated_year = int(update_log['LAST_UPDATED']['YEAR'])

    current_year = datetime.now().year
    url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{current_year}.csv"
    response = requests.get(url, verify=False)

    if response.status_code == 200:
        csv_data = response.text
        df = pd.read_csv(io.StringIO(csv_data), sep=';')
        next_month_rows = df[(df['CO_ANO'] == last_updated_year) & (df['CO_MES'] == last_updated_month + 1)]

        if not next_month_rows.empty:
            print("Novos dados disponíveis. Procedendo com a atualização.")
            return True
        else:
            print("Dados já atualizados para o mês corrente.")
            return False
    else:
        print("Falha ao buscar dados da URL.")
        return False

def run_pipeline_scripts():
    scripts = [
        'scripts/fetch_data.py',
        'scripts/join_aux_data.py',
        'scripts/generate_ipvs.py',
    ]

    success = True
    for script in scripts:
        try:
            subprocess.run(['python', script], check=True)
            print(f"Script {script} executado com sucesso.")
        except subprocess.CalledProcessError as e:
            print(f"Erro ao executar o script {script}: {e}")
            success = False
            break

    return success

def update_log_file():
    current_month = datetime.now().month - 1
    current_year = datetime.now().year

    with open('data/logs/update_log.json', 'r') as log_file:
        update_log = json.load(log_file)

    update_log['LAST_UPDATED']['MONTH'] = str(current_month)
    update_log['LAST_UPDATED']['YEAR'] = str(current_year)

    with open('data/logs/update_log.json', 'w') as log_file:
        json.dump(update_log, log_file, indent=2)

if __name__ == '__main__':
    start_time = datetime.now()
    if check_data_update():
        if run_pipeline_scripts():
            update_log_file()
        else:
            print("Execução do pipeline interrompida.")
    else:
        print("Não há novos dados disponíveis.")
    end_time = datetime.now()
    total_time_seconds = (end_time - start_time).total_seconds()
    print(f"Tempo total de execução do COMEX Pipeline: {total_time_seconds:.2f} segundos.")