import pandas as pd
import os
import glob
import logging
import csv
import time
import json
import datetime
import re

# Configuração do log detalhado
logging.basicConfig(filename='data/logs/ipvs_process.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definição dos caminhos
script_dir = os.path.abspath(os.path.dirname(__file__))
aux_table_dir = os.path.join(script_dir, '..', 'data', 'auxiliar')
input_dir = os.path.join(script_dir, '..', 'data', 'processed')
output_dir = os.path.join(script_dir, '..', 'data', 'ipvs')

# Conversão de códigos de países
country_conversion = {}
with open(os.path.join(aux_table_dir, 'country_conversion.csv'), mode='r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        country_conversion[row['<old>']] = row['<new>']

# Garantir a existência dos diretórios
def ensure_directories(series_types):
    for series_type in series_types:
        path = os.path.join(output_dir, series_type)
        os.makedirs(path, exist_ok=True)
        logging.info(f"Directory ensured: {path}")

# Carregar dados
def load_data(filename):
    path = os.path.join(input_dir, filename)
    try:
        data = pd.read_csv(path)
        logging.info(f"Data loaded from: {path}")
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {path}")
        return pd.DataFrame()

def process_and_save_data(series_df, data_df, output_dir, prefix, series_col, series_type):
    file_count = 0
    for index, row in series_df.iterrows():
        cod_comm = row['COD_COMM']
        series_value = row[series_col]
        filtered_data = data_df[(data_df['COD_COMM'] == cod_comm) & (data_df[series_col] == series_value)]
        if not filtered_data.empty:
            agg_cols = {'KGL': 'sum', 'FOB': 'sum'}
            if prefix == 'IMP':
                agg_cols.update({'VLF': 'sum', 'VLS': 'sum'})
            aggregated_data = filtered_data.groupby('DATA').agg(agg_cols).reset_index()
            if series_type == 'country_series':
                country_code = country_conversion.get(series_value, 'XX')
            else:
                country_code = series_value
            suffix = '_BR'
            aggregated_data['COD'] = f"COMEX:{cod_comm}_{prefix[:2]}_{country_code}{suffix}"
            output_file = os.path.join(output_dir, f"{cod_comm}_{prefix[:2]}_{country_code}{suffix}.ipv")
            aggregated_data.columns = ['<' + col + '>' for col in aggregated_data.columns]
            aggregated_data.to_csv(output_file, index=False)
            logging.info(f"Data processed and saved for {cod_comm} at {output_file}")
            file_count += 1
        else:
            logging.warning(f"No data found for {cod_comm} - {series_value}. Skipping.")
    return file_count


def consolidate_ipvs(series_type_dir, series_type):
    # Define paths and patterns for file types
    export_pattern = os.path.join(series_type_dir, f"???_EX_*.ipv")
    import_pattern = os.path.join(series_type_dir, f"???_IM_*.ipv")

    # Get all relevant files
    export_files = glob.glob(export_pattern)
    import_files = glob.glob(import_pattern)

    # Read and concatenate export files with ignoring headers
    df_exports = pd.concat([pd.read_csv(file, skiprows=1, names=['<DATA>', '<KGL>', '<FOB>', '<COD>']) for file in export_files])

    # Read and concatenate import files with ignoring headers
    df_imports = pd.concat([pd.read_csv(file, skiprows=1, names=['<DATA>', '<KGL>', '<FOB>', '<VLF>', '<VLS>', '<COD>']) for file in import_files])

    # Define output file names
    current_date = datetime.datetime.now()
    formatted_date = current_date.strftime('%Y_%m')
    export_filename = os.path.join(series_type_dir, f"{series_type}_exports_{formatted_date}.ipv")
    import_filename = os.path.join(series_type_dir, f"{series_type}_imports_{formatted_date}.ipv")

    # Save to new files
    df_exports.to_csv(export_filename, index=False)
    df_imports.to_csv(import_filename, index=False)

    # Remove older files
    for file in export_files + import_files:
        try:
            if not file.endswith(f"exports_{formatted_date}.ipv") and not file.endswith(f"imports_{formatted_date}.ipv"):
                os.remove(file)
        except FileNotFoundError as e:
            logging.error(f"Error removing file {file}: {str(e)}")

    print(f"Exportação de dados consolidada e salva em: {export_filename}")
    print(f"Importação de dados consolidada e salva em: {import_filename}")

def generate_wo_rows(file_path):
    # Load the data
    df = pd.read_csv(file_path)

    # Function to extract commodity code and series type
    def extract_details(cod):
        commodity_code = cod[6:9]
        series_type = cod[10:12]
        return commodity_code, series_type

    # Apply the function to extract data
    df['Commodity_Code'], df['Series_Type'] = zip(*df['<COD>'].apply(extract_details))

    # Define aggregation dictionary
    aggregation_dict = {'<KGL>': 'sum', '<FOB>': 'sum'}

    # Include VLF and VLS in aggregation if available
    if '<VLF>' in df.columns and '<VLS>' in df.columns:
        aggregation_dict.update({'<VLF>': 'sum', '<VLS>': 'sum'})

    # Aggregate data
    agg_df = df.groupby(['<DATA>', 'Commodity_Code', 'Series_Type']).agg(aggregation_dict).reset_index()

    # Create new <COD> format and append rows
    agg_df['<COD>'] = 'COMEX:' + agg_df['Commodity_Code'] + '_' + agg_df['Series_Type'] + '_WO_BR'
    agg_df.drop(['Commodity_Code', 'Series_Type'], axis=1, inplace=True)

    # Append and merge the original DataFrame with new aggregated rows
    df = pd.concat([df, agg_df], ignore_index=True)

    # Remove unnecessary columns for export and import
    df = df[['<DATA>', '<KGL>', '<FOB>', '<COD>'] + (['<VLF>', '<VLS>'] if '<VLF>' in df.columns else [])]

    # Save to the same file
    df.to_csv(file_path, index=False)
    print(f"Arquivo atualizado com linhas de visão global: {file_path}")

directory = "data\\ipvs"
def format_dates_in_files(directory):
    # Iterate through subfolders and files in the directory
    for subdir, dirs, files in os.walk(directory):
        for file in files:
            filepath = os.path.join(subdir, file)

            # Check if the file extension is .ipv
            if filepath.endswith(".ipv"):
                with open(filepath, 'r+') as f:
                    content = f.readlines()
                    f.seek(0)
                    for line in content:
                        # Match and format date in the <DATA> column
                        line = re.sub(r'(\d{4})-(\d{1})-', r'\1-0\2-', line)
                        f.write(line)
                    f.truncate()

# Função principal para orquestrar o processamento
def main():
    start_time = time.time()
    series_types = ['country_series', 'harbor_series', 'state_series']
    ensure_directories(series_types)
    exp_data = load_data('EXP_final_processed.csv')
    imp_data = load_data('IMP_final_processed.csv')
    series_files = {
        'country_series': 'country_series.csv',
        'harbor_series': 'harbor_series.csv',
        'state_series': 'state_series.csv'
    }
    file_count_total = 0
    for series_type, file_name in series_files.items():
        series_df = load_data(os.path.join(aux_table_dir, file_name))
        file_count_total += process_and_save_data(series_df, exp_data, os.path.join(output_dir, series_type), 'EXP', 'CO_PAIS_ISOA3' if series_type == 'country_series' else 'COD_URF' if series_type == 'harbor_series' else 'SG_UF_NCM', series_type)
        file_count_total += process_and_save_data(series_df, imp_data, os.path.join(output_dir, series_type), 'IMP', 'CO_PAIS_ISOA3' if series_type == 'country_series' else 'COD_URF' if series_type == 'harbor_series' else 'SG_UF_NCM', series_type)
    consolidate_ipvs('data/ipvs/country_series', 'country_series')
    current_date = datetime.datetime.now()
    formatted_date = current_date.strftime('%Y_%m')
    generate_wo_rows(f'data/ipvs/country_series/country_series_exports_{formatted_date}.ipv')
    generate_wo_rows(f'data/ipvs/country_series/country_series_imports_{formatted_date}.ipv')
    consolidate_ipvs('data/ipvs/harbor_series', 'harbor_series')
    consolidate_ipvs('data/ipvs/state_series', 'state_series')
    format_dates_in_files(output_dir)
    end_time = time.time()
    logging.info(f"Total files created: {file_count_total}")
    print(f"Total de códigos atualizados: {file_count_total}")
    logging.info(f"Tempo total de execução: {end_time - start_time:.2f} segundos")
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")

if __name__ == '__main__':
    main()