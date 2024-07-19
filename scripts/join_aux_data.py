import pandas as pd
import os
import csv

def merge_auxiliary_tables(input_table_path, output_table_path):
    # Carrega o DataFrame final
    final_df = pd.read_csv(input_table_path)

    # Garante que CO_ANO e CO_MES sejam strings e concatena para formar DATA
    final_df['DATA'] = final_df['CO_ANO'].astype(str) + '-' + final_df['CO_MES'].astype(str) + '-01'

    # Descarta CO_ANO e CO_MES após o cálculo de DATA
    final_df.drop(['CO_ANO', 'CO_MES'], axis=1, inplace=True)

    # Garante que CO_NCM seja do tipo string antes de mapear
    final_df['CO_NCM'] = final_df['CO_NCM'].astype(str)

    # Define o caminho base para as tabelas auxiliares
    base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, 'data/auxiliar')

    # Fusão AUX 10
    aux10_df = pd.read_csv(os.path.join(base_path, 'aux_10.csv'), index_col='CO_PAIS')
    final_df = pd.merge(final_df, aux10_df[['CO_PAIS_ISOA3']], on='CO_PAIS', how='left')

    # Fusão AUX 14
    aux14_df = pd.read_csv(os.path.join(base_path, 'aux_14.csv'), index_col='CO_VIA')
    final_df = pd.merge(final_df, aux14_df[['NO_VIA']], on='CO_VIA', how='left')

    # Fusão AUX 15
    aux15_df = pd.read_csv(os.path.join(base_path, 'aux_15.csv'), index_col='CO_URF')
    final_df = pd.merge(final_df, aux15_df[['NO_URF']], on='CO_URF', how='left')

    # Substitui CO_NCM por COD_COMM usando o arquivo cod_comms.csv
    cod_mapping_path = os.path.join(base_path, 'cod_comms.csv')
    with open(cod_mapping_path, 'r') as file:
        reader = csv.reader(file)
        cod_mapping = {row[0]: row[1] for row in reader}
    final_df['COD_COMM'] = final_df['CO_NCM'].map(cod_mapping)
    final_df.drop('CO_NCM', axis=1, inplace=True)

    # Substitui NO_URF por COD_URF usando o arquivo cod_portos.csv
    cod_urf_mapping_path = os.path.join(base_path, 'cod_portos.csv')
    cod_urf_mapping = pd.read_csv(cod_urf_mapping_path)
    final_df = pd.merge(final_df, cod_urf_mapping, on='NO_URF', how='left')
    final_df.drop('NO_URF', axis=1, inplace=True)

    # Descarta colunas especificadas
    final_df.drop(['CO_UNID', 'CO_PAIS', 'CO_VIA', 'CO_URF', 'QT_ESTAT', 'NO_VIA'], axis=1, inplace=True)

    # Renomeia colunas com base no arquivo
    if 'EXP' in output_table_path:
        final_df.rename(columns={'KG_LIQUIDO': 'KGL', 'VL_FOB': 'FOB'}, inplace=True)
    elif 'IMP' in output_table_path:
        final_df.rename(columns={'KG_LIQUIDO': 'KGL', 'VL_FOB': 'FOB', 'VL_FRETE': 'VLF', 'VL_SEGURO': 'VLS'}, inplace=True)

    # Reordena as colunas
    value_columns = [col for col in final_df.columns if col not in ['DATA', 'COD_COMM', 'SG_UF_NCM', 'CO_PAIS_ISOA3', 'COD_URF']]
    final_df = final_df[['DATA', 'COD_COMM', 'SG_UF_NCM', 'CO_PAIS_ISOA3', 'COD_URF'] + value_columns]

    # Salva o DataFrame final
    final_df.to_csv(output_table_path, index=False)
    print(f"Dados mesclados salvos em {output_table_path}")

# Define o caminho base para as pastas de dados brutos e processados
base_path = os.path.dirname(os.path.abspath(__file__))
raw_path = os.path.join(base_path, os.pardir, 'data/raw')
processed_path = os.path.join(base_path, os.pardir, 'data/processed')

# Garante que a pasta processada exista
os.makedirs(processed_path, exist_ok=True)

# Chama a função para as tabelas finais EXP e IMP
merge_auxiliary_tables(os.path.join(raw_path, 'EXP_final.csv'), os.path.join(processed_path, 'EXP_final_processed.csv'))
merge_auxiliary_tables(os.path.join(raw_path, 'IMP_final.csv'), os.path.join(processed_path, 'IMP_final_processed.csv'))
