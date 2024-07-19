### README.md
```markdown
# Documentação do Projeto de Pipeline de Dados do COMEX

## Descrição Geral

Este projeto implementa uma pipeline automatizada para processar e analisar dados do Sistema Comex Stat, que são essenciais para entender as tendências de exportação e importação do Brasil. A pipeline extrai dados, transforma-os em formatos utilizáveis para análise, e gera arquivos IPV para visualizações e relatórios internos.

## Estrutura do Projeto

O projeto é organizado nas seguintes pastas e arquivos principais:

- **data/**: Contém todos os dados brutos, processados e os logs.
  - **auxiliar/**: Armazena dados auxiliares utilizados no processamento.
  - **ipvs/**: Diretório para os arquivos IPV gerados.
  - **logs/**: Guarda logs de erros e de atualização.
  - **processed/**: Armazena dados já processados.
  - **raw/**: Dados brutos coletados.
- **scripts/**: Scripts Python para tarefas específicas de processamento de dados.
  - **fetch_data.py**: Script para a coleta de dados.
  - **join_aux_data.py**: Script para juntar dados com tabelas auxiliares.
  - **generate_ipvs.py**: Script para transformar os dados em arquivos IPV.
- **main.py**: Script principal que coordena as operações de coleta e processamento de dados.

## Configuração e Instalação

Antes de iniciar, certifique-se de que o Python 3.8 ou superior está instalado. Clone o repositório no seu ambiente de desenvolvimento e instale as dependências listadas em `requirements.txt` usando o comando:

```bash
pip install -r requirements.txt
```

## Execução do Projeto

Para executar o projeto, use o comando a partir da linha de comando:

```bash
python main.py
```

Este script irá coordenar as seguintes operações:

1. Coleta de dados das fontes especificadas para os últimos anos.
2. Processamento dos dados JSON e CSV em formatos consolidados.
3. Geração de arquivos IPV para análise interna.

Os arquivos IPV gerados são armazenados em `data/ipvs/`, organizados por série e tipo.

## Logs

Os logs de erros são salvos em `data/logs/error_logs.txt`, e os logs de atualização em `data/logs/update_log.json`, que facilitam o rastreamento das operações e identificação de problemas.

## Limpeza de Dados

Após cada ciclo de execução, o script `main.py` inclui uma rotina para limpar os diretórios de dados brutos e processados para evitar a acumulação de arquivos desnecessários e manter a organização do sistema de arquivos.
`