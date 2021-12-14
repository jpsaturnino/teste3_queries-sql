# Teste 3 - Queries SQL

## Tarefas

- Baixar os arquivos dos últimos 2 anos no repositório público : http://ftp.dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/
- Queries de load: criar as queries para carregar o conteúdo dos arquivos obtidos nas tarefas de preparação num banco MySQL ou Postgres (Atenção ao encoding dos arquivos!)
- Montar uma query analítica que traga a resposta para as seguintes perguntas:
  ```
  Quais as 10 operadoras que mais tiveram despesas com 
  "EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR" no último trimestre?
  ```
  ```
  Quais as 10 operadoras que mais tiveram despesas com 
  µ"EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR" no último ano?
  ```
  -> Queries no arquivo [select_analitica.sql](https://github.com/jpsaturnino/teste3_queries-sql/blob/main/select_analitica.sql)

## Tecnologias

- Python
  - BeaultifulSoup
  - Pandas
  - Threading

## Dependencias

```
pip install -r requirements.txt
```
