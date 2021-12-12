from datetime import datetime
from os import remove
from bs4 import BeautifulSoup as bs
import requests
from zipfile import ZipFile
import pandas as pd

URL = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
FILETYPE = ".zip"


def get_soup(url: str) -> bs:
    """
    Retorna o conteudo da pagina
    :param url: str
    :return: bs
    """
    return bs(requests.get(url).content, "html.parser")


def baixar_dados(soup: bs, ano: str, arquivos_csv: list) -> list:
    """
    Faz o download do ultimo arquivo TISS.
    :param soup: bs
    :param ano: str
    :param arquivos_csv: list
    :return: list
    :raises: Exception
    """
    links = soup.find_all("a")
    try:
        for link in links:
            if FILETYPE in link.get("href"):
                with open(link.get("href"), "wb") as arquivo_zip:
                    arquivo_zip.write(
                        requests.get(URL + ano + "/" + link.get("href")).content
                    )
                extrair_zip(link.get("href"))
                arquivos_csv.append(link.get("href").replace(FILETYPE, ".csv"))
        return arquivos_csv
    except Exception:
        raise Exception("Não foi possivel fazer o download dos arquivos!")


def extrair_zip(nome_arquivo) -> None:
    """
    Extrai os dados do arquivo zip
    :param nome_arquivo: str
    :return: None
    """
    with ZipFile(nome_arquivo, "r") as arquivo_zip:
        arquivo_zip.extractall()
    remove(nome_arquivo)


def montar_queries(arquivos_csv: list) -> dict:
    """
    Monta as queries para o mysql.
    :param arquivos_csv: list
    :return: dict
    """
    sql = {}
    index = 0
    for arquivo_csv in arquivos_csv:
        df = pd.read_csv(arquivo_csv, sep=";", encoding="ISO-8859-1")
        for _, row in df.iterrows():
            sql[
                index
            ] = f"INSERT INTO demonstracao_contabeis (DATA, REG_ANS, CD_CONTA_CONTABIL, DESCRICAO, VL_SALDO_FINAL) VALUES ('{row['DATA']}', '{row['REG_ANS']}', '{row['CD_CONTA_CONTABIL']}', '{row['DESCRICAO']}', '{row['VL_SALDO_FINAL']}');"
            index += 1
        print("Arquivo: " + arquivo_csv + " processado!")
    return sql


def gerar_arquivo_sql(sql: dict) -> None:
    """
    Cria o arquivo sql.
    :param sql: dict
    :return: None
    """
    with open("insert_queries.sql", "w") as arquivo_sql:
        for _, query in sql.items():
            arquivo_sql.write(query + "\n")


def main() -> None:
    """
    Funcão principal.
    :return: None
    """
    arquivos_csv = []
    ano = datetime.now().year

    soup = get_soup(URL + str(ano))
    arquivos_csv = baixar_dados(soup, str(ano), arquivos_csv)

    soup = get_soup(URL + str(ano - 1))
    arquivos_csv = baixar_dados(soup, str(ano - 1), arquivos_csv)

    sql = montar_queries(arquivos_csv)
    gerar_arquivo_sql(sql)


__name__ == "__main__" and main()
