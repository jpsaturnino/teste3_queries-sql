from datetime import datetime
from os import remove, listdir, mkdir
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import threading
import shutil

URL = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
TIPOZIP = ".zip"
TIPOCSV = ".csv"
TIPOSQL = ".sql"
arquivos_csv = []


def get_soup(url: str) -> bs:
    """
    Retorna o conteudo da pagina
    :param url: str
    :return: bs
    """
    return bs(requests.get(url).content, "html.parser")


def baixar_dados(soup: bs, ano: str) -> None:
    """
    Faz o download dos arquivos dos ultimos dois anos.
    :param soup: bs
    :param ano: str
    :return: None
    :raises: Exception
    """
    print("Baixando dados do ano: " + ano)
    links = soup.find_all("a")
    try:
        for link in links:
            if TIPOZIP in link.get("href"):
                with open("./zip/" + link.get("href"), "wb") as arquivo_zip:
                    arquivo_zip.write(
                        requests.get(URL + ano + "/" + link.get("href")).content
                    )
                extrair_zip(link.get("href"))
                arquivos_csv.append(
                    "./dados/" + link.get("href").replace(TIPOZIP, TIPOCSV)
                )
        print("Arquivos do ano: " + ano + " baixados!\n")
    except Exception:
        raise Exception("Não foi possivel fazer o download dos arquivos!")


def criar_pastas() -> None:
    """
    Cria as pastas necessarias para o processamento.
    :return: None
    """
    try:
        mkdir("./dados")
        mkdir("./zip")
        mkdir("./queries")
    except FileExistsError:
        pass


def extrair_zip(nome_arquivo) -> None:
    """
    Extrai o arquivo zip
    :param nome_arquivo: str
    :return: None
    """
    print("Extraindo arquivo zip...")
    try:
        shutil.unpack_archive("./zip/" + nome_arquivo, "./dados/")
    except Exception:
        raise Exception("Não foi possivel extrair o arquivo!")


def montar_queries(sql: list) -> None:
    """
    Monta as queries para o mysql.
    :return: dict
    """
    try:
        lista_i = 0
        print("Montando queries...")
        for arquivo_csv in arquivos_csv:
            dict_i = 0
            sql.append({})
            csv = pd.read_csv(
                arquivo_csv,
                sep=";",
                encoding="ISO-8859-1",
            )
            for _, row in csv.iterrows():
                # coloca a data no formato YYYY-MM-DD
                data = formatar_data(row["DATA"])
                # substitui as virgulas da coluna VL_SALDO_FINAL por ponto
                valor = row["VL_SALDO_FINAL"].replace(",", ".")
                # remove os caracteres vazios desnecessarios na DESCRICAO
                descricao = tratar_descricao(row["DESCRICAO"])
                sql[lista_i][
                    dict_i
                ] = f"INSERT INTO dem_contabeis (DC_DATA, DC_REG_ANS, DC_CD_CONTA_CONTABIL, DC_DESCRICAO, DC_VL_SALDO_FINAL) VALUES ('{data}', '{row['REG_ANS']}', '{row['CD_CONTA_CONTABIL']}', {descricao}, '{valor}');"
                dict_i += 1
            lista_i += 1
            # limpa a memoria
            del csv
            print("Arquivo: " + arquivo_csv + " processado!")
        print("Queries montadas!\n")
    except Exception:
        raise Exception("Não foi possivel montar as queries!")


def tratar_descricao(descricao: str) -> str:
    """
    Trata a ocorrencia desnecessaria de caracteres vazios na string.
    :param descricao: str
    :return: str
    """
    descricao = "'" + descricao + "'"
    descricao = descricao.replace("  ", " ")
    descricao = descricao.replace(" '", "'")
    return descricao


def formatar_data(data: str) -> str:
    """
    Formata a data para YYYY-MM-DD.
    :param data: str
    :return: str
    """
    data = data.split("/")
    return data[2] + "-" + data[1] + "-" + data[0]


def remover_zip() -> None:
    """
    Remove os arquivos zip baixados.
    :return: None
    """
    try:
        print("Deletando a pasta de arquivos zip...")
        remove("./zip/")
    except Exception:
        print("Não foi possivel remover a pasta de arquivos zip!")


def gerar_arquivo_sql(sql: dict) -> None:
    """
    Cria o arquivo sql a partir das queries do dict.
    :param sql: dict
    :return: None
    """
    index = 0
    print("Gerando arquivos sql...")
    while arquivos_csv:
        with open(
            "./queries/" + "insert_" + arquivos_csv.pop()[8:][:6] + TIPOSQL, "w"
        ) as arquivo_sql:
            for dict_i in range(len(sql[index])):
                arquivo_sql.write(sql[index][dict_i] + "\n")
        index += 1
        print("Novo arquivo sql gerado!")


def main() -> None:
    """
    Funcão principal.
    :return: None
    """
    criar_pastas()

    ano = datetime.now().year

    # multithreading na execução dos downloads e extracao dos arquivos
    soup_1 = get_soup(URL + str(ano - 1))
    thread = threading.Thread(
        target=baixar_dados,
        args=(
            soup_1,
            str(ano - 1),
        ),
    )

    thread.start()

    soup_2 = get_soup(URL + str(ano))
    baixar_dados(soup_2, str(ano))

    thread.join()
    # fim multithreading

    sql = [{}]
    montar_queries(sql)
    gerar_arquivo_sql(sql)
    remover_zip()


__name__ == "__main__" and main()
