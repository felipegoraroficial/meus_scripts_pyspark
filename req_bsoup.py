import requests
from bs4 import BeautifulSoup

def req_bsoup(url):
    """
    Faz uma requisição HTTP GET para a URL especificada e analisa o conteúdo HTML da resposta usando BeautifulSoup.

    Parâmetros:
    url (str): A URL para a qual a requisição GET será feita.

    Retorna:
    BeautifulSoup: Um objeto BeautifulSoup contendo o conteúdo HTML analisado da resposta.
    """
    # Define o cabeçalho do agente de usuário para a requisição HTTP
    headers = {'user-agent': 'Mozilla/5.0'}

    # Faz uma requisição GET para a URL especificada com o número da página e cabeçalho
    resposta = requests.get(url, headers=headers)

    # Analisa o conteúdo HTML da resposta usando BeautifulSoup
    sopa = BeautifulSoup(resposta.text, 'html.parser')

    # Formata o conteúdo da página HTML de forma legível
    page_content = sopa.prettify()

    return page_content