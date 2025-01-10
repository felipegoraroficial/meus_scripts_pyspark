import requests

def get_job_runs_databricks(instancia_databricks, token, id_trabalho):
    """
    Obtém a lista de execuções de um trabalho no Databricks.

    Parâmetros:
    instancia_databricks (str): A URL da instância do Databricks.
    token (str): O token de autenticação para acessar a API do Databricks.
    id_trabalho (int): O ID do trabalho no Databricks.

    Retorna:
    list: Uma lista contendo as execuções do trabalho.
    """

    url = f"https://{instancia_databricks}/api/2.0/jobs/runs/list"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {"job_id": id_trabalho}

    response = requests.get(url, headers=headers, params=params)

    response.raise_for_status()

    log = response.json()

    logs_list = []

    for run in log['runs']:
        logs_list.append(run)

    return logs_list