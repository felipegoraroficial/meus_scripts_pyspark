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

    params = {"job_id": id_trabalho, "limit": 25}
    logs_list = []
    has_more = True

    while has_more:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        log = response.json()
        logs_list.extend(log['runs'])
        has_more = log.get('has_more', False)
        if has_more:
            params['offset'] = params.get('offset', 0) + 25

    return logs_list