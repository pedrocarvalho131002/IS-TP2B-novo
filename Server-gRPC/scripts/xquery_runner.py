import requests

def run_xquery(query):
    import requests
    response = requests.post(
        "http://basex:8984/rest",
        data=query,
        headers={"Content-Type": "application/query+xml"}
    )
    return response.text
