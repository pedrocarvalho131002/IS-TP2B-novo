import subprocess
import tempfile
import os

SAXON_JAR = "/app/saxon-he.jar"

def run_xquery_and_save(xml_path, xquery_text, output_path):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xq") as tmp:
        tmp.write(xquery_text.encode("utf-8"))
        query_file = tmp.name

    try:
        cmd = [
            "java", "-cp", SAXON_JAR,
            "net.sf.saxon.Query",
            f"-s:{xml_path}",
            f"-q:{query_file}",
            f"-o:{output_path}"
        ]

        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT)

        return "Arquivo criado em: " + output_path

    except subprocess.CalledProcessError as e:
        return "Erro ao executar XQuery:\n" + e.output.decode()

    finally:
        if os.path.exists(query_file):
            os.remove(query_file)
