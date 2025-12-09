import grpc
import service_pb2, service_pb2_grpc
import os
import csv
from lxml import etree


# -------- CONFIGURAÇÃO DOS CAMINHOS --------

CSV_PATH = "/data/input.csv"
XML_PATH = "/data/output.xml"
RESULT_PATH = "/data/result.txt"

if not os.path.exists(CSV_PATH):
    CSV_PATH = "../src/input.csv"
    XML_PATH = "../src/output.xml"
    RESULT_PATH = "../src/result.txt"


# -------- CONEXÃO gRPC --------

def connect():
    try:
        channel = grpc.insecure_channel("server:50051")
        grpc.channel_ready_future(channel).result(timeout=3)
        return service_pb2_grpc.XMLServiceStub(channel)
    except:
        channel = grpc.insecure_channel("localhost:50051")
        return service_pb2_grpc.XMLServiceStub(channel)


stub = connect()
print("Conectado ao servidor")


# -------- CSV → XML EM CHUNKS --------

def csv_to_xml():

    chunk_count = 0
    total_lines = 0

    if not os.path.exists(CSV_PATH):
        print("Ficheiro input.csv não encontrado.")
        return

    chunk_size = 2000

    # apagar XML antigo
    if os.path.exists(XML_PATH):
        os.remove(XML_PATH)

    root = etree.Element("root")

    with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)

        headers = next(reader)  # cabeçalho

        first_chunk = True
        chunk = []

        for row in reader:
            chunk.append(",".join(row))

            if len(chunk) == chunk_size:
                send_chunk(headers, chunk, root, first_chunk)
                chunk_count += 1
                total_lines += len(chunk)
                first_chunk = False
                chunk.clear()

        # último bloco
        if chunk:
            send_chunk(headers, chunk, root, first_chunk)
            chunk_count += 1
            total_lines += len(chunk)

    # guardar XML final
    with open(XML_PATH, "wb") as f:
        f.write(etree.tostring(root, pretty_print=True))

    print(f"\n{chunk_count} chunks foram implementadas ao XML.")
    print(f"Total: {total_lines} linhas.")
    print(f"XML criado com sucesso em {XML_PATH}")



def send_chunk(headers, chunk, root, first_chunk):

    # só o primeiro bloco tem cabeçalho
    if first_chunk:
        csv_text = ",".join(headers) + "\n" + "\n".join(chunk)
    else:
        csv_text = "\n".join(chunk)

    resp = stub.CsvToXml(service_pb2.CsvRequest(csv=csv_text))

    partial_root = etree.XML(resp.xml.encode())

    for jogador in partial_root:
        root.append(jogador)



# -------- VALIDAR XML --------

def validate_xml():
    if not os.path.exists(XML_PATH):
        print("XML não encontrado. Converta primeiro.")
        return

    with open(XML_PATH, "r", encoding="utf-8") as f:
        xml = f.read()

    resp = stub.ValidateXml(service_pb2.XmlRequest(xml=xml))

    print("valido" if resp.valid else "invalido", resp.message)


# -------- XPATH --------

def xpath_query():
    if not os.path.exists(XML_PATH):
        print("XML não encontrado.")
        return

    query = input("Escreve o XPath: ")

    with open(XML_PATH, "r", encoding="utf-8") as f:
        xml = f.read()

    resp = stub.XPathQuery(service_pb2.QueryRequest(xml=xml, query=query))

    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        f.write(resp.result)

    print("Resultado guardado em:", RESULT_PATH)


# -------- MENU --------

def menu():
    while True:
        print("\n===== CLIENTE gRPC =====")
        print("1 - CSV → XML (chunks de 500)")
        print("2 - Validar XML (XSD)")
        print("3 - XPath")
        print("4 - XQuery (futuro)")
        print("0 - Sair")

        option = input("Escolha: ")

        if option == "1":
            csv_to_xml()
        elif option == "2":
            validate_xml()
        elif option == "3":
            xpath_query()
        elif option == "4":
            print("XQuery em desenvolvimento...")
        elif option == "0":
            print("A sair...")
            break
        else:
            print("Opção inválida.")


# -------- EXECUÇÃO --------

menu()
