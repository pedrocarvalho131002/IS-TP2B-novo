import grpc
import service_pb2, service_pb2_grpc
import os
import csv
import io
from lxml import etree

CSV_PATH = "/app/src/input.csv"
XML_PATH = "/app/src/output.xml"
RESULT_PATH = "/app/src/result.txt"


def connect():
    while True:
        try:
            channel = grpc.insecure_channel(
                "server:50051",
                options=[
                    ("grpc.max_send_message_length", 1024 * 1024 * 100),
                    ("grpc.max_receive_message_length", 1024 * 1024 * 100),
                ]
            )
            grpc.channel_ready_future(channel).result(timeout=5)
            return service_pb2_grpc.XMLServiceStub(channel)
        except:
            print("Aguardando servidor gRPC iniciar...")


stub = connect()
print("Conectado ao servidor")


def csv_to_xml():
    if not os.path.exists(CSV_PATH):
        print("Ficheiro input.csv não encontrado.")
        return

    chunk_size = 500
    chunk_number = 0
    total_lines = 0

    if os.path.exists(XML_PATH):
        os.remove(XML_PATH)

    root = etree.Element("root")

    with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)

        first_chunk = True
        chunk = []

        for row in reader:
            output = io.StringIO()
            writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(row)
            csv_line = output.getvalue().strip()

            chunk.append(csv_line)

            if len(chunk) == chunk_size:
                chunk_number += 1
                total_lines += len(chunk)
                send_chunk(headers, chunk, root, first_chunk)
                first_chunk = False
                chunk = []

        if chunk:
            chunk_number += 1
            total_lines += len(chunk)
            send_chunk(headers, chunk, root, first_chunk)

    with open(XML_PATH, "wb") as f:
        f.write(etree.tostring(root, pretty_print=True))

    print(f"\n{chunk_number} chunks de {chunk_size} linhas foram implementadas ao XML.")
    print(f"Total: {total_lines} linhas processadas.")
    print(f"XML criado com sucesso em {XML_PATH}")


def send_chunk(headers, chunk, root, first_chunk):
    if first_chunk:
        csv_text = ",".join(headers) + "\n" + "\n".join(chunk)
    else:
        csv_text = "\n".join(chunk)

    resp = stub.CsvToXml(service_pb2.CsvRequest(csv=csv_text))
    partial_root = etree.XML(resp.xml.encode())

    for jogador in partial_root:
        root.append(jogador)


def validate_xml():
    if not os.path.exists(XML_PATH):
        print("XML não encontrado. Converta primeiro.")
        return

    with open(XML_PATH, "r", encoding="utf-8") as f:
        xml = f.read()

    resp = stub.ValidateXml(service_pb2.XmlRequest(xml=xml))

    if resp.valid:
        print("XML validado com sucesso!")
    else:
        print("XML inválido:")
        print(resp.message)


def xpath():
    if not os.path.exists(XML_PATH):
        print("XML não encontrado.")
        return

    with open(XML_PATH, "r", encoding="utf-8") as f:
        xml = f.read()

    resp = stub.XmlInfo(service_pb2.XmlRequest(xml=xml))

    print("\nColunas encontradas no XML:")
    for c in resp.colunas:
        print(" -", c)

    print(f"\nTotal de jogadores: {resp.total}")

def xquery_top10():
    if not os.path.exists(XML_PATH):
        print("XML não encontrado.")
        return

    query = """
    declare option saxon:output "indent=yes";

    <clientesPortugal>
    {
        for $r in /root/record
        where $r/Country = "Portugal"
        return $r
    }
    </clientesPortugal>
    """

    resp = stub.ExecuteXQuery(
        service_pb2.XQueryRequest(
            xmlPath=XML_PATH,
            query=query
        )
    )

    print("\nResultado do servidor:")
    print(resp.result)


def menu():
    while True:
        print("\n===== CLIENTE gRPC =====")
        print("1 - CSV para XML")
        print("2 - Validar XML com o XSD")
        print("3 - XPath")
        print("4 - XQuery")
        print("0 - Sair")

        option = input("Escolha: ")

        if option == "1":
            csv_to_xml()
        elif option == "2":
            validate_xml()
        elif option == "3":
            xpath()
        elif option == "4":
            xquery_top10()
        elif option == "0":
            print("A sair...")
            break
        else:
            print("Opção inválida.")

menu()
