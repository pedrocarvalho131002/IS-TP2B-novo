import grpc
import service_pb2, service_pb2_grpc
import os

# -------- CONFIGURAÇÃO DE CAMINHOS --------

# Caminho em Docker
CSV_PATH = "/data/input.csv"
XML_OUT = "/data/output.xml"
RESULT_OUT = "/data/result.txt"

# Fallback para execução local
if not os.path.exists(CSV_PATH):
    CSV_PATH = "../src/input.csv"
    XML_OUT = "../src/output.xml"
    RESULT_OUT = "../src/result.txt"

# -------- CONEXÃO gRPC --------

# Se estiver em Docker, hostname é "server"
HOST = "server:50051"

# Se estiver local, usar localhost
try:
    channel = grpc.insecure_channel(HOST)
    grpc.channel_ready_future(channel).result(timeout=3)
except:
    HOST = "localhost:50051"
    channel = grpc.insecure_channel(HOST)

stub = service_pb2_grpc.XMLServiceStub(channel)

print(f"Conectado ao servidor em {HOST}")

# -------- LÊ O CSV --------

with open(CSV_PATH, "r", encoding="utf-8") as f:
    csv_data = f.read()

print(f"CSV lido de: {CSV_PATH}")

# -------- ENVIA AO SERVIDOR --------

resp = stub.CsvToXml(service_pb2.CsvRequest(csv=csv_data))
xml = resp.xml

# -------- GUARDA XML --------

with open(XML_OUT, "w", encoding="utf-8") as f:
    f.write(xml)

print(f"XML gravado em: {XML_OUT}")

# -------- VALIDA XML --------

val = stub.ValidateXml(service_pb2.XmlRequest(xml=xml))
print("Validação:", val.message)

# -------- XPATH --------

query = "//row/col0/text()"
res = stub.XPathQuery(service_pb2.QueryRequest(xml=xml, query=query))

# -------- GUARDA RESULTADO --------

with open(RESULT_OUT, "w", encoding="utf-8") as f:
    f.write(res.result)

print(f"Resultado XPath em: {RESULT_OUT}")
