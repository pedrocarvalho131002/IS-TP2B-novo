import grpc
from concurrent import futures
import service_pb2, service_pb2_grpc
from lxml import etree
from scripts.csv_to_xml import csv_to_xml

XSD_FILE = "schema.xsd"


class XMLService(service_pb2_grpc.XMLServiceServicer):

    def CsvToXml(self, request, context):
        xml = csv_to_xml(request.csv)
        return service_pb2.XmlResponse(xml=xml)

    def ValidateXml(self, request, context):
        xml = etree.XML(request.xml.encode())

        with open(XSD_FILE, "rb") as f:
            schema_root = etree.XML(f.read())
            schema = etree.XMLSchema(schema_root)

        valid = schema.validate(xml)
        msg = "XML válido" if valid else str(schema.error_log)

        return service_pb2.ValidationResponse(valid=valid, message=msg)

    def XPathQuery(self, request, context):
        xml = etree.XML(request.xml.encode())
        result = xml.xpath(request.query)

        return service_pb2.QueryResponse(result=str(result))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    service_pb2_grpc.add_XMLServiceServicer_to_server(XMLService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Servidor gRPC ativo na porta 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
