import grpc
from concurrent import futures
import service_pb2, service_pb2_grpc
from scripts.csv_to_xml import csv_to_xml
from scripts.validate_xml import validate_xml_with_xsd
from scripts.XPath import load_xml_string, listar_colunas, contar_registos
from scripts.XQuery import run_xquery_and_save


class XMLService(service_pb2_grpc.XMLServiceServicer):

    def CsvToXml(self, request, context):
        xml = csv_to_xml(request.csv)
        return service_pb2.XmlResponse(xml=xml)

    def ValidateXml(self, request, context):
        xsd_path = "/app/src/schema.xsd"
        valid, message = validate_xml_with_xsd(request.xml, xsd_path)
        return service_pb2.ValidationResponse(valid=valid, message=message)

    def XPathQuery(self, request, context):
        from lxml import etree
        xml = etree.XML(request.xml.encode())
        result = xml.xpath(request.query)
        return service_pb2.QueryResponse(result=str(result))

    def XmlInfo(self, request, context):
        xml_root = load_xml_string(request.xml)
        if xml_root is None:
            return service_pb2.XmlInfoResponse(colunas=[], total=0)

        colunas = listar_colunas(xml_root)
        total = contar_registos(xml_root)

        return service_pb2.XmlInfoResponse(colunas=colunas, total=total)
    
    def ExecuteXQuery(self, request, context):
        xml_path = request.xmlPath
        query = request.query

        output_path = "/app/src/clientes_portugal.xml"

        result_message = run_xquery_and_save(xml_path, query, output_path)

        return service_pb2.XQueryResponse(result=result_message)


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(),
        options=[
            ("grpc.max_send_message_length", 1024 * 1024 * 100),
            ("grpc.max_receive_message_length", 1024 * 1024 * 100),
        ]
    )
    service_pb2_grpc.add_XMLServiceServicer_to_server(XMLService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Servidor gRPC ativo na porta 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
