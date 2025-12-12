from lxml import etree

def validate_xml_with_xsd(xml_string: str, xsd_path: str):

    try:
        xml_doc = etree.fromstring(xml_string.encode("utf-8"))
    except Exception as e:
        return False, f"Erro ao carregar XML: {e}"

    try:
        with open(xsd_path, "rb") as f:
            schema_doc = etree.parse(f)
        schema = etree.XMLSchema(schema_doc)
    except Exception as e:
        return False, f"Erro ao carregar XSD: {e}"

    try:
        schema.assertValid(xml_doc)
        return True, "XML válido de acordo com o XSD."
    except etree.DocumentInvalid as e:
        return False, f"XML inválido: {e}"
    except Exception as e:
        return False, f"Erro na validação: {e}"
