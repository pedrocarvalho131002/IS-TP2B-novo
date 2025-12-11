from lxml import etree

def validate_xml_with_xsd(xml_string: str, xsd_path: str):
    """
    Valida APENAS os primeiros 5 jogadores do XML.
    """

    try:
        # carregar XML
        xml_doc = etree.XML(xml_string.encode())

        # encontrar jogadores
        jogadores = xml_doc.findall("jogador")

        # extrair só os primeiros 5
        subset_root = etree.Element("root")
        for jogador in jogadores[:5]:
            subset_root.append(jogador)

        subset_xml = etree.tostring(subset_root)

        # carregar XSD
        with open(xsd_path, "rb") as f:
            schema_root = etree.XML(f.read())
            schema = etree.XMLSchema(schema_root)

        # validar subset
        valid = schema.validate(etree.XML(subset_xml))

        if valid:
            return True, print("XML válido (apenas os primeiros 5 jogadores validados)")
        else:
            return False, str(schema.error_log)

    except Exception as e:
        return False, print(f"Erro ao validar XML: {str(e)}")
