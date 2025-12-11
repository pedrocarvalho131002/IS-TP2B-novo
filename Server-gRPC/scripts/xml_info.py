from lxml import etree


def load_xml_string(xml_string):
    try:
        return etree.XML(xml_string.encode())
    except Exception:
        return None


def listar_colunas(xml_root):
    primeiro = xml_root.find("jogador")
    if primeiro is None:
        return []

    colunas = [child.tag for child in primeiro]
    return colunas


def contar_registos(xml_root):
    total = int(xml_root.xpath("count(/root/jogador)"))
    return total
