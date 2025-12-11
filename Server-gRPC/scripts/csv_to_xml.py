import csv
import io
import re
from lxml import etree


def safe_xml_tag(name: str, idx: int) -> str:
    #limpar espaços
    name = name.strip()

    #se estiver vazio, inventa nome
    if not name:
        name = f"campo_{idx}"

    #substitui caracteres inválidos
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)

    #não pode começar por um número
    if name[0].isdigit():
        name = "_" + name

    return name


def csv_to_xml(csv_chunk: str) -> str:

    f = io.StringIO(csv_chunk)
    reader = csv.reader(f)

    headers = next(reader)

    #sanitizar todos os headers
    headers = [safe_xml_tag(h, i) for i, h in enumerate(headers)]

    root = etree.Element("root")

    for row in reader:

        jogador = etree.SubElement(root, "jogador")

        for campo, valor in zip(headers, row):
            etree.SubElement(jogador, campo).text = valor.strip()
    

    return etree.tostring(root, encoding="unicode")
