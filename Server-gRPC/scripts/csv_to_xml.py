import csv
import io
from lxml import etree


def csv_to_xml(csv_string: str) -> str:
    """
    Recebe CSV em texto e devolve XML em string.
    """
    f = io.StringIO(csv_string)
    reader = csv.reader(f)

    root = etree.Element("root")

    for row in reader:
        item = etree.SubElement(root, "row")
        for i, value in enumerate(row):
            field = etree.SubElement(item, f"col{i}")
            field.text = value

    return etree.tostring(root, pretty_print=True, encoding="unicode")