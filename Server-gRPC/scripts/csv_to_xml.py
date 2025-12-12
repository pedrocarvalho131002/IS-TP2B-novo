import csv
import io
import re
from lxml import etree

_LAST_HEADERS = None

def _sanitize_tag(name: str) -> str:

    if name is None:
        return "field"
    s = name.strip()
    s = re.sub(r'[\s\/]+', '_', s)
    s = re.sub(r'[^0-9A-Za-z_]', '', s)
    if s == "":
        s = "field"
    if re.match(r'^\d', s):
        s = 'f_' + s
    return s

def csv_to_xml(csv_text: str) -> str:

    global _LAST_HEADERS

    sio = io.StringIO(csv_text)
    reader = csv.reader(sio)

    rows = [r for r in reader if r is not None and len(r) > 0]

    if not rows:
        root = etree.Element("root")
        return etree.tostring(root, pretty_print=True, encoding="utf-8").decode("utf-8")

    if _LAST_HEADERS is None:
        header_row = rows[0]
        data_rows = rows[1:]
        _LAST_HEADERS = header_row
    else:
        header_row = _LAST_HEADERS
        data_rows = rows

        if len(rows) > 0 and len(rows[0]) == len(header_row):
            first_row = [c.strip() for c in rows[0]]
            header_strip = [c.strip() for c in header_row]
            if first_row == header_strip:
                data_rows = rows[1:]

    sanitized_headers = [_sanitize_tag(h) for h in header_row]

    root = etree.Element("root")

    for row in data_rows:
        if len(row) < len(sanitized_headers):
            row = row + [""] * (len(sanitized_headers) - len(row))
        record_el = etree.Element("record")
        for idx, tag in enumerate(sanitized_headers):
            value = row[idx] if idx < len(row) else ""
            child = etree.SubElement(record_el, tag)
            child.text = value if value is not None else ""
        root.append(record_el)

    return etree.tostring(root, pretty_print=True, encoding="utf-8").decode("utf-8")