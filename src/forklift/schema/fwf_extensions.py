def parse_fwf_row(raw: bytes, spec: dict) -> dict:
    text = raw.decode(spec.get("encoding", "utf-8"), errors="replace").rstrip("\r\n")
    out = {}
    for field in spec["fields"]:
        name = field["name"]
        start = field["start"] - 1
        length = field["length"]
        chunk = text[start:start + length]
        if field.get("rstrip", True):
            chunk = chunk.rstrip()
        if field.get("lstrip", False):
            chunk = chunk.lstrip()
        out[name] = chunk
    return out
