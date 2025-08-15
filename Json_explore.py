from pymongo import MongoClient
from datetime import datetime
import json

# Mapping Python type name → JSON Schema type + extra info
def pytype_to_json_type(pyname):
    if pyname == "str":
        return "string", {}
    if pyname == "int":
        return "integer", {}
    if pyname == "float":
        return "number", {}
    if pyname == "bool":
        return "boolean", {}
    if pyname == "NoneType":
        return "null", {}
    if pyname == "datetime":
        return "string", {"format": "date-time"}
    if pyname == "ObjectId":
        return "string", {"pattern": "^[a-f0-9]{24}$"}
    return "string", {"x-python-type": pyname}

def scalar_schema(value):
    pyname = type(value).__name__
    if isinstance(value, datetime):
        pyname = "datetime"
    jtype, extra = pytype_to_json_type(pyname)
    node = {"type": [jtype]}
    node.update(extra)
    return node

def merge_types_list(t1, t2):
    return sorted(list(set(t1) | set(t2)))

def merge_schema(a, b):
    if not a:
        return b
    if not b:
        return a

    if a.get("type") == ["object"] and b.get("type") == ["object"]:
        props = a.setdefault("properties", {})
        for k, v in b.get("properties", {}).items():
            props[k] = merge_schema(props.get(k), v)
        return a

    if a.get("type") == ["array"] and b.get("type") == ["array"]:
        a["items"] = merge_schema(a.get("items"), b.get("items"))
        return a

    if isinstance(a.get("type"), list) and isinstance(b.get("type"), list):
        out = {"type": merge_types_list(a["type"], b["type"])}
        for k in ("format", "pattern"):
            if a.get(k) == b.get(k) and a.get(k) is not None:
                out[k] = a[k]
        return out

    return {"anyOf": [a, b]}

def analyze_value(value):
    if isinstance(value, dict):
        props = {}
        for k, v in value.items():
            props[k] = analyze_value(v)
        return {"type": ["object"], "properties": props}

    if isinstance(value, list):
        items_schema = None
        for item in value:
            items_schema = merge_schema(items_schema, analyze_value(item))
        return {"type": ["array"], "items": items_schema or {}}

    return scalar_schema(value)

def analyze_document(doc):
    props = {}
    for k, v in doc.items():
        props[k] = analyze_value(v)
    return {"type": ["object"], "properties": props}

# --- Kết nối MongoDB ---
client = MongoClient("mongodb://localhost:27017")
db = client["mock_shop"]

final_schema = None

for doc in db["user_messages"].find():
    s = analyze_document(doc)
    final_schema = merge_schema(final_schema, s)

# Xuất ra JSON hợp lệ
json_text = json.dumps(final_schema, indent=2, ensure_ascii=False)

# Lưu file để import vào công cụ trực quan hóa
with open("schema.json", "w", encoding="utf-8") as f:
    f.write(json_text)

print("Schema đã lưu vào user_sessions_schema.json")
