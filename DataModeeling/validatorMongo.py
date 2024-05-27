from pymongo import MongoClient
from jsonschema import validate

# Conexión a la base de datos MongoDB
client = MongoClient("mongodb+srv://ymeshir:bxLMwFBJcGKGkSnG@cluster0.y16yvqn.mongodb.net/")
db = client["db_pruebas"]
coleccion = db["prueba"]

# Definir el esquema del documento
schema = {
    "type": "object",
    "properties": {
        "nombre": {"type": "string"},
        "edad": {"type": "integer"},
        "email": {"type": "string", "format": "email"}
    },
    "required": ["nombre", "edad", "email"]
}

# Definir el esquema del documento
schema1 = {
    "type": "object",
    "properties": {
        "nombre": {"type": "string"},
        "edad": {"type": "integer"},
        "email": {"type": "string", "format": "email"},
        "descripcion": {"type": "string"}
    },
    "required": ["nombre", "edad", "email", "descripcion"]
}

# Documento a insertar
documento = {
    "nombre": "Ejemplo",
    "edad": 31,
    "email": "ejemplo@example.com"
}

# Documento a insertar
documento1 = {
    "nombre": "Ejemplo",
    "edad": 23,
    "email": "ejemplo@example.com",
    "descripcion": "prueba",
}

# Validar el documento contra el esquema
try:
    validate(documento, schema)
    print("El documento es válido.")
    # Insertar el documento si pasa la validación
    try:
        coleccion.insert_one(documento)
        print("Documento insertado correctamente.")
    except Exception as e:
        print("Error al insertar el documento:", e)

except Exception as e:
    print("Error de validación:", e)


# Validar el documento contra el esquema
try:
    validate(documento1, schema1)
    print("El documento 1 es válido.")
    # Insertar el documento si pasa la validación
    try:
        coleccion.insert_one(documento1)
        print("Documento 1 insertado correctamente.")
    except Exception as e:
        print("Error al insertar el documento 1:", e)

except Exception as e:
    print("Error de validación 1 :", e)