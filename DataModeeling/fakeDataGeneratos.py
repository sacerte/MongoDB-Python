from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime
import sys, json

# Conexión a la base de datos de MongoDB
client = MongoClient("mongodb+srv://ymeshir:bxLMwFBJcGKGkSnG@cluster0.y16yvqn.mongodb.net/")
db = client['nomenclator']

with open('schemas/MydbCsfmsbes01_MongoDBSchema.json', 'r') as file:
    schema = json.load(file)

name_col = schema['title']
coll= name_col.split('.')[1]

collection = db[coll]

if collection == None:
    db.create_collection(coll, validator={'$jsonSchema': schema})
    collection = db[coll]

# Genera una cantidad específica de datos ficticios basados en el esquema
fake = Faker()
num_documentos = 50  # Cambia este valor al número de documentos que deseas insertar
total_peso = 0

type_map = {
    "string": fake.text,
    "int": lambda: fake.random_int(min=2, max=99),
    "double": lambda: fake.random_number(digits=5, fix_len=True),
    "email": fake.email,
    "address": fake.address,
    "date": fake.date,
    "boolean": fake.boolean
}

properties = name_col = schema['properties']

for _ in range(num_documentos):
    record = {}
    for field, detail in properties.items():
        bson_type = detail["bsonType"]
        if bson_type == "array":
                item_type = detail['items']['bsonType']
                if item_type == "object":
                    sub_properties = detail['items']['properties']
                    sub_record = []
                    for _ in range(fake.random_int(min=1, max=5)):
                        sub_item = {}
                        for sub_field, sub_details in sub_properties.items():
                            sub_bson_type = sub_details['bsonType']
                            sub_item[sub_field] = type_map[sub_bson_type]()
                        sub_record.append(sub_item)
                    record[field] = sub_record
                else:
                    record[field] = [type_map[item_type]() for _ in range(fake.random_int(min=1, max=5))]
        elif bson_type == "object":
            sub_properties = detail['properties']
            sub_record = {}
            for sub_field, sub_detail in sub_properties.items():
                sub_bson_type=sub_detail['bsonType']
                sub_record[sub_field] = type_map[sub_bson_type]()
            record[field] = sub_record
        elif bson_type in type_map:
            record[field] = type_map[bson_type]()
        #else: 
        #    record[field] = "undefinded"

    # Inserta cada documento en la colección
    insert_result = collection.insert_one(record)
    print("Documento insertado con el ID:", insert_result.inserted_id)

    # Calcula el peso del documento y suma al total
    peso_documento = sys.getsizeof(record)
    total_peso += peso_documento

# Convertir el peso total a MB
total_peso_mb = total_peso / (1024 * 1024)  # Convertir bytes a megabytes

# Muestra el total de documentos insertados y el peso total de los documentos
print(f"Total de documentos insertados: {num_documentos}")
print(f"Peso total de los documentos insertados: {total_peso} bytes")
print(f"Peso total de los documentos insertados: {total_peso_mb:.2f} MB")