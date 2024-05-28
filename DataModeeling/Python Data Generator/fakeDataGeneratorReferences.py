import json
import os
import sys
from pymongo import MongoClient, errors
from faker import Faker
from bson import ObjectId

# Función para leer todos los archivos de esquema desde una ruta específica
def read_schemas_from_directory(directory_path):
    schemas = []
    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):
            with open(os.path.join(directory_path, filename), 'r') as file:
                schema = json.load(file)
                schemas.append((filename.replace('.json', ''), schema))
    return schemas

with open('Python Validatos Schema/parametrics.json', 'r') as fileparam:
    param = json.load(fileparam) 

# Conectar a MongoDB
client = MongoClient(param['client'])
db = client[param['db']]

# Inicializar Faker
fake = Faker()

# Mapeo de tipos de datos de schema a faker
type_map = {
    "string": fake.text,
    "int": lambda: fake.random_int(min=18, max=99),
    "double": lambda: fake.random_number(digits=5, fix_len=True),
    "email": fake.email,
    "address": fake.address,
    "date": fake.date,
    "boolean": fake.boolean,
    "array": lambda: [fake.text() for _ in range(fake.random_int(min=1, max=5))],
    "object": lambda: {fake.word(): fake.text() for _ in range(fake.random_int(min=1, max=5))},
    "objectId": lambda: ObjectId()
}

# Función para generar datos falsos
def generate_fake_data(schema, num_records, existing_ids=None, ref_field=None):
    properties = schema['properties']
    data = []
    for _ in range(num_records):
        record = {}
        for field, details in properties.items():
            bson_type = details['bsonType']
            if bson_type == "array":
                item_type = details['items']['bsonType']
                if item_type == "object":
                    sub_properties = details['items']['properties']
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
                sub_properties = details['properties']
                sub_record = {}
                for sub_field, sub_details in sub_properties.items():
                    sub_bson_type = sub_details['bsonType']
                    sub_record[sub_field] = type_map[sub_bson_type]()
                record[field] = sub_record
            elif field == ref_field and existing_ids:
                record[field] = fake.random_element(existing_ids)
            elif bson_type in type_map:
                record[field] = type_map[bson_type]()
            else:
                record[field] = "undefined"
        data.append(record)
    return data

# Ruta de los archivos de esquema
schemas_directory = param['pathSchemas']

# Leer todos los esquemas
schemas = read_schemas_from_directory(schemas_directory)

# Inicializar contadores
total_documents = 0
total_size_bytes = 0

# Diccionario para almacenar IDs generados por colección
generated_ids = {}

references = param['references']

# Procesar cada esquema
for collection_name, schema in schemas:
    # Crear la colección con el validador de esquema si no existe
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
    
    collection = db[collection_name]

    ref_field = None
    for item in references:
        if isinstance(item, dict) and item.get("collection_name") == collection_name:
            print('Esta')
            ref_field = item['ref_filed'] 

    # Generar y contar datos falsos
    fake_data = generate_fake_data(schema, param['cant_documents'], existing_ids=generated_ids.get(item['collect_from_reference']), ref_field=ref_field)
    
    # Intentar insertar los datos en la colección
    try:
        result = collection.insert_many(fake_data, ordered=False)
        inserted_count = len(result.inserted_ids)
        total_documents += inserted_count
        for doc_id in result.inserted_ids:
            generated_ids.setdefault(collection_name, []).append(doc_id)
        for doc in fake_data:
            total_size_bytes += sys.getsizeof(doc)
        print(f'{inserted_count} documentos insertados en la colección {collection_name}')
    except errors.BulkWriteError as bwe:
        print(f'Error al insertar documentos en la colección {collection_name}')
        for error in bwe.details['writeErrors']:
            print(f'Error en el índice {error["index"]}: {error["errmsg"]}')

# Convertir el tamaño total a MB
total_size_mb = total_size_bytes / (1024 * 1024)

# Mostrar resultados
print(f'Total de documentos insertados: {total_documents}')
print(f'Tamaño total de los documentos: {total_size_mb:.2f} MB')

# Cerrar la conexión
client.close()