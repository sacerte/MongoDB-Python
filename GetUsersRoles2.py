"""
@@Author: Yaser Meshir
Instalar las librerias:
pip install pymongo
"""

import pymongo
import csv

client = pymongo.MongoClient('mongodb://your_user:your_pass@yout_host:port/admin?authSource=admin')
db = client["admin"]
coluser = db["system.users"]
colrol = db["system.roles"]
x = coluser.find({}, {"user":1, "db":1, "roles":1})

with open('usuariosroles.csv','w', newline='') as csvfile:
    fieldnames = ["Usuario", "Database", "Rol", "Privilegios"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()
    for user in x:
        roles = user["roles"]
        for rol in roles:
            writer.writerow({"Usuario": user["user"], "Database": user["db"], "Rol": rol["role"], "Privilegios": ""})
            datarol = colrol.find({"role":rol["role"]}, {"role":1, "db":1, "privileges": 1})
            for data in datarol:
                writer.writerow({"Usuario": user["user"], "Database": user["db"], "Rol": rol["role"], "Privilegios": str(data["privileges"])})