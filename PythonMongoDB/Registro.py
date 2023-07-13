import pymongo
import json
import time
import os

fields = ["Nombre", "Apellido", "TipoDeDocumento",
          "NumeroDocumentoIdentidad", "Sexo", "telefono", "FechaNacimiento"]


def parseData(field_list):
    extractedDataList = []

    with open("data.json", "r") as read_file:
        data = json.load(read_file)

        if isinstance(data, dict):
            # JSON data is a dictionary
            extractedData = {}
            for field in field_list:
                try:
                    value = data[field]
                    extractedData[field] = value
                except KeyError:
                    print(f"Field '{field}' not found in JSON data.")
            extractedDataList.append(extractedData)
        elif isinstance(data, list):
            # JSON data is a list of dictionaries
            for item in data:
                extractedData = {}
                for field in field_list:
                    try:
                        value = item[field]
                        extractedData[field] = value
                    except KeyError:
                        print(f"Field '{field}' not found in JSON data.")
                extractedDataList.append(extractedData)
        else:
            print("Invalid JSON data format.")

    return extractedDataList


def get_database():
   # Insertar connection string a la base de datos

    client = pymongo.MongoClient(os.environ['CONEXIONSTRING'])
    mydb = client["Experimentación"]
    mycol = mydb["Clientes"]
    extractedDataList = parseData(fields)
    try:
        mycol.insert_many(extractedDataList)
        return 0
    except:
        print("Error when inserting")
        return 1


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
    # Get the database
    startTime = time.time()
    dbname = get_database()
    endTime = time.time()
    executionTime = endTime-startTime
    print(f"Code: {dbname}, Elapsed Time: {executionTime}")
