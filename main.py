import pymongo
from pymongo import MongoClient, errors
from config import config
import csv
import time
import datetime



def sub_files(file, year, logs_f):
    logs_f.write(str(datetime.datetime.now()) + " -- sub_files start \n")
    amount = 15
    with open(file, 'r') as f:
        f.readline()
        csv_file = f.readlines()
        file_len = len(csv_file)
        rows = file_len//amount + 1
        filename_list = []
        filename = 1
        for i in range(file_len):
            if i % rows == 0:
                name = str(filename) + '.'+str(year)+'.csv'
                sub = open(str(filename) + '.'+str(year)+'.csv', 'w+')
                sub.writelines(csv_file[i:i+rows])
                filename_list += [name]
                filename += 1
                sub.close()
    logs_f.write(str(datetime.datetime.now()) + " -- sub_files done\n")
    return filename_list


def insert_data(lst, year, collection1, collection2, logs_f):
    zno_collection_col = ['_id', 'zno_year', 'OUTID', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME', 'TERNAME', 'REGTYPENAME',
                 'TerTypeName', 'ClassProfileNAME', 'ClassLangName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName',
                 'EOTerName', 'EOParent', 'UkrTest', 'UkrTestStatus', 'UkrBall100', 'UkrBall12', 'UkrBall',
                 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName', 'UkrPTAreaName', 'UkrPTTerName', 'histTest', 'HistLang',
                 'histTestStatus', 'histBall100', 'histBall12', 'histBall', 'histPTName', 'histPTRegName',
                 'histPTAreaName', 'histPTTerName', 'mathTest', 'mathLang', 'mathTestStatus', 'mathBall100',
                 'mathBall12', 'mathBall', 'mathPTName', 'mathPTRegName', 'mathPTAreaName', 'mathPTTerName', 'physTest',
                 'physLang', 'physTestStatus', 'physBall100', 'physBall12', 'physBall', 'physPTName', 'physPTRegName',
                 'physPTAreaName', 'physPTTerName', 'chemTest', 'chemLang', 'chemTestStatus', 'chemBall100',
                 'chemBall12', 'chemBall', 'chemPTName', 'chemPTRegName', 'chemPTAreaName', 'chemPTTerName', 'bioTest',
                 'bioLang', 'bioTestStatus', 'bioBall100', 'bioBall12', 'bioBall', 'bioPTName', 'bioPTRegName',
                 'bioPTAreaName', 'bioPTTerName', 'geoTest', 'geoLang', 'geoTestStatus', 'geoBall100', 'geoBall12',
                 'geoBall', 'geoPTName', 'geoPTRegName', 'geoPTAreaName', 'geoPTTerName', 'engTest', 'engTestStatus',
                 'engBall100', 'engBall12', 'engDPALevel', 'engBall', 'engPTName', 'engPTRegName', 'engPTAreaName',
                 'engPTTerName', 'fraTest', 'fraTestStatus', 'fraBall100', 'fraBall12', 'fraDPALevel', 'fraBall',
                 'fraPTName', 'fraPTRegName', 'fraPTAreaName', 'fraPTTerName', 'deuTest', 'deuTestStatus', 'deuBall100',
                 'deuBall12', 'deuDPALevel', 'deuBall', 'deuPTName', 'deuPTRegName', 'deuPTAreaName', 'deuPTTerName',
                 'spaTest', 'spaTestStatus', 'spaBall100', 'spaBall12', 'spaDPALevel', 'spaBall', 'spaPTName',
                 'spaPTRegName', 'spaPTAreaName', 'spaPTTerName']
    numeric_col = [1, 18, 19, 20, 21, 29, 30, 31, 39, 40, 41, 49, 50, 51, 59, 60, 61, 69, 70, 71, 79, 80, 81, 88, 89, 91, 98, 99, 101, 108, 109, 111, 118, 119, 121]
    logs_f.write(str(datetime.datetime.now()) + " -- insert_data start\n")
    file_insert_to_bd = list()
    inserted = False
    index = 0
    while not inserted:
        try:
            count = 0
            in_coll_id = 0
            query = collection2.find_one({}, sort=[('_id', -1)])
            if query == None:
                row_num = 0
            else:
                row_num = query['last_row']
                collection1.delete_many({'_id': {'$gt': row_num}})
                in_coll_id = query['_id'] + 1
                index = in_coll_id

            for file_name in lst:
                count += 1
                if count < index:
                    print('pass')
                    continue

                elif count >= index:
                    index += 1

                    file_insert_to_bd = list()
                    with open(file_name) as file:
                        reader = csv.reader(file, delimiter=';', quoting=csv.QUOTE_ALL)
                        for row in reader:
                            row_num += 1
                            for i in range(len(row)):
                                if row[i] == 'null':
                                    row[i] = None
                                else:
                                    if i in numeric_col:
                                        row[i] = row[i].replace(',', '.')
                                        row[i] = float(row[i])

                            file_insert_to_bd.append(dict(zip(zno_collection_col, [row_num] + [year] + row)))
                    print("before insert")
                    collection1.insert_many(file_insert_to_bd)
                    last_row_inx = file_insert_to_bd[-1]["_id"]
                    plus = 0
                    if year == 2020:
                        plus = 15
                    collection2.insert_one({'_id': lst.index(file_name)+1+plus+in_coll_id, 'filename': file_name, 'last_row': last_row_inx})

                first_row_inx = file_insert_to_bd[0]["_id"]
                query = collection2.find_one({}, sort=[('_id', -1)])
                if query['filename'] != file_name:
                    while query['filename'] != file_name:
                        print("loop")
                        collection1.delete_many({'_id': {'$gt': first_row_inx - 1}})
                        collection2.delete_one({'filename': file_name})
                        collection1.insert_many(file_insert_to_bd)
                        collection2.insert_one({'_id': lst.index(file_name) + 1, 'filename': file_name})
                        query = collection2.find_one({}, sort=[('_id', -1)])
                elif query['filename'] == lst[-1]:
                    inserted = True
                print("insert success" + file_name)
                logs_f.write(str(datetime.datetime.now()) + " -- insert success " + str(file_name) + "\n")

        except pymongo.errors.ConnectionFailure as er:
            print('Error: ', er)
            first_row_inx = file_insert_to_bd[0]["_id"]
            last_row_inx = file_insert_to_bd[-1]["_id"]
            query = collection2.find_one({}, sort=[('_id', -1)])
            collection1.delete_many({'_id': {'$gt': first_row_inx - 1}})
            if last_row_inx == query['last_row']:
                collection2.delete_one({}, sort=[('_id', -1)])


def write_result(collection):
    with open("answer.csv", "w", newline="") as file:
        writer = csv.writer(file)
        result = collection.aggregate([
            {"$match": {"histTestStatus": "Зараховано"}},
            {"$group": {
                "_id": {
                    "regname": "$REGNAME",
                    "zno_year": "$zno_year",
                    "histteststatus": "$histTestStatus"
                },
                "avgball": {
                    "$avg": "$histBall100"
                }
            }},
            {"$sort": {"_id": 1}}
        ])
        writer.writerow(["regname", "zno_year", "histteststatus", "avgball"])
        for doc in result:
            regname = doc["_id"]["regname"]
            zno_year = doc["_id"]["zno_year"]
            histteststatus = doc["_id"]['histteststatus']
            avgball = doc["avgball"]
            writer.writerow([regname, zno_year, histteststatus, avgball])


def connect():
    #Connect to the mongoDB
    cluster = None
    try:
        params = config()
        cluster = MongoClient(f'mongodb://{params["host"]}:{params["port"]}')
        db = cluster[params['database']]
        collection1 = db["zno19_20"]
        collection2 = db["success_commit"]
        logs_file = open('logs.txt', 'w')
        l_2019 = sub_files('Odata2019File.csv', 2019, logs_file)
        l_2020 = sub_files('Odata2020File.csv', 2020, logs_file)

        insert_data(l_2019, 2019, collection1, collection2, logs_file)
        insert_data(l_2020, 2020, collection1, collection2, logs_file)

        logs_file.close()
        write_result(collection1)
        print("result has just been written")
    except (Exception, pymongo.errors.ConnectionFailure) as error:
        print(error)
    finally:
        if cluster is not None:
            cluster.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()
