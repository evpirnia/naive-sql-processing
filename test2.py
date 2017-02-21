#!/usr/bin/python3
import pymysql
import csv
import threading
import sys
from sys import argv

def loadCSV(argv):
    # Take in Command Line Arguments for files
    fname, clustercfg, csvfile = argv

    # Print Contents of varaibles: clustercfg and csvfile
    # print(clustercfg)
    # print(csvfile)

    # url = ""
    # hostname = ""
    # port = ""
    # db = ""
    # username = ""
    # passwd = ""
    # tname = ""
    # nodedriver = ""
    # nodeurl = ""
    # nodeuser = ""
    # nodepasswd = ""
    # partmtd = ""
    # partcol = ""
    # partparam1 = ""
    # partparam2 = ""
    partmtd = -1
    numnodes = -1

    mtd1info = []
    mtd2info = []
    # Read clustercfg file line by line for catalog information
    k = open(clustercfg, "r")
    with open(clustercfg) as fin:
        for line in fin:
            if line.strip():
                temp = line.strip().split("=")
                # Printing Left Value =
                if temp[0].split(".")[0].find("catalog") > -1:
                    if temp[0].split(".")[1].find("driver") > -1:
                        pass
                    elif temp[0].split(".")[1].find("hostname") > -1:
                        url = temp[1]
                        hostname = temp[1].split("/", 2)[2].split(":")[0]
                        port = temp[1].split("/", 2)[2].split(":")[1].split("/")[0]
                        db = temp[1].split("/", 2)[2].split(":")[1].split("/")[1]
                    elif temp[0].split(".")[1].find("username") > -1:
                        username = temp[1]
                    elif temp[0].split(".")[1].find("passwd") > -1:
                        passwd = temp[1]
                        catalog = Catalog(hostname, username, passwd, db, url)
                        catalog.createCatalog()
                elif temp[0].find("tablename") > -1:
                    tname = temp[1]
                elif temp[0].find("partition.method") > -1:
                    if temp[1].find("notpartition") > -1:
                        partmtd = 0
                    elif temp[1].find("range") > -1:
                        partmtd = 1
                    elif temp[1].find("hash") > -1:
                        partmtd = 2
                elif partmtd == 0:
                    if temp[0].find("numnodes") > -1:
                        numnodes = temp[1]
                elif partmtd == 1:
                    if temp[0].find("numnodes") > -1:
                        numnodes = temp[1]
                    elif temp[0].find(".column") > -1:
                        column = temp[1]
                    elif temp[0].find(".param1") > -1:
                        param1 = temp[1]
                    elif temp[0].find(".param2") > -1:
                        param2 = temp[1]
                        nodenum = temp[0].split(".")[1].replace("node","")
                        temp = []
                        temp.append(nodenum)
                        temp.append(column)
                        temp.append(param1)
                        temp.append(param2)
                        mtd1info.append(temp)
                elif partmtd == 2:
                    if temp[0].find(".column") > -1:
                        column = temp[1]
                    elif temp[0].find(".param1") > -1:
                        param1 = temp[1]
                        temp = []
                        temp.append(column)
                        temp.append(param1)
                        mtd2info.append(temp)

    # Read csv file with python3
    nodes = []
    with open(csvfile) as c:
        filtered = (line.replace('\n','') for line in c)
        reader = csv.reader(filtered, delimiter=',')
        header = next(reader)
        for row in reader:
            if any(field.strip() for field in row):
                nodes.append(row)

    if partmtd == 0:
        if len(nodes) != int(numnodes):
            print("Error")
        else:
            catalog.insert0(nodes, tname)
    elif partmtd == 1:
        if len(nodes) != int(numnodes):
            print("Error")
        else:
            for m in mtd1info:
                catalog.insert1(header, nodes, int(m[0]), tname, m[1], int(m[2]), int(m[3]))
    elif partmtd == 2:
        catalog.insert2(header, nodes, tname, mtd2info)

class Catalog:
    'Base Class for Catalog'
    def __init__(self, hostname, username, passwd, db, url):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.url = url
    def insert0(self, nodes, tname):
        print()
        # for n in nodes:
            # try to insert into table nodes
            # update Catalog
            # catch if error
    def insert1(self, header, nodes, dnum, tname, col, p1, p2):
        i = 0
        for h in header:
            if h == col:
                break
            else:
                i = i + 1
        # if p1 < int(nodes[dnum-1][i]):
        #     if int(nodes[dnum-1][i]) < p2:
                # try to insert into table nodes
                # updateCatalog
                # catch if error
    def insert2(self, header, nodes, tname, info):
        for i in info:
            col = i[0]
            p1 = int(i[1])
            i = 0
            for h in header:
                if h == col:
                    break
                else:
                    i = i + 1
            for n in nodes:
                if int(n[i]) == ((i % p1) + 1):
                    # try to insert into table nodes
                    # updateCatalog
                    # catch if error
    def displayCatalogInfo(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db)
    def updateCatalog(self, table, driver, url, user, passwd, mtd, nodeid, col, param1, param2):
        try:
            print(n)
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute("""INSERT INTO dtables VALUES (%s, %s, %s, %s, %s, %d, %d, %s, %s, %s)""", (table, driver, url, user, passwd, mtd, nodeid, col, param1, param2))
            connect.commit()
            connect.close()
        except pymysql.InternalError:
            print("Error")
        except pymysql.OperationalError:
            print("Error")
    def createCatalog(self):
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute("""CREATE TABLE dtables (tname VARCHAR(32), nodedriver VARCHAR(64), nodeurl VARCHAR(128), nodeuser VARCHAR(16), nodepasswd VARCHAR(16), partmtd INT, nodeid INT, partcol VARCHAR(32), partparam1 VARCHAR(32), partparam2 VARCHAR(32))""")
            connect.close()
        except pymysql.InternalError:
            pass

loadCSV(argv)
