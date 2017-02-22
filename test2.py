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

    nodes = []
    cmd = "select * from dtables"
    try:
        connect = pymysql.connect(catalog.hostname, catalog.username, catalog.passwd, catalog.db)
        cur = connect.cursor()
        cur.execute(cmd)
        data = cur.fetchall()
        for d in data:
            nodes.append(Node(str(d[0]), str(d[2]).split("/", 2)[2].split(":")[0], str(d[3]), str(d[4]), str(str(d[2]).split("/", 2)[2].split(":")[1].split("/")[1]), d[6], str(d[2]), str(str(d[2]).split("/", 2)[2].split(":")[1].split("/")[0])))
        connect.close()
    except pymysql.OperationalError:
        print("[", catalog.url, "]:", ddlfile, " failed.")

    # print("dtables contents................")
    # for n in nodes:
    #     n.displayNode()

    # Read csv file with python3
    csvcontents = []
    with open(csvfile) as c:
        filtered = (line.replace('\n','') for line in c)
        reader = csv.reader(filtered, delimiter=',')
        header = next(reader)
        for row in reader:
            if any(field.strip() for field in row):
                csvcontents.append(row)

    # print("csv contents.............")
    # for c in csvcontents:
    #     print(c)
    # print("end csv contents.............")

    if partmtd == 0:
        if len(csvcontents) != int(numnodes):
            print("Error")
        else:
            catalog.insert0(header, nodes, csvcontents, tname)
    elif partmtd == 1:
        if len(csvcontents) != int(numnodes):
            print("Error")
        else:
            for m in mtd1info:
                catalog.insert1(header, nodes, csvcontents, m, tname)
    elif partmtd == 2:
        for m in mtd2info:
            catalog.insert2(header, nodes, csvcontents, m, tname)
class Catalog:
    'Base Class for Catalog'
    def __init__(self, hostname, username, passwd, db, url):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.url = url
    def insert0(self, header, nodes, csvcontents, tname):
        for n in nodes:
            count = 0
            for c in csvcontents:
                if str(n.tname) == str(tname):
                    count += n.updateNode(', '.join("'{0}'".format(w.strip()) for w in c))
            print("[", n.url, "]:", count, " rows inserted.")
            if count > 0:
                self.updateCatalog(tname, n, 0, [])
                print("updating catalog for node ", n.url)
    def insert1(self, header, nodes, csvcontents, m, tname):
        # m = {desired node num, col, p1, p2}
        colindex = 0
        for h in header:
            if h == m[1]:
                break
            else:
                colindex = colindex + 1
        # select nodes from csvfile (stored in csvcontents) that match range partition (m)
        # 0-based colindex
        for n in nodes:
            count = 0
            for c in csvcontents:
                if int(m[2]) < int(c[colindex-1]):
                    if int(c[colindex-1]) <= int(m[3]):
                        if str(n.tname) == str(tname):
                            count += n.updateNode(', '.join("'{0}'".format(w.strip()) for w in c))
            print("[", n.url, "]:", count, " rows inserted.")
            if count > 0:
                self.updateCatalog(tname, n, 1, m)
                print("updating catalog for node ", n.url)
    def insert2(self, header, nodes, csvcontents, m, tname):
        print("insert2 ...........")
        # m = {col, p1}
        colindex = 0
        for h in header:
            if h == m[0]:
                break
            else:
                colindex = colindex + 1
        # select nodes from csvfile (stored in csvcontents) that match range partition (m)
        for n in nodes:
            count = 0
            for c in csvcontents:
                if int(c[colindex]) == (colindex % int(m[1])):
                    if str(n.tname) == str(tname):
                        count += n.updateNode(', '.join("'{0}'".format(w.strip()) for w in c))
            print("[", n.url, "]:", count, " rows inserted.")
            if count > 0:
                self.updateCatalog(tname, n, 2, m)
                print("updating catalog for node ", n.url)

    def displayCatalogInfo(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db)
    def updateCatalog(self, table, nodeinfo, mtd, methodinfo):
        if mtd == 0:
            nodeid = "NULL"
            partcol = "NULL"
            partp1 = "NULL"
            partp2 = "NULL"
            sql = "UPDATE dtables SET partmtd=%s, nodeid=%s, partcol=%s, partparam1=%s, partparam2=%s WHERE tname='%s'" % (mtd, nodeid, partcol, partp1, partp2, table)
        if mtd == 1:
            nodeid = methodinfo[0]
            partcol = methodinfo[1]
            partp1 = methodinfo[2]
            partp2 = methodinfo[3]
            sql = "UPDATE dtables SET partmtd='%s', nodeid='%s', partcol='%s', partparam1='%s', partparam2='%s' WHERE tname='%s'" % (mtd, nodeid, partcol, partp1, partp2, table)
        elif mtd == 2:
            nodeid = "NULL"
            partcol = methodinfo[0]
            partp1 = methodinfo[1]
            partp2 = "NULL"
            sql = "UPDATE dtables SET partmtd='%s', nodeid=%s, partcol='%s', partparam1='%s', partparam2=%s WHERE tname='%s'" % (mtd, nodeid, partcol, partp1, partp2, table)
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute(sql)
            connect.commit()
            connect.close()
        except pymysql.InternalError:
            print("InternalError")
        except pymysql.OperationalError:
            print("OperationalError")
    def createCatalog(self):
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute("""CREATE TABLE dtables (tname VARCHAR(32), nodedriver VARCHAR(64), nodeurl VARCHAR(128), nodeuser VARCHAR(16), nodepasswd VARCHAR(16), partmtd INT, nodeid INT, partcol VARCHAR(32), partparam1 VARCHAR(32), partparam2 VARCHAR(32))""")
            connect.close()
        except pymysql.InternalError:
            pass

class Node:
    'Base Class for Nodes'
    def __init__(self, tname, hostname, username, passwd, db, num, url, port):
        self.tname = tname.replace(" ", "")
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.num = num
        self.url = url.replace(" ", "")
        self.port = port.replace(" ", "")
    def displayNode(self):
        print("Table: ", self.tname, "Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db, " Num: ", self.num, " Url: ", self.url, " Port: ", self.port)
    def updateNode(self, values):
        # add information from csv
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            sql = "INSERT INTO %s VALUES (%s)" % (self.tname, values)
            cur.execute(sql)
            connect.commit()
            connect.close()
            return 1
        except pymysql.InternalError:
            return 0
        except pymysql.OperationalError:
            return 0

loadCSV(argv)
