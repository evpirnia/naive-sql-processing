#!/usr/bin/python3
import pymysql
import threading
import sys
from sys import argv

def runDDL(argv):
    # Define variables
    hostname = ""
    username = ""
    passwd = ""
    url = ""
    port = ""
    db = ""
    num = 0

    # List of Nodes
    nodes = []

    # Takes in Command Line Arguments for files
    fname, clustercfg, ddlfile = argv

    # Reads clustercfg file line by line
    # Parses contents and creates a Cluster object for every catalog or node read in
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
                elif temp[0].split(".")[0].find("numnodes") > -1:
                    pass
                elif temp[0].split(".")[0].find("node") > -1:
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
                        num = num + 1
                        passwd = temp[1]
                        nodes.append(Node(hostname, username, passwd, db, num, url, port))

    # Reads ddlfile, Remove Whitespace, Remove new lines, Parse contents on ';'
    k = open(ddlfile, "r")
    sqlcmds = list(filter(None, k.read().strip().replace("\n","").split(';')))

    # Run Commands from ddlfile on each Thread
    threads = []
    table = ""
    for cmd in sqlcmds:
        # Find table name
        table = cmd.split("TABLE ")[1].split("(")[0]
        for n in nodes:
            threads.append(NodeThread(n, cmd, ddlfile).start())

    for n in nodes:
        catalog.updateCatalog(table, n)
    print("[", catalog.url, "]: catalog updated.")

    k.close()

def runCommands(node, cmd, ddlfile):
    try:
        connect = pymysql.connect(node.hostname, node.username, node.passwd, node.db)
        cur = connect.cursor()
        cur.execute(cmd)
        connect.close()
        print("[", node.url, "]:", ddlfile, " success.")
    except pymysql.InternalError:
        print("[", node.url, "]:", ddlfile, " failed.")
    except pymysql.OperationalError:
        print("[", node.url, "]:", ddlfile, " failed to connect to server.")

class NodeThread(threading.Thread):
    def __init__(self, node, cmd, ddlfile):
        threading.Thread.__init__(self)
        self.node = node
        self.cmd = cmd
        self.ddlfile = ddlfile
    def run(self):
        runCommands(self.node, self.cmd, self.ddlfile)

class Catalog:
    'Base Class for Catalog'
    def __init__(self, hostname, username, passwd, db, url):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.url = url
    def displayCatalog(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db)
    def updateCatalog(self, table, n):
        try:
            connect = pymysql.connect(self.hostname, self.username, self.passwd, self.db)
            cur = connect.cursor()
            cur.execute("""INSERT INTO dtables VALUES (%s, NULL, %s, %s, %s, NULL, %s, NULL, NULL, NULL)""", (table, n.url, n.username, n.passwd, n.num))
            connect.commit()
            connect.close()
        except pymysql.InternalError:
            print("Error")
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
    def __init__(self, hostname, username, passwd, db, num, url, port):
        self.hostname = hostname.replace(" ", "")
        self.username = username.replace(" ", "")
        self.passwd = passwd.replace(" ", "")
        self.db = db.replace(" ", "")
        self.num = num
        self.url = url.replace(" ", "")
        self.port = port.replace(" ", "")
    def displayNode(self):
        print("Hostname: ", self.hostname, " Username: ", self.username, " Passwd: ", self.passwd, " DB: ", self.db, " Num: ", self.num, " Url: ", self.url, " Port: ", self.port)

runDDL(argv)
