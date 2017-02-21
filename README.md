# Naive-SQL-Processing-for-Parallel-DBMS
https://lipyeow.github.io/ics421s17/morea/queryproc/experience-hw2.html

Using your code from Part 1 as a template, write a program runSQL that executes a given SQL statement on a cluster of computers each running an instance of a DBMS. The input to runSQL consists of two filenames (stored in variables clustercfg and sqlfile) passed in as commandline arguments. The file clustercfg contains access information for the catalog DB. The file sqlfile contains the SQL terminated by a semi-colon to be executed. The runSQL program will execute the same SQL on the database instance of each of the computers on the cluster (that holds data fragments for the table) concurrently using threads. One thread should be spawned for each computer in the cluster. The runSQL programm should output the rows retrieved to the standard output on success or report failure.<br />

You may assumed that the SQL queries only operate on single tables and do not contain any nested subqueries.<br />

You should consider using the ANTLR compiler compiler to generate a SQL parser that you can use to extract the table name.<br />

You may test your program on a single computer by using different databases to simulate the multiple computers.<br />

===========================

LOCAL MACHINE (macOS)

Install Homebrew:<br />
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"<br />

Install virtualbox & vagrant:<br />
$ brew cask install virtualbox<br />
$ brew cask install vagrant<br />
$ brew cask install vagrant-manager<br />

Install python3 if not already on machine: <br />
$ brew install python3<br />

============================

LOCAL MACHINE (Linux)<br />

Install virtualbox & vagrant:<br />
$ sudo apt install virtualbox<br />
$ sudo apt install vagrant<br />
$ sudo apt install vagrant-manager<br />

Install python3 if not already on machine:<br />
$ sudo apt install python3<br />

==========================

Install PyMySQL if not already on machine:<br />
$ pip3 install PyMySQL<br />

Create a Directory for the Catalog and each Node:<br />
$ mkdir catalog<br />
$ mkdir machine1<br />
$ mkdir machine2<br />

Clone all necessary files from the repo:<br />
1) test.py<br />
2) cluster.cfg<br />
3) sqlfile.sql<br />
4) run.sh<br />

Initialize a virtual machine in each directory:<br />
$ vagrant init ubuntu/xenial64<br />
$ vagrant up<br />
$ vagrant ssh<br />

Change to the /vagrant directory of each virtual machine<br />
$ cd /vagrant<br />

Open Vagrantfile in Each Directory:<br />
Replace line 25 with:<br />
  config.vm.network "forwarded_port", guest: 3306, host: 3306, auto_correct: true<br />
Replace line 29 with:<br />
  config.vm.network "private_network", ip: "ADDRESS_VALUE"<br />
Note: ADDRESS_VALUE depends on Directory:<br />
  /machine2, address_value = localhost_network.20<br />
  /machine1, address_value = localhost_network.10<br />
  /catalog, address_value = localhost_network.30<br />

Install MySQL in Each Directory:<br />
$ sudo apt-get install mysql-server<br />
Note: Password for MySQL-server: password<br />
$ /usr/bin/mysql_secure_installation<br />
Note: Respond No to everything but Remove test database and access to it, and Reload privilege tables<br />

Connect to MySQL in Each Directory:<br />
$ mysql -u root -p;<br />
Enter password: 'password'<br />

Create a database in Each Directory: <br />
NOTE: If a user already exists then drop it:<br />
mysql> drop user 'username'<br />
Create the database:<br />
mysql> create database TESTDB;<br />

Create Remote Users in Each Directory:<br />
mysql> use TESTDB;<br />
mysql> create user 'username';<br />
mysql> grant all on TESTDB.* to username@'%' identified by 'password';<br />
mysql> exit<br />
$ exit<br />

Insert the existing tables into dtables of the catalog machine:<br />
mysql> use TESTDB;<br />
mysql> insert into dtables values ('candy', NULL, 'jdbc:mysql://192.168.10.10:3306/TESTDB', 'evelynp', 'netflix', NULL, '1', NULL, NULL, NULL);<br />
mysql> insert into dtables values ('movies', NULL, 'jdbc:mysql://192.168.10.20:3306/TESTDB', 'blakela', 'hulu', NULL, '2', NULL, NULL, NULL);<br />
Note: The nodeurl has the specific user's ip address (ie. .10 or .20). nodeid, nodeuser, nodepasswd also correspond with the specific user. <br />

Run the script from your local host repo:<br />
./run.sh ./cluster.cfg ./sqlfile.sql<br />
