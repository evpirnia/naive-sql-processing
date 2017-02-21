# Naive-SQL-Processing-for-Parallel-DBMS
https://lipyeow.github.io/ics421s17/morea/queryproc/experience-hw2.html

Using your code from Part 1 as a template, write a program runSQL that executes a given SQL statement on a cluster of computers each running an instance of a DBMS. The input to runSQL consists of two filenames (stored in variables clustercfg and sqlfile) passed in as commandline arguments. The file clustercfg contains access information for the catalog DB. The file sqlfile contains the SQL terminated by a semi-colon to be executed. The runSQL program will execute the same SQL on the database instance of each of the computers on the cluster (that holds data fragments for the table) concurrently using threads. One thread should be spawned for each computer in the cluster. The runSQL programm should output the rows retrieved to the standard output on success or report failure.<br />

You may assumed that the SQL queries only operate on single tables and do not contain any nested subqueries.<br />

You should consider using the ANTLR compiler compiler to generate a SQL parser that you can use to extract the table name.<br />

You may test your program on a single computer by using different databases to simulate the multiple computers.<br />
================================================================================

Install ANTLR:

OS X<br />
$ cd /usr/local/lib<br />
$ sudo curl -O http://www.antlr.org/download/antlr-4.6-complete.jar<br />
$ export CLASSPATH=".:/usr/local/lib/antlr-4.6-complete.jar:$CLASSPATH"<br />
$ alias antlr4='java -jar /usr/local/lib/antlr-4.6-complete.jar'<br />
$ alias grun='java org.antlr.v4.gui.TestRig'<br />

================================================================================
