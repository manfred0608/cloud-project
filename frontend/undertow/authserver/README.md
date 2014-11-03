CONFIGURE
=========
Set the mysql connection parameters in ``dbserver.ConnectionPooler``

BUILD
=====
``mvn compile package``

RUN
===
* Q1: ``java -cp target/uber-authserver-0.0.1-SNAPSHOT.jar authserver.QueryServer <port> <ip-addr-or-hostname-to-bind>``
* Q2-MySQL: ``java -cp target/uber-authserver-0.0.1-SNAPSHOT.jar dbserver.QueryServer <port> <ip-addr-or-hostname-to-bind>``
