CONFIGURE
=========
Set the mysql connection parameters in ``edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ServerConfig``

BUILD
=====
``mvn compile package``

RUN
===
* Mixed-Q1Q2Q3Q4Q5Q6: ``java -cp target/uber-queryserver-0.0.1-SNAPSHOT.jar edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.QueryServer <port> <ip-addr-or-hostname-to-bind>``
