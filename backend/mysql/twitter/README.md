CONFIGURE
==========
Set the mysql connection parameters in ``twitter.TwitterETL``

BUILD
=====
``mvn compile package``

RUN
===
1. Set up MySQL server (``tweet-init.sql``)
2. Set up an EMR cluster on Amazon
3. Add step with JAR: ``target/uber-twitter-0.0.1-SNAPSHOT.jar`` and arguments: ``twitter.TwitterETL s3n://15619f14twittertest2/600GB``
4. Create index after data is ready on MySQL (``tweet-after.sql``)
