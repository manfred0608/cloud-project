CONTENTS
===========
src : ETL for phase 1
    hbase : 
      ETL.java : main class for all the ETL jobs
      Extract.java : parse JSON and get useful data
      LoadMapper.java : own mapper class

    twitter : 
        censor : package used to do "censor" and "calculate sentiment score" jobs;

Phase2 : ETL for Q2 and Q3 of phase 2
    ETL.java : main class for all the ETL jobs
    Extract.java : parse JSON and get useful data
    ETLMapper.java : mapper class.
        input : raw data
        output : original tweeter - retweeter pair, retweeter - original tweeter pair
    ETLReducer : reducer class.
        generate query result as a volume of 'F'->'res'
        result sorted
        interactive users marked
        output : write to hbase directly