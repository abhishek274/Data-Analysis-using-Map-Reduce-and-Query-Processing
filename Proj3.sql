SQL> --Abhishek Duggirala(1002031522)
SQL> --Fetching distintct title types from TITLE_BASICS
SQL> select distinct(TITLETYPE) from  imdb00.TITLE_BASICS;

TITLETYPE                                                                       
--------------------------------------------------------------------------------
tvMovie                                                                         
tvSpecial                                                                       
tvPilot                                                                         
tvMiniSeries                                                                    
tvEpisode                                                                       
tvSeries                                                                        
movie                                                                           
video                                                                           
videoGame                                                                       
short                                                                           
tvShort                                                                         

11 rows selected.

SQL> --Fetching top 5 movies based on their movie rating which have received minimum of 100000 votes
SQL> --Tthe range of years is between 2000 and 2006
SQL> --Genres are Comedy and Romance
SQL> --And the records of type movie and tvMovie
SQL> SELECT A.PRIMARYTITLE,A.TITLETYPE, B.NUMVOTES, B.AVERAGERATING FROM imdb00.title_basics A JOIN  imdb00.title_ratings B ON A.TCONST=B.TCONST WHERE B.NUMVOTES>=100000 AND A.TITLETYPE IN('movie','tvMovie') AND A.STARTYEAR BETWEEN 2000 AND 2006 AND GENRES LIKE '%Comedy%' and GENRES LIKE '%Romance%'  ORDER BY B.AVERAGERATING DESC FETCH FIRST 5 ROWS ONLY;

PRIMARYTITLE                                                                    
--------------------------------------------------------------------------------
TITLETYPE                                                                       
--------------------------------------------------------------------------------
  NUMVOTES AVERAGERATING                                                        
---------- -------------                                                        
Amelie                                                                          
movie                                                                           
    748048           8.3                                                        
                                                                                
Good Bye Lenin!                                                                 
movie                                                                           
    145590           7.7                                                        

PRIMARYTITLE                                                                    
--------------------------------------------------------------------------------
TITLETYPE                                                                       
--------------------------------------------------------------------------------
  NUMVOTES AVERAGERATING                                                        
---------- -------------                                                        
                                                                                
Love Actually                                                                   
movie                                                                           
    476576           7.6                                                        
                                                                                
Sideways                                                                        
movie                                                                           

PRIMARYTITLE                                                                    
--------------------------------------------------------------------------------
TITLETYPE                                                                       
--------------------------------------------------------------------------------
  NUMVOTES AVERAGERATING                                                        
---------- -------------                                                        
    190672           7.5                                                        
                                                                                
Garden State                                                                    
movie                                                                           
    216016           7.4                                                        
                                                                                

SQL> --Generating plan for the above query
SQL> EXPLAIN PLAN FOR SELECT A.PRIMARYTITLE,A.TITLETYPE, B.NUMVOTES, B.AVERAGERATING FROM imdb00.title_basics A JOIN  imdb00.title_ratings B ON A.TCONST=B.TCONST WHERE B.NUMVOTES>=100000 AND A.TITLETYPE IN('movie','tvMovie') AND A.STARTYEAR BETWEEN 2000 AND 2006 AND GENRES LIKE '%Comedy%' and GENRES LIKE '%Romance%'  ORDER BY B.AVERAGERATING DESC FETCH FIRST 5 ROWS ONLY;

Explained.

SQL> --Printing the plan
SQL> SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

PLAN_TABLE_OUTPUT                                                               
--------------------------------------------------------------------------------
Plan hash value: 2653010624                                                     
                                                                                
--------------------------------------------------------------------------------
----------------                                                                
                                                                                
| Id  | Operation                      | Name          | Rows  | Bytes | Cost (%
CPU)| Time     |                                                                
                                                                                
--------------------------------------------------------------------------------
----------------                                                                
                                                                                

PLAN_TABLE_OUTPUT                                                               
--------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |               |     5 | 10280 |  5228  
 (1)| 00:00:01 |                                                                
                                                                                
|*  1 |  VIEW                          |               |     5 | 10280 |  5228  
 (1)| 00:00:01 |                                                                
                                                                                
|*  2 |   WINDOW SORT PUSHED RANK      |               |     5 |   575 |  5228  
 (1)| 00:00:01 |                                                                
                                                                                
|   3 |    NESTED LOOPS                |               |     5 |   575 |  5227  
 (1)| 00:00:01 |                                                                

PLAN_TABLE_OUTPUT                                                               
--------------------------------------------------------------------------------
                                                                                
|   4 |     NESTED LOOPS               |               |  2071 |   575 |  5227  
 (1)| 00:00:01 |                                                                
                                                                                
|*  5 |      TABLE ACCESS FULL         | TITLE_RATINGS |  2071 | 35207 |  1084  
 (2)| 00:00:01 |                                                                
                                                                                
|*  6 |      INDEX UNIQUE SCAN         | SYS_C00547784 |     1 |       |     1  
 (0)| 00:00:01 |                                                                
                                                                                
|*  7 |     TABLE ACCESS BY INDEX ROWID| TITLE_BASICS  |     1 |    98 |     2  

PLAN_TABLE_OUTPUT                                                               
--------------------------------------------------------------------------------
 (0)| 00:00:01 |                                                                
                                                                                
--------------------------------------------------------------------------------
----------------                                                                
                                                                                
                                                                                
Predicate Information (identified by operation id):                             
---------------------------------------------------                             
                                                                                
   1 - filter("from$_subquery$_004"."rowlimit_$$_rownumber"<=5)                 
   2 - filter(ROW_NUMBER() OVER ( ORDER BY INTERNAL_FUNCTION("B"."AVERAGERATING"

PLAN_TABLE_OUTPUT                                                               
--------------------------------------------------------------------------------
) DESC                                                                          
                                                                                
              )<=5)                                                             
   5 - filter("B"."NUMVOTES">=100000)                                           
   6 - access("A"."TCONST"="B"."TCONST")                                        
   7 - filter("A"."GENRES" LIKE U'%Comedy%' AND "A"."GENRES" LIKE U'%Romance%' A
ND                                                                              
                                                                                
              ("A"."TITLETYPE"=U'movie' OR "A"."TITLETYPE"=U'tvMovie') AND      
              TO_NUMBER("A"."STARTYEAR")>=2000 AND TO_NUMBER("A"."STARTYEAR")<=2
006 AND "A"."GENRES"                                                            

PLAN_TABLE_OUTPUT                                                               
--------------------------------------------------------------------------------
                                                                                
              IS NOT NULL AND "A"."GENRES" IS NOT NULL)                         
                                                                                
Note                                                                            
-----                                                                           
   - this is an adaptive plan                                                   

31 rows selected.

SQL> spool off
