create table test_street.Output_table_lucknow
		(
		  srno serial Not Null,
		  "STREET_NAM" text,
		  raw_name text,
		  raw_street_nme text,
		  "ADMIN_ROAD_NME" text,
		  "ADMIN_ALT_NME" text,
		  "L_LOC_NME" text,
		  "L_LOC_ID" integer,
		  "L_SUBL_NME" character varying(100),
		  "L_SUBL_ID" integer,
		  "L_SSLC_NME" character varying(100),
		  "L_SSLC_ID" integer,
		  "R_LOC_NME" text,
		  "R_LOC_ID" integer,
		  "R_SUBL_NME" character varying(100),
		  "R_SUBL_ID" integer,
		  "R_SSLC_NME" character varying(100),
		  "R_SSLC_ID" integer,
		  "Unmatch_String" text,
		  "Match_String" text,
		  status text,
		  "PIN_CD" character varying(100),
		  "N_LOC" text,
		  "N_LOC_MATCHED" text,
		  "N_LOC_MATCHED_ID" integer,
		  "N_SUBL_MATCHED" text,
		  "N_SUBL_MATCHED_ID" integer,
		  "N_SSLC_MATCHED" text,
		  "N_SSLC-MATCHED_ID" integer,
		  "M_N_LOC" text,
		  "M_N_LOC_MATCHED_ID" integer
		)
		
insert into test_street.Output_table_lucknow (srno,"STREET_NAM",raw_name,"PIN_CD") select srno,"STREET_NAM",raw_name,"PIN_CD" from test_street.unique_gstn_raw_lucknow


select * from test_street.Output_table_lucknow where coalesce(status,'')=''

 
select * from test_street.Output_table_lucknow where status='FUZZY_ROAD_MATCHED'



UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","L_LOC_NME"=t1."L_LOC_NME","L_LOC_ID"=t1."L_LOC_ID","R_LOC_NME"=t1."R_LOC_NME",
"R_LOC_ID"=t1."R_LOC_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_LOC_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME from (select * from test_street.Output_table_lucknow) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") 
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME )AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID"
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME from (select * from test_street.Output_table_lucknow  ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") 
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME )AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."R_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID") as t1
where  A.srno=t1.srno 


-------------------------------------------------------fuzzy_match_loc_road----------------------------------------------------
UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","L_LOC_NME"=t1."L_LOC_NME","L_LOC_ID"=t1."L_LOC_ID","R_LOC_NME"=t1."R_LOC_NME",
"R_LOC_ID"=t1."R_LOC_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_LOC_FUZZY_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME" ,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID" FROM
(select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.Output_table_lucknow where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."L_LOC_NME",'',1))||SOUNDEX(SPLIT_PART(T2."L_LOC_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME" ,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID"
union all
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME" ,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID" FROM 
(select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.Output_table_lucknow where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."R_LOC_NME",'',1))||SOUNDEX(SPLIT_PART(T2."R_LOC_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME" ,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID") as t1
where  A.srno=t1.srno 

-------------------------------------------------------------------------road_alt-----------------------------------------------------------------
UPDATE test_street.Output_table_lucknow A set "ADMIN_ALT_NME"=t1."ROAD_ALT","L_LOC_NME"=t1."L_LOC_NME","L_LOC_ID"=t1."L_LOC_ID","R_LOC_NME"=t1."R_LOC_NME",
"R_LOC_ID"=t1."R_LOC_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_LOC_FUZZY_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT" ,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."L_LOC_NME",'',1))||SOUNDEX(SPLIT_PART(T2."L_LOC_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT" ,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID"
union all
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT" ,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."R_LOC_NME",'',1))||SOUNDEX(SPLIT_PART(T2."R_LOC_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT" ,T2."L_LOC_NME",T2."R_LOC_NME",T2."L_LOC_ID",T2."R_LOC_ID") as t1
where  A.srno=t1.srno 

-----------subl_match----------------------------------------------
UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","L_SUBL_NME"=t1."L_SUBL_NME","L_SUBL_ID"=t1."L_SUBL_ID","R_SUBL_NME"=t1."R_SUBL_NME",
"R_SUBL_ID"=t1."R_SUBL_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_SUBL_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") 
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME )AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_SUBL_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID"
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") 
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME )AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."R_SUBL_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID") as t1
where  A.srno=t1.srno 

-------------------------------------------------------fuzzy_subl_loc-----------------------------------------------------------------------
UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","L_SUBL_NME"=t1."L_SUBL_NME","L_SUBL_ID"=t1."L_SUBL_ID","R_SUBL_NME"=t1."R_SUBL_NME",
"R_SUBL_ID"=t1."R_SUBL_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_SUBL_FUZZY_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME" ,T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_SUBL_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."L_SUBL_NME",'',1))||SOUNDEX(SPLIT_PART(T2."L_SUBL_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME" ,T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID"
union all
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME",T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID"FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."R_SUBL_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."R_SUBL_NME",'',1))||SOUNDEX(SPLIT_PART(T2."R_SUBL_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME" ,T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID") as t1
where  A.srno=t1.srno 


----------------------------------------------------------fuzzy_alt------------------------------------------------------------
UPDATE test_street.Output_table_lucknow A set "ADMIN_ALT_NME"=t1."ROAD_ALT","L_SUBL_NME"=t1."L_SUBL_NME","L_SUBL_ID"=t1."L_SUBL_ID","R_SUBL_NME"=t1."R_SUBL_NME",
"R_SUBL_ID"=t1."R_SUBL_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_SUBL_FUZZY_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT",T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_SUBL_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_ALT",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."L_SUBL_NME",'',1))||SOUNDEX(SPLIT_PART(T2."L_SUBL_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT",T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID"
union all
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT" ,T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") AND LOWER(T1.RAW_NAME)=LOWER(T2."R_SUBL_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_ALT",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."R_SUBL_NME",'',1))||SOUNDEX(SPLIT_PART(T2."R_SUBL_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT",T2."L_SUBL_NME",T2."R_SUBL_NME",T2."L_SUBL_ID",T2."R_SUBL_ID") as t1
where  A.srno=t1.srno 

-----------sslc_match----------------------------------------------
UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","L_SSLC_NME"=t1."L_SSLC_NME","L_SSLC_ID"=t1."L_SSLC_ID","R_SSLC_NME"=t1."R_SSLC_NME",
"R_SSLC_ID"=t1."R_SSLC_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_SSLC_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") 
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME )AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_SSLC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID"
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID" FROM (
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") 
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME )AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."R_SSLC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID") as t1
where  A.srno=t1.srno 




UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","L_SSLC_NME"=t1."L_SSLC_NME","L_SSLC_ID"=t1."L_SSLC_ID","R_SSLC_NME"=t1."R_SSLC_NME",
"R_SSLC_ID"=t1."R_SSLC_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_SSLC_FUZZY_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME",T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_SSLC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."L_SSLC_NME",'',1))||SOUNDEX(SPLIT_PART(T2."L_SSLC_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME" ,T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID"
union all
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME",T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") AND LOWER(T1.RAW_NAME)=LOWER(T2."R_SSLC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_NME",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."R_SSLC_NME",'',1))||SOUNDEX(SPLIT_PART(T2."R_SSLC_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_NME",T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID") as t1
where  A.srno=t1.srno 

----------------------------------------------------------fuzzy_alt------------------------------------------------------------
UPDATE test_street.Output_table_lucknow A set "ADMIN_ALT_NME"=t1."ROAD_ALT","L_SSLC_NME"=t1."L_SSLC_NME","L_SSLC_ID"=t1."L_SSLC_ID","R_SSLC_NME"=t1."R_SSLC_NME",
"R_SSLC_ID"=t1."R_SSLC_ID","Match_String"=(t1."STREET_NAM"||','||t1."raw_name")
,status='ROAD_SSLC_FUZZY_MATCHED',"Unmatch_String"='',raw_street_nme=t1."STREET_NAM" FROM (
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT",T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") AND LOWER(T1.RAW_NAME)=LOWER(T2."L_SSLC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_ALT",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."L_SSLC_NME",'',1))||SOUNDEX(SPLIT_PART(T2."L_SSLC_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT",T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID"
union all
SELECT t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT",T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID" FROM (select SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" from test_street.unique_gstn_raw_kanpur where SRNO not in (
SELECT T1.SRNO from (select * from test_street.Output_table_lucknow  where coalesce(status,'')='' ) AS T1 INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON
LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") AND LOWER(T1.RAW_NAME)=LOWER(T2."R_SSLC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_ALT",T1.RAW_NAME)) AS T1,
 MMI_MASTER."LUCKNOW_ROAD_NETWORK" T2 WHERE 
SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',1))||SOUNDEX(SPLIT_PART(T1."STREET_NAM",'',2))=
SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',1))||SOUNDEX(SPLIT_PART(T2."ROAD_ALT",'',2)) and
SOUNDEX(SPLIT_PART(T1."raw_name",'',1))||SOUNDEX(SPLIT_PART(T1."raw_name",'',2))=
SOUNDEX(SPLIT_PART(T2."R_SSLC_NME",'',1))||SOUNDEX(SPLIT_PART(T2."R_SSLC_NME",'',2))
GROUP BY t1.srno,T1."STREET_NAM",T1."raw_name",T2."ROAD_ALT",T2."L_SSLC_NME",T2."R_SSLC_NME",T2."L_SSLC_ID",T2."R_SSLC_ID") as t1
where  A.srno=t1.srno 

----------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME",status='ROAD_MATCHED_LOC_NOT'
,"Match_String"=(t1."STREET_NAM"),"Unmatch_String"=t1."raw_name",raw_street_nme=t1."STREET_NAM" from(
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME" FROM (select * from test_street.Output_table_lucknow where coalesce(status,'')='') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")<>LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME"
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME" FROM (select * from test_street.Output_table_lucknow where coalesce(status,'')='') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")<>LOWER(T2."R_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME") as t1
where  A.srno=t1.srno 


UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","ADMIN_ALT_NME"=t1."ROAD_ALT",status='ROAD_MATCHED_LOC_NOT' 
,"Match_String"=(t1."STREET_NAM"),"Unmatch_String"=t1."raw_name",raw_street_nme=t1."STREET_NAM" from(
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME"  FROM (select * from test_street.Output_table_lucknow where coalesce(status,'')='')AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") and LOWER(T1."raw_name")<>LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME" 
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME"  FROM (select * from test_street.Output_table_lucknow where coalesce(status,'')='')AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") and LOWER(T1."raw_name")<>LOWER(T2."R_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME" )as t1
where  A.srno=t1.srno 


select * FROM MMI_MASTER."LUCKNOW_ROAD_NETWORK"  where  "ROAD_NME"<>''order by "ROAD_NME"


UPDATE test_street.Output_table_lucknow A set "L_LOC_NME"=t1."L_LOC_NME","R_LOC_NME"=t1."R_LOC_NME",status='LOC_MATCHED_ROAD_NOT',
"Unmatch_String"=t1."ROAD_NME","Match_String"=(t1."raw_name") from(
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME" FROM (select * from test_street.Output_table_lucknow where coalesce(status,'')='') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")<>LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME"
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME" FROM (select * from test_street.Output_table_lucknow where coalesce(status,'')='') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")<>LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")=LOWER(T2."R_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME") as t1
where  A.srno=t1.srno 

UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME",status='ROAD_MATCHED_BY_PINCODE'
,"Unmatch_String"=t1."raw_name","Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
select T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t1."PIN_CD",t2."PINCODE" from
(SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE" FROM (select * from test_street.Output_table_lucknow where status='LOC_MATCHED_ROAD_NOT' ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")<>LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE"
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE" FROM (select * from test_street.Output_table_lucknow where status='LOC_MATCHED_ROAD_NOT') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")<>LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")=LOWER(T2."R_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE") as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") and t1."PIN_CD"=t2."PINCODE"
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME,t1."PIN_CD",t2."PINCODE") as t1
where  A.srno=t1.srno 



UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","ADMIN_ALT_NME"=t1."ROAD_ALT",status='ROAD_MATCHED_BY_PINCODE'
,"Unmatch_String"=t1."raw_name","Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
select T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t1."PIN_CD",t2."PINCODE" from
(SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE" FROM (select * from test_street.Output_table_lucknow where status='LOC_MATCHED_ROAD_NOT' ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")<>LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE"
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE" FROM (select * from test_street.Output_table_lucknow where status='LOC_MATCHED_ROAD_NOT') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")<>LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")=LOWER(T2."R_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE") as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") and t1."PIN_CD"=t2."PINCODE"
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t1."PIN_CD",t2."PINCODE") as t1
where  A.srno=t1.srno 


---------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE test_street.Output_table_lucknow SET status='NOT_MATCHED' where coalesce(status,'')=''


UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME",status='ROAD_MATCHED_LOC_NOT'
,"Unmatch_String"=t1."raw_name","Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME FROM (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") 
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",T1.RAW_NAME ) as t1
where  A.srno=t1.srno 


UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME", "ADMIN_ALT_NME"=t1."ROAD_ALT",status='ROAD_MATCHED_LOC_NOT'
,"Unmatch_String"=t1."raw_name","Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME FROM
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") 
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME ) as t1
where  A.srno=t1.srno 




UPDATE test_street.Output_table_lucknow A set "L_LOC_NME"=t1."L_LOC_NME","R_LOC_NME"=t1."R_LOC_NME",status='LOC_MATCHED_ROAD_NOT'
,"Unmatch_String"=t1."ROAD_NME","Match_String"=(t1."raw_name") from(
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_LOC_NME",t2."R_LOC_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."L_LOC_NME")  )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_LOC_NME",t2."R_LOC_NME",T2."ROAD_NME" ,t1.status
union all
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_LOC_NME",t2."R_LOC_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."R_LOC_NME") )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_LOC_NME",t2."R_LOC_NME",T2."ROAD_NME" ,t1.status ) as t1
where  A.srno=t1.srno


UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME",status='ROAD_MATCHED_BY_PINCODE'
,"Unmatch_String"=t1."raw_name","Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_LOC_NME",t2."R_LOC_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."L_LOC_NME")  )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_LOC_NME",t2."R_LOC_NME",T2."ROAD_NME" ,t1.status
union all
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_LOC_NME",t2."R_LOC_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."R_LOC_NME") )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_LOC_NME",t2."R_LOC_NME",T2."ROAD_NME" ,t1.status) as t1
where  A.srno=t1.srno 



UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME","ADMIN_ALT_NME"=t1."ROAD_ALT",status='ROAD_MATCHED_BY_PINCODE'
,"Unmatch_String"=t1."raw_name","Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
select T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t1."PIN_CD",t2."PINCODE" from
(SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE" FROM (select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")<>LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")=LOWER(T2."L_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE"
union all
SELECT T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE" FROM (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")<>LOWER(T2."ROAD_NME") and LOWER(T1."raw_name")=LOWER(T2."R_LOC_NME")
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t2."L_LOC_NME",t2."R_LOC_NME",t1."PIN_CD",t2."PINCODE") as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_ALT") and t1."PIN_CD"=t2."PINCODE"
GROUP BY T1.SRNO,T1."STREET_NAM",T2."ROAD_NME",t2."ROAD_ALT",T1.RAW_NAME,t1."PIN_CD",t2."PINCODE") as t1
where  A.srno=t1.srno 



UPDATE test_street.Output_table_lucknow A set "L_SUBL_NME"=t1."L_SUBL_NME","R_SUBL_NME"=t1."R_SUBL_NME",status='SUBL_MATCHED_ROAD_NOT'
,"Unmatch_String"=t1."ROAD_NME","Match_String"=(t1."raw_name") from(
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SUBL_NME",t2."R_SUBL_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."L_SUBL_NME") and t1."PIN_CD"=t2."PINCODE" )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SUBL_NME",t2."R_SUBL_NME",T2."ROAD_NME" ,t1.status
union all
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SUBL_NME",t2."R_SUBL_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."R_SUBL_NME") and t1."PIN_CD"=t2."PINCODE" )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SUBL_NME",t2."R_SUBL_NME",T2."ROAD_NME" ,t1.status ) as t1
where  A.srno=t1.srno 


UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME",status='ROAD_MATCHED_BY_PINCODE'
,"Unmatch_String"=t1."raw_name","Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
select T1.SRNO,T1."STREET_NAM",T1."ROAD_NME",T1.RAW_NAME,t1."PIN_CD",t1."PINCODE" from
(
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SUBL_NME",t2."R_SUBL_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."L_SUBL_NME") and t1."PIN_CD"=t2."PINCODE" )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SUBL_NME",t2."R_SUBL_NME",T2."ROAD_NME" ,t1.status
union all
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SUBL_NME",t2."R_SUBL_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."R_SUBL_NME") and t1."PIN_CD"=t2."PINCODE" )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SUBL_NME",t2."R_SUBL_NME",T2."ROAD_NME" ,t1.status )
as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") and t1."PIN_CD"=t2."PINCODE"
GROUP BY T1.SRNO,T1."STREET_NAM",T1."ROAD_NME",T1.RAW_NAME,t1."PIN_CD",t1."PINCODE") as t1
where  A.srno=t1.srno 




UPDATE test_street.Output_table_lucknow A set "L_SSLC_NME"=t1."L_SSLC_NME","R_SSLC_NME"=t1."R_SSLC_NME",status='SSLC_MATCHED_ROAD_NOT'
,"Unmatch_String"=t1."ROAD_NME","Match_String"=(t1."raw_name") from(
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SSLC_NME",t2."R_SSLC_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."L_SSLC_NME") and t1."PIN_CD"=t2."PINCODE" )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SSLC_NME",t2."R_SSLC_NME",T2."ROAD_NME" ,t1.status
union all
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SSLC_NME",t2."R_SSLC_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."R_SSLC_NME") and t1."PIN_CD"=t2."PINCODE" )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SSLC_NME",t2."R_SSLC_NME",T2."ROAD_NME" ,t1.status ) as t1
where  A.srno=t1.srno 

	
	
UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME",status='ROAD_MATCHED_BY_PINCODE'
,"Unmatch_String"=t1."raw_name","Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
select T1.SRNO,T1."STREET_NAM",T1."ROAD_NME",T1.RAW_NAME,t1."PIN_CD",t1."PINCODE" from
(
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SSLC_NME",t2."R_SSLC_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."L_SSLC_NME") and t1."PIN_CD"=t2."PINCODE" )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SSLC_NME",t2."R_SSLC_NME",T2."ROAD_NME" ,t1.status
union all
select t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SSLC_NME",t2."R_SSLC_NME",T2."ROAD_NME" ,t1.status from (
(select * from test_street.Output_table_lucknow where status='NOT_MATCHED' ) as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."raw_name")=LOWER(T2."R_SSLC_NME") and t1."PIN_CD"=t2."PINCODE" )
group by t1.srno,t1.raw_name,T1."STREET_NAM",t1."PIN_CD",t2."PINCODE",t2."L_SSLC_NME",t2."R_SSLC_NME",T2."ROAD_NME" ,t1.status)
as t1 inner join mmi_master."LUCKNOW_ROAD_NETWORK"
as t2 on LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") and t1."PIN_CD"=t2."PINCODE"
GROUP BY T1.SRNO,T1."STREET_NAM",T1."ROAD_NME",T1.RAW_NAME,t1."PIN_CD",t1."PINCODE") as t1
where  A.srno=t1.srno 





update test_street.Output_table_lucknow a set "STREET_NAM"=b.btrim from (
select srno,raw_name,"STREET_NAM" ,trim(trim(trim(trim(trim("STREET_NAM",'[,]'),'[-]'),'[ ]'),'[.]'),'\s+')from  test_street.Output_table_lucknow where status ='NOT_MATCHED'
)b where a."STREET_NAM"=b."STREET_NAM" and a.srno=b.srno



update test_street.Output_table_lucknow a set "STREET_NAM"=b.REGEXP_REPLACE from (
select srno,raw_name,"STREET_NAM",REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("STREET_NAM",'[,]',' ','g'),'[-]',' ','g')
,'[ ]',' ','g'),'[.]',' ','g'),'\s+',' ','g')from  test_street.Output_table_lucknow 
)b where a."STREET_NAM"=b."STREET_NAM" and a.srno=b.srno
	

SELECT STATUS,COUNT(*) FROM test_street.Output_table_lucknow GROUP BY STATUS	
 

UPDATE MMI_MASTER."KANPUR_ROAD_NETWORK" t1 SET "PINCODE"=t2."PINCODE" from (
	SELECT T1."ROAD_NME",T1."EDGE_ID",T2."STT_ID",t2."PINCODE"
	FROM MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T1,MMI_MASTER."PINCODE_BOUNDARY" AS T2
	WHERE ST_INTERSECTS(T1."SP_GEOMETRY",T2."SP_GEOMETRY")=TRUE AND T1."ROAD_NME"!=''
)as t2												
WHERE  LOWER(T1."ROAD_NME")= LOWER(T2."ROAD_NME") and (T1."EDGE_ID") =(T2."EDGE_ID")

UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME",status='FUZZY_ROAD_MATCHED'
,"Unmatch_String"=t1.remaining_string,"Match_String"=(t1."STREET_NAM1"),raw_street_nme=t1."STREET_NAM1" from(
select t1.remaining_string,t1.sim,t1.srno,t1."STREET_NAM",t1."STREET_NAM1",t1."ROAD_NME" from(
WITH TAB5 AS(
SELECT "STREET_NAM","ROAD_NME",t1.srno FROM( select "STREET_NAM","ROAD_NME",t1.srno from (
select t1.srno,t1."STREET_NAM",t2."ROAD_NME" from (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') as t1,
 mmi_master."LUCKNOW_ROAD_NETWORK" t2 where soundex(split_part(t1."STREET_NAM",'',1))||soundex(split_part(t1."STREET_NAM",'',2))
||soundex(split_part(t1."STREET_NAM",'',3))||soundex(split_part(t1."STREET_NAM",'',4))=
soundex(split_part(t2."ROAD_NME",'',1))||soundex(split_part(t2."ROAD_NME",'',2)) 
||soundex(split_part(t2."ROAD_NME",'',3))||soundex(split_part(t2."ROAD_NME",'',4))) as t1				   
group by "STREET_NAM","ROAD_NME",t1.srno	
UNION ALL
select "STREET_NAM","ROAD_NME",t1.srno from (
select t1.srno,t1."STREET_NAM",t2."ROAD_NME" from (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') as t1,
 mmi_master."LUCKNOW_ROAD_NETWORK" t2 where soundex(split_part(t1."STREET_NAM",'',1))||soundex(split_part(t1."STREET_NAM",'',2))
=soundex(split_part(t2."ROAD_NME",'',1))||soundex(split_part(t2."ROAD_NME",'',2))||soundex(split_part(t2."ROAD_NME",'',3))||soundex(split_part(t2."ROAD_NME",'',4)) 
) as t1				   
 group by "STREET_NAM","ROAD_NME",t1.srno															  
UNION ALL
select "STREET_NAM","ROAD_NME",t1.srno from (
select t1.srno,t1."STREET_NAM",t2."ROAD_NME" from (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') as t1,
 mmi_master."LUCKNOW_ROAD_NETWORK" t2 where soundex(split_part(t1."STREET_NAM",'',1))
=soundex(split_part(t2."ROAD_NME",'',1))||soundex(split_part(t2."ROAD_NME",'',2))||soundex(split_part(t2."ROAD_NME",'',3))
) as t1				   
 group by "STREET_NAM","ROAD_NME",t1.srno													  
UNION ALL
select "STREET_NAM","ROAD_NME",t1.srno from (
select t1.srno,t1."STREET_NAM",t2."ROAD_NME" from (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') as t1,
 mmi_master."LUCKNOW_ROAD_NETWORK" t2 where soundex(split_part(t1."STREET_NAM",'',1))||soundex(split_part(t1."STREET_NAM",'',2))
=soundex(split_part(t2."ROAD_NME",'',1))||soundex(split_part(t2."ROAD_NME",'',2))||soundex(split_part(t2."ROAD_NME",'',3))||soundex(split_part(t2."ROAD_NME",'',4))  
) as t1				   
 group by "STREET_NAM","ROAD_NME",t1.srno ORDER BY SRNO
) AS T1 GROUP BY "STREET_NAM","ROAD_NME",t1.srno),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,"STREET_NAM",ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"ROAD_NME" FROM(
SELECT SRNO,"STREET_NAM",UNNEST(STRING_TO_ARRAY("STREET_NAM",' ')) U1,UNNEST(STRING_TO_ARRAY("ROAD_NME",' ')) U2,"ROAD_NME"FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),' '),"ROAD_NME")*100 AS SIM,SRNO,"STREET_NAM",array_to_string(array_agg(U1 order by ID asc),' ')AS "STREET_NAM1","ROAD_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,"STREET_NAM","ROAD_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,"STREET_NAM",ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"ROAD_NME" FROM(
SELECT SRNO,"STREET_NAM",UNNEST(STRING_TO_ARRAY("STREET_NAM",' ')) U1,UNNEST(STRING_TO_ARRAY("ROAD_NME",' ')) U2,"ROAD_NME" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),' '),"ROAD_NME")*100 AS SIM,SRNO,"STREET_NAM",array_to_string(array_agg(U1 order by ID asc),' ')AS "STREET_NAM1","ROAD_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,"STREET_NAM","ROAD_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX("STREET_NAM") M3,MAX("STREET_NAM1") M4,MAX("ROAD_NME") FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'','g'),'[-]',' ','g'),'\s+',' ','g'),' ') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS "STREET_NAM",TAB4.M4 AS "STREET_NAM1",TAB4.MAX AS "ROAD_NME" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 37) as t1
where  A.srno=t1.srno 







UPDATE test_street.Output_table_lucknow A set "ADMIN_ALT_NME"=t1."ROAD_ALT",status='FUZZY_ROAD_MATCHED'
,"Unmatch_String"=t1.remaining_string,"Match_String"=(t1."STREET_NAM1"),raw_street_nme=t1."STREET_NAM1" from(
select t1.remaining_string,t1.sim,t1.srno,t1."STREET_NAM",t1."STREET_NAM1",t1."ROAD_ALT",t1."ROAD_NME" from(
WITH TAB5 AS(
SELECT "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno FROM( select "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno from (
select t1.srno,t1."STREET_NAM",t2."ROAD_ALT",t2."ROAD_NME" from (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') as t1,
 mmi_master."LUCKNOW_ROAD_NETWORK" t2 where soundex(split_part(t1."STREET_NAM",'',1))||soundex(split_part(t1."STREET_NAM",'',2))
||soundex(split_part(t1."STREET_NAM",'',3))||soundex(split_part(t1."STREET_NAM",'',4))=
soundex(split_part(t2."ROAD_ALT",'',1))||soundex(split_part(t2."ROAD_ALT",'',2)) 
||soundex(split_part(t2."ROAD_ALT",'',3))||soundex(split_part(t2."ROAD_ALT",'',4))) as t1				   
group by "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno	
UNION ALL
select "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno from (
select t1.srno,t1."STREET_NAM",t2."ROAD_ALT",t2."ROAD_NME" from (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') as t1,
 mmi_master."LUCKNOW_ROAD_NETWORK" t2 where soundex(split_part(t1."STREET_NAM",'',1))||soundex(split_part(t1."STREET_NAM",'',2))
=soundex(split_part(t2."ROAD_ALT",'',1))||soundex(split_part(t2."ROAD_ALT",'',2)) 
) as t1				   
 group by "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno															  
UNION ALL
select "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno from (
select t1.srno,t1."STREET_NAM",t2."ROAD_ALT",t2."ROAD_NME" from (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') as t1,
 mmi_master."LUCKNOW_ROAD_NETWORK" t2 where soundex(split_part(t1."STREET_NAM",'',1))
=soundex(split_part(t2."ROAD_ALT",'',1))||soundex(split_part(t2."ROAD_ALT",'',2))||soundex(split_part(t2."ROAD_ALT",'',3))
) as t1				   
 group by "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno													  
UNION ALL
select "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno from (
select t1.srno,t1."STREET_NAM",t2."ROAD_ALT",t2."ROAD_NME" from (select * from test_street.Output_table_lucknow where status='NOT_MATCHED') as t1,
 mmi_master."LUCKNOW_ROAD_NETWORK" t2 where soundex(split_part(t1."STREET_NAM",'',1))||soundex(split_part(t1."STREET_NAM",'',2))
=soundex(split_part(t2."ROAD_ALT",'',1))||soundex(split_part(t2."ROAD_ALT",'',2)) 
) as t1				   
 group by "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno ORDER BY SRNO
) AS T1 GROUP BY "STREET_NAM","ROAD_ALT","ROAD_NME",t1.srno),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,"STREET_NAM",ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"ROAD_ALT" FROM(
SELECT SRNO,"STREET_NAM",UNNEST(STRING_TO_ARRAY("STREET_NAM",' ')) U1,UNNEST(STRING_TO_ARRAY("ROAD_ALT",' ')) U2,"ROAD_ALT" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),' '),"ROAD_ALT")*100 AS SIM,SRNO,"STREET_NAM",array_to_string(array_agg(U1 order by ID asc),' ')AS "STREET_NAM1","ROAD_ALT" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,"STREET_NAM","ROAD_ALT" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,"STREET_NAM",ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"ROAD_ALT" FROM(
SELECT SRNO,"STREET_NAM",UNNEST(STRING_TO_ARRAY("STREET_NAM",' ')) U1,UNNEST(STRING_TO_ARRAY("ROAD_ALT",' ')) U2,"ROAD_ALT" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),' '),"ROAD_ALT")*100 AS SIM,SRNO,"STREET_NAM",array_to_string(array_agg(U1 order by ID asc),' ')AS "STREET_NAM1","ROAD_ALT" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,"STREET_NAM","ROAD_ALT" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX("STREET_NAM") M3,MAX("STREET_NAM1") M4,MAX("ROAD_ALT") FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'','g'),'[-]',' ','g'),'\s+',' ','g'),' ') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS "STREET_NAM",TAB4.M4 AS "STREET_NAM1",TAB4.MAX AS "ROAD_ALT",TAB4.MAX AS "ROAD_NME" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2

) as t1 where sim >= 35) as t1
where  A.srno=t1.srno 



SELECT * FROM test_street.Output_table_lucknow where STATUS ='NOT_MATCHED'



do $$
declare 
k integer;
begin 
for k in select srno from test_street.Output_table_lucknow where status ='NOT_MATCHED' order by srno 
loop
insert into test_street.gstn_lucknow_comb(srno,"STREET_NAM","STREET_NAM1")
select ct,combo,"STREET_NAM1" from(															 
with recursive t(srno,i,"STREET_NAM1") as (select srno, unnest(string_to_array(lower("STREET_NAM"),' ')) ,"STREET_NAM" AS "STREET_NAM1"
from test_street.Output_table_lucknow where status ='NOT_MATCHED' and srno=k),cte as									
(  
   	select  i as combo, i, srno as ct,"STREET_NAM1" from t
   	union all
	select cte.combo||' '||t.i,t.i,ct,T."STREET_NAM1" from cte join t on t.i>cte.i
	
),cte2 as 
(
	select  i as combo, i,srno as ct,"STREET_NAM1" from t
   	union all
	select cte2.combo||' '||t.i,t.i,ct,T."STREET_NAM1" from cte2 join t on t.i<cte2.i
)
select ct,combo,"STREET_NAM1" from cte2 union all select ct,combo,"STREET_NAM1" 
from cte union all select SRNO,lower("STREET_NAM"),"STREET_NAM" as "STREET_NAM"
from test_street.Output_table_lucknow where status ='NOT_MATCHED' and srno=k ) as S group by combo,ct,"STREET_NAM1" order by ct,combo,"STREET_NAM1";
end loop;
end;
$$




UPDATE test_street.Output_table_lucknow A set "ADMIN_ROAD_NME"=t1."ROAD_NME",status='ROAD_MATCHED_BY_COMBINATION'
,"Unmatch_String"=t1.rg,"Match_String"=(t1."STREET_NAM"),raw_street_nme=t1."STREET_NAM" from(
SELECT T1.SRNO,T1."STREET_NAM",T1."STREET_NAM1",T2."ROAD_NME",regexp_replace(trim(regexp_replace(lower(t1."STREET_NAM1"),lower("ROAD_NME"),'','g'),'  '),'\s+',' ','g') as rg
FROM (select * from test_street.gstn_lucknow_comb ) AS T1 
INNER JOIN MMI_MASTER."LUCKNOW_ROAD_NETWORK" AS T2 ON LOWER(T1."STREET_NAM")=LOWER(T2."ROAD_NME") 
GROUP BY T1.SRNO,T1."STREET_NAM",T1."STREET_NAM1",T2."ROAD_NME",rg) as t1
where  A.srno=t1.srno 



SELECT * FROM test_street.Output_table_lucknow where STATUS ='NOT_MATCHED'



select table_name from information_schema.tables where table_schema='tier3' and table_name like 'gstn_output%%'


select * from tier3.gstn_output_sitapur_up limit 10000
