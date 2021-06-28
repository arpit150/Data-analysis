
select mmi_master.gstndata_street_final_test('street_raw_data','raw_street_ayodhya','277','10','ayodhya','15841',
										'mmi_master_road','UP_ROAD_NETWORK','AYODHYA_ADDR_ADMIN_R','street_output_data')


CREATE OR REPLACE FUNCTION mmi_master.gstndata_street_final_test(
	production_schema text,
	raw_table text,
	dst_id integer,
	stt_id integer,
	city_name text,
	city_id integer,
	master_schema_street text ,
	master_road_table text,
	admin_addr_table text,
	outputschema text DEFAULT 0)
    RETURNS text
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE 
f1 text; f2 text;
t1 text; t2 text;
DECLARE SQLQuery text;
error_tab_name text;
stat_code text;
master_db_table text;
outputtablename text;
BEGIN 
error_tab_name = 'gstn_output_data.gstn_error';

stat_code = UPPER(LEFT(UPPER(master_road_table), 2));
RAISE INFO 'State Code -> %', stat_code;

outputtableName = 'gstn_output_'||Replace(city_name,' ','')||'_'||Replace(lower(stat_code),' ','');
RAISE INFO 'outputtableName Code -> %', outputtableName;

SQLQuery = 'Drop Table If Exists '||production_schema||'."'|| raw_table ||'"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='CREATE TABLE '||production_schema||'."'|| raw_table ||'"
(
    srno integer,
    "BLDG_NAM" character varying COLLATE pg_catalog."default",
    "STREET_NAM" character varying COLLATE pg_catalog."default",
    raw_name character varying COLLATE pg_catalog."default",
    "PIN_CD" character varying COLLATE pg_catalog."default",
    "Address" character varying COLLATE pg_catalog."default",
    "houseName" character varying COLLATE pg_catalog."default",
    poi character varying COLLATE pg_catalog."default",
    street character varying COLLATE pg_catalog."default",
    "subSubLocality" character varying COLLATE pg_catalog."default",
    "subLocality" character varying COLLATE pg_catalog."default",
    locality character varying COLLATE pg_catalog."default",
    village character varying COLLATE pg_catalog."default",
    "subDistrict" character varying COLLATE pg_catalog."default",
    district character varying COLLATE pg_catalog."default",
    city character varying COLLATE pg_catalog."default",
    state character varying COLLATE pg_catalog."default"
)';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='CREATE INDEX indx_srno_'||city_name||'
    ON '||production_schema||'."'|| raw_table ||'" USING btree
    (srno)
    TABLESPACE pg_default';
	
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
    
    
SQLQuery='INSERT INTO '||production_schema||'."'|| raw_table ||'"(
srno, "BLDG_NAM", "STREET_NAM",raw_name, "PIN_CD", "Address", "houseName", poi, street, "subSubLocality", "subLocality", locality, village, "subDistrict", district, city, state)
select row_number() over() as srno,"BLDG_NAM","STREET_NAM","AREA_NAM" as raw_name,"PIN_CD","Address","houseName",poi,street,"subSubLocality","subLocality","locality",village,
"subDistrict",district,city,state from gstn_raw_data.new_upload_standardize where city ilike ''%'||Replace(city_name,'_',' ')||'%'' and state ilike ''%'||stat_code||'%'' ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
    

SQLQuery='select mmi_master.gstndata_after_cleaning_process('''||production_schema||''','''|| raw_table ||''','''|| dst_id ||''','''|| stt_id ||''','''||city_name||''')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
    
	
SQLQuery='select mmi_master.gstndata_after_cleaning_street_process('''||production_schema||''','''|| raw_table ||''','''|| dst_id ||''','''|| stt_id ||''','''||city_name||''')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;	
	

SQLQuery = 'Drop Table If Exists  '||production_schema||'."unique_'|| raw_table ||'"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
	
SQLQuery='create table '||production_schema||'."unique_'|| raw_table ||'" as 
			SELECT  SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" FROM  (
			SELECT SRNO,UPPER(REGEXP_REPLACE(REGEXP_REPLACE("STREET_NAM",''[-]'','' '',''g''),''\s+'','' '',''g'')) AS "STREET_NAM",
			RAW_NAME,"PIN_CD" FROM '||production_schema||'."'|| raw_table ||'" AS T1,MMI_MASTER."ROAD_TYPE" AS T2 WHERE T1."STREET_NAM" ILIKE  ''%''||"RD_TYP"||''%''
			GROUP BY SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" ) AS T1 
			GROUP BY SRNO,"STREET_NAM",RAW_NAME,"PIN_CD" ';
	
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
	
		--CREATE MASTER DB CITY TABLE
		master_db_table = UPPER(city_name)||'_ROAD_NETWORK';
		sqlquery = FORMAT('Drop Table If Exists %1$s."%2$s"',master_schema_street,master_db_table);
		RAISE INFO 'SQL_STATEMENT:%',sqlquery;
		EXECUTE sqlquery;
		
		sqlquery = FORMAT('CREATE TABLE IF NOT EXISTS '||master_schema_street||'."%1$s" AS SELECT "EDGE_ID", "ROAD_NME", "ROAD_BSE", "ROAD_TYP", "TYP_POS", "ROAD_ALT", "SPL_NME", "ROUTE_NO", "ALTROUT_NO", "NEWROUT_NO", "FTR_CRY", "FOW_PREV", "FOW_NME", "BP_CRY", "BP_NME", "ONE_WAY", "DIVIDER", "SPD_LMT", "SPD_M", "PUBVSPVT", "PVDVSUVD", "MOTORABLE", "MOT_M", "ROUTABLE", "ROUTABLE_M", "FRC", "FRC_M", "CONST_ST", "PARKING", "FT", "EXP", "TOLL_RD", "TOLL", "TOLL_NME", "PJ", "MD", "P_SPDLMT_M", "PMBJP_TYPE", "FRM_JNC", "TO_JNC", "FROM_ELEV", "TO_ELEV", "STT_ID", "CITY_ID", "L_STT_ID", "R_STT_ID", "L_DIST_ID", "R_DIST_ID", "L_SDB_ID", "R_SDB_ID", "L_CITY_ID", "R_CITY_ID", "L_ADMIN_ID", "R_ADMIN_ID", "PROC_STAT", "PROC_DATE", "TMC_ID", "MD_GRP_ID", "EXCP", "SPL_REMARK", "REMARK1", "REMARK2", "REMARK3", "MI_STYLE", "MI_PRINX", "SP_GEOMETRY"  FROM %2$s."%3$s" WHERE "CITY_ID"=%4$s',master_db_table,master_schema_street,master_road_table,city_id);
		EXECUTE sqlquery;
	
	
SQLQuery = 'ALTER TABLE '||master_schema_street||'."'|| master_db_table ||'" ADD COLUMN 
			"L_SUBL_ID" integer,
			ADD COLUMN "L_SUBL_NME" text,
			ADD COLUMN "L_SSLC_ID" integer,
			ADD COLUMN "L_SSLC_NME" text,
			ADD COLUMN "L_LOC_ID" integer,
			ADD COLUMN "L_LOC_NME" text,
			ADD COLUMN "R_SUBL_ID" integer,
			ADD COLUMN "R_SUBL_NME" text,
			ADD COLUMN "R_SSLC_ID" integer,
			ADD COLUMN "R_SSLC_NME" text,
			ADD COLUMN "R_LOC_ID" integer,
			ADD COLUMN "R_LOC_NME" text,
	 		ADD COLUMN "PINCODE" varchar';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

		
SQLQuery = 'UPDATE '||master_schema_street||'."'|| master_db_table ||'" t1 SET "PINCODE"= t2."PIN1"
			FROM MMI_MASTER."'|| admin_addr_table ||'" t2 WHERE t1."L_ADMIN_ID"=t2."SSLC_ID"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;


SQLQuery ='UPDATE '||master_schema_street||'."'|| master_db_table ||'"t1 SET "PINCODE"= t2."PIN1"
			FROM MMI_MASTER."'|| admin_addr_table ||'" t2 WHERE t1."L_ADMIN_ID"=t2."SUBL_ID"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;


SQLQuery ='UPDATE '||master_schema_street||'."'|| master_db_table ||'" t1 SET "PINCODE"= t2."PIN1"
			FROM MMI_MASTER."'|| admin_addr_table ||'" t2 WHERE t1."L_ADMIN_ID"=t2."LOC_ID"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

	
SQLQuery ='UPDATE '||master_schema_street||'."'|| master_db_table ||'" t1 SET "L_SUBL_ID"=t2."SUBL_ID","L_LOC_ID"=t2."LOC_ID","L_SSLC_ID"=t2."SSLC_ID",
		  "L_SUBL_NME"=t2."SUBL_NME","L_LOC_NME"=t2."LOC_NME","L_SSLC_NME"=t2."SSLC_NME"
		   FROM MMI_MASTER."'|| admin_addr_table ||'" t2 WHERE t1."L_ADMIN_ID"=t2."SSLC_ID"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;



SQLQuery ='UPDATE '||master_schema_street||'."'|| master_db_table ||'" t1 SET "R_SUBL_ID"=t2."SUBL_ID","R_LOC_ID"=t2."LOC_ID","R_SSLC_ID"=t2."SSLC_ID",
		  "R_SUBL_NME"=t2."SUBL_NME","R_LOC_NME"=t2."LOC_NME","R_SSLC_NME"=t2."SSLC_NME"
		   FROM MMI_MASTER."'|| admin_addr_table ||'" t2 WHERE t1."R_ADMIN_ID"=t2."LOC_ID"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;



SQLQuery ='select mmi_master.gstndata_street_process('''||production_schema||''','''|| raw_table ||''','''||outputschema||''','''||city_name||'''
										 ,'''||UPPER(city_name)||'_ROAD_NETWORK'','''||master_schema_street||''')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
										  
    
	
RETURN 1;
	EXCEPTION
	WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS 
		f1=MESSAGE_TEXT,
		f2=PG_EXCEPTION_CONTEXT; 
		RAISE info 'error caught:%',f1;
		RAISE info 'error caught:%',f2;
		--SQLQuery = FORMAT('INSERT INTO %1$s (table_name,table_schema,message,context) Values(''%2$s'',''%3$s'',''%4$s'',''%5$s'')',error_tab_name,output_table,outputschema,f1,f2);
		--EXECUTE SQLQuery;
		
	RETURN -1;
END

$BODY$;