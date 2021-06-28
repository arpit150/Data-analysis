CREATE OR REPLACE FUNCTION mmi_master.make_view_poi(
	stat_code text,
	sch_nme text,
	view_sch text)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE tbl_nme_poi CHARACTER VARYING(50);
DECLARE t timestamptz := clock_timestamp();

DECLARE
RETURNVAL INTEGER;
sttcount integer;
i integer;
j integer;
k integer;
r record;
count integer;
tablename CHARACTER VARYING(50);
arr text [];
yyyy_mm varchar(254);
state_abbr Text;

DECLARE
conquery text;
conquery1 text;

BEGIN
    RETURNVAL = 0;
    yyyy_mm = to_char(now(),'yyyymmddhh24miss');
    RAISE WARNING 'yyyy_mm % AA :%',yyyy_mm,'';
	state_abbr = 'mmi_master."STATE_ABBR"';
	BEGIN
		If UPPER(stat_code) = 'ALL' Then
			k=0;
			EXECUTE FORMAT('SELECT count(*) FROM %1$s',state_abbr) INTO sttcount;
			Raise Info 'State Count is: %',sttcount;
			For r IN Execute FORMAT('select "STT_CODE" From %1$s',state_abbr)
			Loop
				stat_code=UPPER(r."STT_CODE");
				k=k+1;
				EXECUTE 'SELECT count(table_name) FROM information_schema.tables WHERE UPPER(table_name) LIKE '''||UPPER(stat_code)||'____POI'' AND TABLE_SCHEMA ='''||sch_nme||'''' into count;
				i=0;
				j=0;
				IF count > 1 THEN
					Raise Info 'Create View for State %',stat_code;
					-- tbl_nme_road=''|| UPPER(stat_code) ||'_ROAD_NETWORK';
					FOR r IN EXECUTE FORMAT('SELECT table_name FROM information_schema.tables WHERE UPPER(table_name) LIKE '''||UPPER(stat_code)||'____POI'' AND TABLE_SCHEMA ='''||sch_nme||''' ') 
					LOOP
						  tablename = UPPER(r.table_name);
						  arr[i]=tablename;
						  RAISE WARNING 'Count % AA :%',arr[i],'';
						  i:=i+1;
					END LOOP;
					i=i-1;
					conquery=' SELECT * FROM '||sch_nme||'."'||arr[0]||'" ';
					LOOP 
						EXIT WHEN i=0;
						conquery1='union all  SELECT * FROM '||sch_nme||'."'||arr[i]||'"';
						conquery = CONCAT(conquery,conquery1);
						i=i-1;
						-- RAISE WARNING 'QUERY % QUERY %',conquery,'';
					END LOOP;			
					EXECUTE 'drop VIEW if exists '||view_sch||'."'|| UPPER(stat_code) ||'_POI"';
					EXECUTE 'CREATE VIEW '||view_sch||'."'|| UPPER(stat_code) ||'_POI" As ('|| conquery||')';
					RETURNVAL = 1;
				END IF;
			END LOOP;	
		Else
			i=0;
			EXECUTE 'SELECT count(table_name) FROM information_schema.tables WHERE UPPER(table_name) LIKE '''||UPPER(stat_code)||'____POI'' AND TABLE_SCHEMA ='''||sch_nme||'''' into count;
			RAISE INFO 'count->%',count;
			IF count > 1 THEN 
				-- tbl_nme_road=''|| UPPER(stat_code) ||'_ROAD_NETWORK';
				FOR r IN EXECUTE FORMAT('SELECT table_name FROM information_schema.tables WHERE UPPER(table_name) LIKE '''||UPPER(stat_code)||'____POI'' AND TABLE_SCHEMA ='''||sch_nme||''' ')
				LOOP
					  tablename = UPPER(r.table_name);
					  arr[i]=tablename;
					  RAISE WARNING 'Count % AA :%',arr[i],'';
					  i:=i+1;
				END LOOP;
				i=i-1;  
				conquery=' SELECT * FROM '||sch_nme||'."'||arr[0]||'" ';
				LOOP 
					EXIT WHEN i=0;
					conquery1='union all  SELECT * FROM '||sch_nme||'."'||arr[i]||'"';
					conquery = CONCAT(conquery,conquery1);
					i=i-1;
					-- RAISE WARNING 'QUERY % QUERY %',conquery,'';
				END LOOP;				
				EXECUTE'drop VIEW if exists '||view_sch||'."'|| UPPER(stat_code)||'_POI"';
				EXECUTE'CREATE VIEW '||view_sch||'."'|| UPPER(stat_code) ||'_POI" As ('||conquery||')';				
			END IF;
			RAISE INFO 'check for POI network';
			RAISE NOTICE 'time spent =%', clock_timestamp() - t;			
		End If;
		RETURNVAL=1;
	END;	
	RETURN RETURNVAL;
END;
$BODY$