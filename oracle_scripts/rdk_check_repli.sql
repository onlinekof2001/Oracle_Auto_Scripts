whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set head off
set feed off

define reference_tns_entry=&1
define schema_to_compare=&2
define output_folder=&3
set lines 512
column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
define logfile=to_del_check_repli_&schema_to_compare._&horodateur..log
prompt spooling to &output_folder./&logfile.
spool &output_folder./&logfile.

declare
st long;
begin
	st:='drop public database link &reference_tns_entry.';
	execute immediate st;
EXCEPTION
WHEN OTHERS THEN
   null;
 end;
/

declare
ref_server varchar2(30):='&reference_tns_entry.';
st long;
domain_name varchar2(30);
begin
    select value into domain_name from v$parameter where upper(name)='DB_DOMAIN';
	st:='create public database link &reference_tns_entry. using ''(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST='||substr('&reference_tns_entry.',instr('&reference_tns_entry.','_')+1,length('&reference_tns_entry.')-instr('&reference_tns_entry.','_'))||'.'||domain_name||')(PORT=1531)))(CONNECT_DATA=(SID=tetrix02)))''';
	execute immediate st;
EXCEPTION
WHEN OTHERS THEN
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM || 'qry='||st);
 end;
/

set head on
column stcom_reference heading "stcom_reference"  new_value stcom_reference;
select owner stcom_reference from dba_tables@&reference_tns_entry. where table_name=upper('id_schema_stcom') and rownum=1;

column pool_reference heading "pool_reference"  new_value pool_reference;
select lpad(ID_SCHEMA_STCOM,4,'0') pool_reference from &stcom_reference..id_schema_stcom@&reference_tns_entry.;

column reference_schema heading "reference_schema"  new_value reference_schema;
select distinct owner reference_schema from dba_mviews@&reference_tns_entry. where owner like 'MD&pool_reference.%' and rownum=1; 

prompt compare &schema_to_compare. with &reference_schema. on &reference_tns_entry.

col srv for a20
col owner for a20
set head on
prompt ###############
prompt table check
select 'local' srv,'TABLES',count(1) from dba_tables where owner=upper('&schema_to_compare.')
union
select  '&reference_tns_entry.' srv,'TABLES',count(1) from dba_tables@&reference_tns_entry. where owner=upper('&reference_schema.');
prompt ###############
prompt index check
select 'local' srv ,'INDEXES',count(1) from dba_indexes where owner=upper('&schema_to_compare.')
union
select '&reference_tns_entry.' srv ,'INDEXES',count(1) from dba_indexes@&reference_tns_entry. where owner=upper('&reference_schema.');
prompt ###############
prompt refresh group check
select owner,'local' srv ,'RG',rowner,rname,count(1) from dba_refresh_children where owner=upper('&schema_to_compare.') group by owner,rowner,rname
union
select owner,'&reference_tns_entry.' srv ,'RG',rowner,rname,count(1) from dba_refresh_children@&reference_tns_entry. where owner=upper('&reference_schema.') group by owner,rowner,rname;
prompt ###############
prompt grants check
define pad=20
col grantee for a20
col grantor for a20
col privilege for a20
col NB for a20
select rpad(grantee,&pad.) grantee,'local' srv ,grantor,rpad(privilege,&pad.) privilege,rpad(count(1),&pad.) nb from dba_tab_privs where owner=upper('&schema_to_compare.') group by grantee,owner,grantor,privilege
union
select rpad(grantee,&pad.) grantee,'&reference_tns_entry.' srv ,grantor,rpad(privilege,&pad.) privilege,rpad(count(1),&pad.) nb from dba_tab_privs@&reference_tns_entry. where owner=upper('&reference_schema.') group by grantee,owner,grantor,privilege
order by 1,2,3,4;
prompt ###############
prompt statistics
select owner,table_name,last_analyzed from dba_tables where owner=upper('&schema_to_compare.');

prompt ###############  
set serveroutput on
declare
ref_server varchar2(30):='&reference_tns_entry.';
ref_schema varchar2(30):='&schema_to_compare.';
st long;
num_error number:=0;
compare_schema exception;
begin
	for c in (select table_name from dba_tables@&reference_tns_entry. where owner=upper('&reference_schema.') minus select table_name from dba_tables where owner=upper('&schema_to_compare.'))
	loop
	 dbms_output.put_line('Table '||c.table_name||' exists on &reference_tns_entry. but not locally');	 
	 num_error:=num_error+1;
	end loop;
	for c in (select table_name from dba_tables where owner=upper('&schema_to_compare.') minus select  table_name from dba_tables@&reference_tns_entry. where owner=upper('&reference_schema.'))
	loop
	 dbms_output.put_line('Table NOK : '||c.table_name||' exists locally but not on &reference_tns_entry.');
	 num_error:=num_error+1;
	end loop;
	if num_error=0 then
	 dbms_output.put_line('Tables OK between '||ref_server||' and localy for schema '||ref_schema);
	end if;
	for c in (select table_name,count(1) as nb from dba_indexes@&reference_tns_entry. where owner=upper('&reference_schema.') group by table_name minus select table_name,count(1) as nb from dba_indexes where owner=upper('&schema_to_compare.') group by table_name)
	loop
	 dbms_output.put_line('index NOK on '||c.table_name);
	 num_error:=num_error+1;
	end loop;
	for c in (select grantee,privilege,table_name from dba_tab_privs@&reference_tns_entry. where owner=upper('&reference_schema.') minus select grantee,privilege,table_name from dba_tab_privs where owner=upper('&schema_to_compare.'))
	loop
	 dbms_output.put_line('privilege '||c.privilege||' on '||c.table_name||' exists for '||c.grantee||' on '||ref_server||' but not locally');
	 num_error:=num_error+1;
	end loop;
	for c in (select grantee,privilege,table_name from dba_tab_privs where owner=upper('&schema_to_compare.') minus select grantee,privilege,table_name from dba_tab_privs@&reference_tns_entry. where owner=upper('&reference_schema.'))
	loop
	 dbms_output.put_line('privilege '||c.privilege||' on '||c.table_name||' exists for '||c.grantee||' locally but not on '||ref_server);
	 num_error:=num_error+1;
	end loop;
	for c in (select table_name,last_analyzed from dba_tables where owner=upper('&schema_to_compare.') and (last_analyzed is null))
	loop
	 dbms_output.put_line('stats NOK '||c.table_name||' not analyzed ');
	 num_error:=num_error+1;
	end loop;
	if num_error>0 then 
		RAISE compare_schema;
	end if;	
exception
 WHEN compare_schema THEN
      raise_application_error (-20001,'compare not ok between '||ref_server||' and localy for schema '||ref_schema);
 WHEN OTHERS THEN
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM || 'qry='||st);
 end;	
/

prompt drop public db link to &reference_tns_entry. 
declare
st long;
begin
	st:='drop public database link &reference_tns_entry.';
	execute immediate st;
EXCEPTION
WHEN OTHERS THEN
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM || 'qry='||st);
 end;
/


spool off

