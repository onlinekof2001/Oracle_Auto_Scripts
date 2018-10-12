set serveroutput on
set long 10000

define username=&1
define target_tns_entry=&2
define reference_tns_entry=&3
define output_folder=&4
define cible_username=&5
define dba_user=&6
define dba_password=&7

prompt testing connection on &reference_tns_entry.
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect &dba_user./&dba_password@&reference_tns_entry.
set serveroutput on
whenever sqlerror CONTINUE

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
define logfile=to_del_duplique_user._&horodateur..sql

exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS_AS_ALTER',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',FALSE);


col ddl for A380 wor
set long 10000
set heading off
set pages 0
set feedback off
set verify off
set wrap on

set lines 1024
spool &output_folder./&logfile.

declare
source varchar2(1000) default upper('&username.');
cible varchar2(1000) default upper('&cible_username.');
st clob;
st2 clob;
begin
	begin
	select username into st from dba_users where username=upper(source);
	exception	
	 WHEN NO_DATA_FOUND THEN
	  raise_application_error (-20001,source||' does not exists on &reference_tns_entry.');
    end;
	select dbms_metadata.get_ddl('USER', source) into st FROM DUAL;
	dbms_output.put_line(replace(st,source,cible));
	SELECT DBMS_METADATA.GET_GRANTED_DDL('ROLE_GRANT',source) into st FROM DUAL;
	dbms_output.put_line(replace(st,source,cible));
	SELECT DBMS_METADATA.GET_GRANTED_DDL('SYSTEM_GRANT',source) into st FROM DUAL;
	dbms_output.put_line(replace(st,source,cible));
	begin
	SELECT DBMS_METADATA.GET_GRANTED_DDL('OBJECT_GRANT',source) into st FROM DUAL;
	exception when others then null;
	end;
	dbms_output.put_line(replace(st,source,cible));
	if cible<>source then
		st:='alter user '||cible||' identified by "Decathlon01";';
		dbms_output.put_line(replace(st,source,cible));
	end if;
end;
/
spool off
prompt connection on &target_tns_entry.
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect &dba_user./&dba_password@&target_tns_entry.
set serveroutput on
prompt execute @&output_folder./&logfile.
@&output_folder./&logfile.

exit
