--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_create_mviews_on_slave.sql
-- SYNOPSIS:  
-- USAGE:      
-- PARAMETERS:
-- 
-- 
-- 
-- 
-- 

--  Actions performed 
--  
--  

--	Version
-- 	1.0  Creation	20/01/2016
--------------------------------------------------------------------------------



whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set lines 512
define pool=&1
define slave_tns_entry=&2
define master_tns_entry=&3
define db_link_owner_pwd=Decathlon0147
define output_folder=&4

define owner_rg_target=MD0000STCOM
define rg_target=R_MD0000STCOM
define frequence=4


--testing connection part
prompt testing connection /@&slave_tns_entry._oraexploit
connect /@&slave_tns_entry._oraexploit
column schema_stcom heading "schema_stcom" new_value schema_stcom;
select username as schema_stcom from dba_users where username = 'STCOM&pool.' or username='STCOM'; 
column schema_nbo heading "schema_nbo" new_value schema_nbo;
select username as schema_nbo from dba_users where username = 'NBO&pool.' or username='NBO'; 
prompt testing connection /@&master_tns_entry._oraexploit
connect /@&master_tns_entry._oraexploit
prompt testing connection @&slave_tns_entry._&schema_stcom.
connect /@&slave_tns_entry._&schema_stcom.
prompt testing connection @&slave_tns_entry._&schema_nbo.
connect /@&slave_tns_entry._&schema_nbo.
whenever sqlerror CONTINUE
set serveroutput on
set feed off



connect /@&master_tns_entry._oraexploit
column md_master heading "md_master" new_value md_master;
col md_master for a10
SELECT username AS md_master
FROM
  (SELECT username
  FROM dba_users
  WHERE username IN
    (SELECT owner
    FROM dba_mviews
    WHERE rtrim(mview_name,'123456')='JM_RANKING_EN'
    AND owner LIKE '%&pool.%')
    ORDER BY CREATED DESC)
WHERE rownum=1;

var md_master varchar2(7);
exec :md_master := '&md_master.';

define db_link_owner=visu_&md_master.

prompt connecting to  /@&slave_tns_entry._oraexploit
connect /@&slave_tns_entry._oraexploit
set serveroutput on

declare 
several_user exception;
begin
for c in (select username as md_master,COUNT(*) over () tot_rows from dba_users where username in (select owner from dba_mviews where rtrim(mview_name,'123456789')='JM_RANKING_EN' and owner like '%&pool.%'))
loop
 if c.tot_rows>1 then
   RAISE several_user;
 end if;
end loop;
exception
 WHEN several_user THEN
      raise_application_error (-20001,'qry on &slave_tns_entry. : select username as md_master from dba_users where username in (select owner from dba_mviews where rtrim(mview_name)=''JM_RANKING_EN'' and owner like ''%&pool.%'' return more than 1 row');
end;
/


set feed off
--prompt testing connection &dba_user./&dba_pwd.@master_&mduser. ...
--connect &mduser./&mdpwduser.@master_&mduser.
column local_md_user heading "local_md_user" new_value local_md_user;
column mduser heading "mduser" new_value mduser;

--select '&md_master.STCOM' as local_md_user,'&md_master.' as mduser from dual;

--if it is the first time:
define mduser=&md_master.
define local_md_user=&md_master.STCOM

select '&md_master.' as mduser ,'MD&pool'||decode(rtrim(ltrim(username,'MD&pool.'),'STCOM'),'A','B','B','A')||'STCOM' as local_md_user 
from 
dba_users 
where username in (select owner from dba_mviews where rtrim(mview_name,'123456789')='JM_RANKING_EN' and owner like '%&pool.%') and rownum=1;



var mdexist varchar2(15);
exec :mdexist := '&local_md_user.';

--if local_md_user is null then default=MD&pool.ASTCOM
select decode(:mdexist,'','MD&pool.ASTCOM',:mdexist) local_md_user from dual;

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
define logfile=to_del_create_mviews_on_slave_&mduser._&horodateur..log
spool &output_folder./&logfile.

column tbs_data  heading "tbs_data"  new_value tbs_data;
column tbs_index  heading "tbs_index"  new_value tbs_index;
column db_name heading "db_name" new_value db_name;

select upper('&local_md_user._SNAP_DATA') as tbs_data from dual;
select upper('&local_md_user._SNAP_INDEX') as tbs_index from dual;
select lower(name) as db_name from v$database;

prompt creating tbs...
begin
if (upper(:md_master)='MD0000') or (upper(:md_master)='MASTERDATAS') then
 execute immediate 'CREATE SMALLFILE TABLESPACE "&tbs_data." DATAFILE ''/u01/app/oracle/oradata/data1/&db_name./&local_md_user._snap_data.data1'' SIZE 16M REUSE autoextend on next 128M maxsize 30G,''/u01/app/oracle/oradata/data1/&db_name./&local_md_user._snap_data.data2'' SIZE 16M REUSE autoextend on next 128M maxsize 30G,''/u01/app/oracle/oradata/data1/&db_name./&local_md_user._snap_data.data3'' SIZE 16M REUSE autoextend on next 128M maxsize 30G';
else
 execute immediate 'CREATE SMALLFILE TABLESPACE "&tbs_data." DATAFILE ''/u01/app/oracle/oradata/data1/&db_name./&local_md_user._snap_data.data1'' SIZE 1024M REUSE autoextend on next 128M maxsize 30G';
end if;
EXCEPTION
WHEN OTHERS THEN
  if SQLCODE<>-1543 then
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  end if;
end;
/

begin
if (upper(:md_master)='MD0000') or (upper(:md_master)='MASTERDATAS') then
 execute immediate 'CREATE SMALLFILE TABLESPACE "&tbs_index." DATAFILE ''/u01/app/oracle/oradata/index1/&db_name./&local_md_user._snap_index.data1'' SIZE 16M REUSE autoextend on next 128M maxsize 30G,''/u01/app/oracle/oradata/index1/&db_name./&local_md_user._snap_index.data2'' SIZE 16M REUSE autoextend on next 128M maxsize 30G,''/u01/app/oracle/oradata/index1/&db_name./&local_md_user._snap_index.data3'' SIZE 16M REUSE autoextend on next 128M maxsize 30G';
else
 execute immediate 'CREATE SMALLFILE TABLESPACE "&tbs_index." DATAFILE ''/u01/app/oracle/oradata/index1/&db_name./&local_md_user._snap_index.data1'' SIZE 1024M REUSE autoextend on next 128M maxsize 30G';
end if;
EXCEPTION
WHEN OTHERS THEN
  if SQLCODE<>-1543 then
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  end if;
end;
/

spool &output_folder./&logfile. append

whenever sqlerror EXIT SQL.SQLCODE
prompt creating user &local_md_user identified by "xxxxxxxxxxxx"
begin
execute immediate 'create user &local_md_user. identified by &db_link_owner_pwd.
default tablespace &local_md_user._SNAP_DATA
profile DEFAULT';
EXCEPTION
when others then
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
   raise;
end;
/
whenever sqlerror CONTINUE

prompt settings grants....
grant RESOURCE to &local_md_user.;
grant CONNECT to &local_md_user.;
grant CREATE DATABASE LINK to &local_md_user. ;
grant create job to &local_md_user.;
grant RESUMABLE to &local_md_user. ;
grant CREATE MATERIALIZED VIEW to &local_md_user. ;
grant execute on ORAEXPLOIT.PKG_DUREE to &local_md_user.;
alter user &local_md_user. quota unlimited on &local_md_user._SNAP_DATA;
alter user &local_md_user. quota unlimited on &local_md_user._SNAP_INDEX;
create or replace directory TOOLS as '/usr/local/sbin/oracle_tools/create_masterdata';
grant read,write on directory TOOLS to &local_md_user.;
grant alter any materialized view to &local_md_user.;

prompt connecting to &master_tns_entry._oraexploit 
prompt generating &output_folder./to_del_run_on_slave_&mduser._&horodateur..sql
spool off
connect /@&master_tns_entry._oraexploit
set feedback off
set serveroutput on
set trimspool on
set lines 550

spool &output_folder./to_del_run_on_slave_&mduser._&horodateur..sql
declare
  st clob;
begin
  dbms_output.put_line('spool &output_folder./to_del_run_on_slave_&mduser._&horodateur..log');
  for c in (select mview_name from dba_mviews where owner=upper('&mduser.')) loop
   st:='prompt creating '||c.mview_name||'...';
   dbms_output.put_line(st);
   dbms_output.put_line('BEGIN');
   st:='create materialized view &local_md_user..'||c.mview_name||' parallel 4 tablespace &local_md_user._snap_data build immediate using index tablespace &local_md_user._snap_index refresh force on demand as select * from '||c.mview_name||'@&master_tns_entry.';
   dbms_output.put_line('execute immediate('''||st||''');');
   dbms_output.put_line('EXCEPTION');
   dbms_output.put_line('WHEN OTHERS THEN');
   dbms_output.put_line('IF SQLCODE<>-12006 then');
   dbms_output.put_line('dbms_output.put_line(''Exception SQLCODE='' || SQLCODE || ''  SQLERRM='' || SQLERRM);');
   dbms_output.put_line('end if;');
   dbms_output.put_line('END;');
   dbms_output.put_line('/');
  end loop;
 dbms_output.put_line('spool off');
end;
/
spool off

prompt connecting to &slave_tns_entry. as &local_md_user.
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect &local_md_user./&db_link_owner_pwd.@&slave_tns_entry._oraexploit
whenever sqlerror CONTINUE
set serveroutput on
set feed off
spool &output_folder./&logfile. append
prompt creating database link master_&local_md_user. to master_&mduser.

declare
st long;
begin
 st:='drop database link &master_tns_entry.';
 begin
  execute immediate st;
 exception
 when others then null;
 end;
 st:='create database link &master_tns_entry. connect to &db_link_owner. identified by "&db_link_owner_pwd." using ''&master_tns_entry.''';
 execute immediate st;
 EXCEPTION
 WHEN OTHERS THEN
  if SQLCODE<>-2011 then
   dbms_output.put_line('Excepdtion SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
 end if;
End;
/

set feedback off
set serveroutput on
set trimspool on
set lines 550

spool off

prompt executing script to_del_run_on_slave_&mduser._&horodateur..sql
@&output_folder./to_del_run_on_slave_&mduser._&horodateur..sql
--prompt executing script run_on_slave_from_list_&mduser..sql
--@run_on_slave_from_list_&mduser..sql

prompt connecting to  /@&slave_tns_entry._oraexploit
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect /@&slave_tns_entry._oraexploit
--whenever sqlerror CONTINUE
set serveroutput on
set feed off

spool &output_folder./to_del_check_select_on_slave_&local_md_user._&horodateur..sql
declare
  st clob;
  i number:=0;
begin
  dbms_output.put_line('spool &output_folder./to_del_check_select_on_slave_&local_md_user._&horodateur..log');
  st:='whenever sqlerror EXIT SQL.SQLCODE';
  dbms_output.put_line(st);
  for c in (select owner,table_name from dba_tables where owner=upper('&local_md_user.')) loop
   st:='prompt selecting from '||c.table_name||'...';
   dbms_output.put_line(st);
   st:='select * from '||c.owner||'.'||c.table_name||' where rownum=1;';
   dbms_output.put_line(st);
   i:=i+1;
  end loop;
  if i=0 then
   dbms_output.put_line('no mviews => raise error');
  end if;
  dbms_output.put_line('spool off');
end;
/
spool off

spool &output_folder./&logfile. append

prompt setting grants to this role.
column old_md heading "old_md" new_value old_md;
select 'MD&pool'||decode(rtrim(ltrim('&local_md_user.','MD&pool.'),'STCOM'),'A','B','B','A')||'STCOM' as old_md from dual;
column old_md_master heading "old_md_master" new_value old_md_master;
select 'MD&pool'||decode(rtrim(ltrim('&local_md_user.','MD&pool.'),'STCOM'),'A','B','B','A') as old_md_master from dual;

declare
st clob;
begin
	for c in (select owner,table_name from dba_tables where owner=upper('&local_md_user.'))
	loop
		st:='grant select on '||c.owner||'.'||c.table_name||' to select_masterdatas';
		dbms_output.put_line(st);
		begin
		 execute immediate st;
		EXCEPTION WHEN OTHERS THEN null;
		end;
		st:='grant select on '||c.owner||'.'||c.table_name||' to sunopsis';
		dbms_output.put_line(st);
		begin
		 execute immediate st;
		EXCEPTION WHEN OTHERS THEN null;
		end;
		st:='grant select on '||c.owner||'.'||c.table_name||' to estcom0000';
		dbms_output.put_line(st);
		begin
		 execute immediate st;
		EXCEPTION WHEN OTHERS THEN null;
		end;
	end loop;
	st:='grant select_masterdatas to &local_md_user.,stcom,nbo';
	dbms_output.put_line(st);
	execute immediate st;
/*    dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',FALSE);
    SELECT DBMS_METADATA.GET_GRANTED_DDL('ROLE_GRANT','&old_md.') into st FROM DUAL;
	dbms_output.put_line('1 '||replace(st,'&old_md.','&local_md_user.'));
	if st<>'' then execute immediate st; end if;
	SELECT DBMS_METADATA.GET_GRANTED_DDL('SYSTEM_GRANT','&old_md.') into st FROM DUAL;
	dbms_output.put_line('2 '||replace(st,'&old_md.','&local_md_user.'));
	if st<>'' then execute immediate st; end if;
	SELECT DBMS_METADATA.GET_granted_DDL('TABLESPACE_QUOTA', '&old_md.') into st FROM dual;
	dbms_output.put_line('3 '||replace(st,'&old_md.','&local_md_user.'));
	if st<>'' then execute immediate st; end if;
	dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',FALSE);
    SELECT DBMS_METADATA.GET_GRANTED_DDL('OBJECT_GRANT','&old_md.') into st FROM DUAL;
	dbms_output.put_line('4 '||replace(st,'&old_md.','&local_md_user.'));
	if st<>'' then execute immediate st; end if;*/
	EXCEPTION
    WHEN OTHERS THEN
	if sqlcode<>-1917 then
     dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
    end if;
end;
/

prompt connect /@&slave_tns_entry._&schema_stcom. to check select on &local_md_user. tables
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect /@&slave_tns_entry._&schema_stcom.
whenever sqlerror CONTINUE
set serveroutput on
set feed off
@&output_folder./to_del_check_select_on_slave_&local_md_user._&horodateur..sql

prompt connect /@&slave_tns_entry._&schema_nbo. to check select on &local_md_user. tables
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect /@&slave_tns_entry._&schema_nbo.
--whenever sqlerror CONTINUE
set serveroutput on
set feed off
@&output_folder./to_del_check_select_on_slave_&local_md_user._&horodateur..sql

prompt connecting to  /@&slave_tns_entry._oraexploit to execute rdk_move_mviews_to_rgroup
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect /@&slave_tns_entry._oraexploit
@rdk_move_mviews_to_rgroup.sql &local_md_user. &owner_rg_target. &rg_target. &frequence. &output_folder.

prompt creating indexes - compute statistics... 
@rdk_create_indexes.sql &local_md_user.


select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
exit

