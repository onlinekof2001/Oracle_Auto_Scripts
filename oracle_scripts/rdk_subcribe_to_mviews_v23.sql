

--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: Thomas Delporte
-- SCRIPT:     rdk_subcribe_to_mviews.sql
-- SYNOPSIS:   create a replicated schema "schema_on_slave" on server "slave_tns_entry" based on "schema_on_master" on "master_tns_entry"
-- USAGE:      sqlplus -S -L /nolog @rdk_subcribe_to_mviews.sql xxxxx02_xxxxz1odb20 MD0000 xxxxx02_xxxxz2odb01 MD0000STCOM1 not_used.lst 4 d:\temp
-- PARAMETERS:
-- master_tns_entry= master tns name
-- schema_on_master= schema on master
-- slave_tns_entry= slave tns
-- schema_on_slave= schema name on slave
-- table_list= list of table none="ALL the schema" not_used.lst="list of table in file not_used.lst" no_filtered="list of table in no_filtered file"
-- frequence= refresh every X hours
-- output_folder= working folder


--  Actions performed example with MD0000 on master xxxxz1odb20 and MD0000STCOM1 on slave xxxxz2odb01
--   1. test sqlnet connections to xxxxz1odb20 & xxxxz2odb01
--   2. create same tbs on xxxxz2odb01 as the xxxxz1odb20: MD0000STCOM1_SNAP_DATA & MD0000STCOM1_SNAP_INDEX
--   3. create a user visu_MD0000_MD0000STCOM1 on xxxxz1odb20 (for refresh only) with only synonyms and grant select on MD0000
--   4. create a private db_link MD0000STCOM1.xxxxx02_xxxxz1odb20 to xxxxz1odb20. the db_link connects to visu_MD0000_MD0000STCOM1
--   5. create a locked schema MD0000STCOM1 on xxxxx02_xxxxz1odb20
--   6. if table_list=none then retreive all MD0000 's mviews. if table_list=not_used.lst: retreive only MD0000's mviews present in not_used.lst
--   7. add slave mviews to refresh group MD0000STCOM1.R_MD0000STCOM1 (create if not exists)
--   8. create refresh job every X hours


--	Version
-- 	1.0  Creation	20/01/2016
--------------------------------------------------------------------------------


whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set lines 512
set feed off


define master_tns_entry=&1
define schema_on_master=&2
define slave_tns_entry=&3
define schema_on_slave=&4
define table_list=&5
define frequence=&6
define output_folder=&7
define dba_user=&8
define dba_pwd=&9
define dryrun=&10
define db_link_owner_pwd=&11

define default_password_on_slave=Decathlon0147


define db_link_owner=visu_&schema_on_master.
define owner_rg_target=&schema_on_slave.
define rg_target=r_&schema_on_slave.

define db_link_name=MASTER_&schema_on_master.
define db_link_tns=&master_tns_entry.

--testing connection part
whenever sqlerror EXIT SQL.SQLCODE
prompt --testing connection @&slave_tns_entry. &dba_user.
connect &dba_user./&dba_pwd.@&slave_tns_entry.
prompt --testing connection @&master_tns_entry &dba_user.
connect &dba_user./&dba_pwd.@&master_tns_entry.

--declare variables part
set serveroutput on
set feed off
var slave_tns_entry varchar2(255);
exec :slave_tns_entry := '&slave_tns_entry.';
var master_tns_entry varchar2(255);
exec :master_tns_entry := '&master_tns_entry.';
var table_list varchar2(255);
exec :table_list := '&table_list';

var tbs_metadata clob;

--connect on master to get the tbs metadata
--spool to output file

--connect on slave and execute spooled file
prompt --connection @&slave_tns_entry.
connect &dba_user./&dba_pwd.@&slave_tns_entry.
set feed off
create or replace directory TOOLS as '/usr/local/sbin/oracle_tools/create_masterdata';
column db_name_on_slave heading "db_name_on_slave"  new_value db_name_on_slave;
select lower(name) as db_name_on_slave from v$database;

prompt --connecting to &master_tns_entry.
connect &dba_user./&dba_pwd.@&master_tns_entry.

set serveroutput on
set verify off
set feedback off

prompt --creating tbs...
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS_AS_ALTER',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',FALSE);

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
--define logfile=to_del_rdk_subcribe_to_mviews_&horodateur..log

column db_name_on_master heading "db_name_on_master"  new_value db_name_on_master;
select lower(name) as db_name_on_master from v$database;

column master_sql_file heading "master_sql_file"  new_value master_sql_file;
select 'to_del_master_&schema_on_slave._&horodateur..sql' as master_sql_file from dual;

column slave_sql_file heading "slave_sql_file"  new_value slave_sql_file;
select 'to_del_slave_&schema_on_slave._&horodateur..sql' as slave_sql_file from dual;

set feed off

spool &output_folder./&master_sql_file.
prompt set feed off
prompt --run as dba on master to
prompt --create visu_&schema_on_master for refresh process
declare
st long;
nb number;
begin
select count(1) into nb from dba_users where username=upper('&db_link_owner.');
if nb=0 then
st:='create user &db_link_owner. identified by "&db_link_owner_pwd." default tablespace dbtools temporary tablespace TEMP profile DEFAULT';
dbms_output.put_line(st||';');
dbms_output.put_line('grant connect to &db_link_owner.;');
else
dbms_output.put_line('--user &db_link_owner. already exists on master');
--st:='alter user &db_link_owner. identified by "&db_link_owner_pwd."';
--dbms_output.put_line(st||';');
end if;
end;
/

prompt --setting the grants for user &db_link_owner.
prompt set feed off
prompt prompt set grants
declare
st long;
begin
 for c in (select owner,table_name from dba_tables where upper(owner) = upper('&schema_on_master.') order by 2)
 loop
   dbms_output.put_line('grant select on '||c.owner||'.'||c.table_name||' to &db_link_owner.;');
 end loop;
end;
/

prompt --create synonyms from &db_link_owner. to &schema_on_master.
prompt set feed off
prompt prompt create synonymd for &db_link_owner. to &schema_on_master. mviews
declare
st long;
begin
 for c in (select owner,mview_name from dba_mviews where upper(owner) = upper('&schema_on_master.') order by 2)
 loop
   dbms_output.put_line('create or replace synonym &db_link_owner..'||c.mview_name||' for &schema_on_master..'||c.mview_name||';');
 end loop;
end;
/
spool off


set trimspool on
spool &output_folder./&slave_sql_file.
prompt --run as dba on slave
prompt --to create same tablespace on salve as master
prompt whenever sqlerror CONTINUE
declare
st clob;
begin
for c in (select distinct tablespace_name from dba_tablespaces where tablespace_name like upper('&schema_on_master._SNAP%'))
loop
select replace(replace(lower(replace(dbms_metadata.get_ddl('TABLESPACE', c.tablespace_name),lower('&db_name_on_master.'),lower('&db_name_on_slave.'))),'"',''),lower('&schema_on_master._'),lower('&schema_on_slave._')) into st FROM DUAL;
st:=substr(st,1,instr(st,';'));
dbms_output.put_line(st);
end loop;
end;
/

set trimspool on
whenever sqlerror continue
set serveroutput on
prompt set feed off
prompt --connect on slave as dba
--creating user &schema_on_slave. identified by "default_password_on_slave"
declare
begin
dbms_output.put_line('create user &schema_on_slave. identified by "&default_password_on_slave."
default tablespace &schema_on_slave._SNAP_DATA
profile DEFAULT;');
dbms_output.put_line( 'grant RESOURCE to &schema_on_slave.;');
dbms_output.put_line( 'grant CONNECT to &schema_on_slave.;');
dbms_output.put_line( 'grant CREATE DATABASE LINK to &schema_on_slave. ;');
dbms_output.put_line( 'grant create job to &schema_on_slave.;');
dbms_output.put_line( 'grant RESUMABLE to &schema_on_slave. ;');
dbms_output.put_line( 'grant CREATE MATERIALIZED VIEW to &schema_on_slave. ;');
dbms_output.put_line( 'grant execute on ORAEXPLOIT.PKG_DUREE to &schema_on_slave.;');
dbms_output.put_line( 'alter user &schema_on_slave. quota unlimited on &schema_on_slave._SNAP_DATA;');
dbms_output.put_line( 'alter user &schema_on_slave. quota unlimited on &schema_on_slave._SNAP_INDEX;');
dbms_output.put_line( 'create or replace directory TOOLS as ''/usr/local/sbin/oracle_tools/create_masterdata'';');
dbms_output.put_line( 'grant read,write on directory TOOLS to &schema_on_slave.;');
dbms_output.put_line( 'grant alter any materialized view to &schema_on_slave.;');
dbms_output.put_line( 'alter user &schema_on_slave. account unlock;');
end;
/

prompt --creating Refresh group,job,grants...
declare
st clob;
BEGIN
st:='
begin
DBMS_REFRESH.MAKE(name => ''&owner_rg_target..&rg_target.'',
        list => '''',
        next_date => null,
        interval =>'''',
        implicit_destroy => FALSE,
        lax => FALSE,job => 0,
        rollback_seg => NULL,
        push_deferred_rpc => TRUE,
        refresh_after_errors => TRUE,
        purge_option => NULL,
        parallelism => NULL,
        heap_size => NULL);
EXCEPTION
WHEN OTHERS THEN
  if sqlcode<>-23403 then
   dbms_output.put_line(''Exception SQLCODE='' || SQLCODE || ''  SQLERRM='' || SQLERRM);
  end if;
end;
/';
dbms_output.put_line(st);
dbms_output.put_line('commit;');
End;
/

whenever sqlerror EXIT SQL.SQLCODE --CONTINUE

prompt --connect on slave as replicated schema
prompt connect &schema_on_slave./&default_password_on_slave.@&slave_tns_entry.
set serveroutput on
set feed off
--creating database link &db_link_name. to &schema_on_master.
declare
st long;
begin
st:='create database link &db_link_name. connect to &db_link_owner. identified by "&db_link_owner_pwd." using ''&db_link_tns.''';
dbms_output.put_line(st||';');
End;
/


declare
st clob;
table_list varchar2(50):='&table_list.';
nb number;
begin
       if (table_list='none') then
	    begin
         execute immediate 'create table temp_list as select 1 as mview_name from dual';
		exception
         when others then null;
        end;
       end if;
       if (table_list<>'none') then
        begin
         execute immediate 'drop table temp_list';
         exception
         when others then null;
         end;
		 st:='CREATE TABLE temp_list (mview_name varchar2(100)
)
ORGANIZATION EXTERNAL (
Type oracle_loader
Default directory TOOLS
Location ('''||table_list||''')
) REJECT LIMIT UNLIMITED ';
	dbms_output.put_line(st||';');
	execute immediate(st);
end if;
end;
/



whenever sqlerror EXIT SQL.SQLCODE

set serveroutput on
set feed off
set verify off
prompt set feed off
declare
  st clob;
  --table_list varchar2(50):='&table_list.';
  nb number:=0;
begin
 --select count(*) into nb from temp_list where mview_name is not null;
 if (:table_list<>'none') then
  --if nb<>0 then
	for c in (select mview_name from temp_list where mview_name is not null) loop
	  dbms_output.put_line('prompt --creating '||c.mview_name);
	  st:='create materialized view &schema_on_slave..'||c.mview_name||' parallel 4 tablespace &schema_on_slave._snap_data build immediate using index tablespace &schema_on_slave._snap_index refresh force on demand as select * from '||c.mview_name||'@&db_link_name.';
	  dbms_output.put_line(st||';');
	  st:='begin DBMS_REFRESH.ADD(''&owner_rg_target..&rg_target.'',''&schema_on_slave..'|| c.mview_name ||''',TRUE); end;';
	  dbms_output.put_line(st);
	  dbms_output.put_line('/');
	end loop;
  --end if;
 end if;
 if (:table_list='none') then
  --if nb<>0 then
	for c in (select mview_name from dba_mviews where owner=upper('&schema_on_master.')) loop
	  dbms_output.put_line('prompt --creating '||c.mview_name);
	  st:='create materialized view &schema_on_slave..'||c.mview_name||' parallel 4 tablespace &schema_on_slave._snap_data build immediate using index tablespace &schema_on_slave._snap_index refresh force on demand as select * from '||c.mview_name||'@&db_link_name.';
	  dbms_output.put_line(st||';');
	  st:='begin DBMS_REFRESH.ADD(''&owner_rg_target..&rg_target.'',''&schema_on_slave..'|| c.mview_name ||''',TRUE); end;';
	  dbms_output.put_line(st);
	  dbms_output.put_line('/');
	end loop;
  --end if;
 end if;
 exception
 when others then dbms_output.put_line('Create mview : Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/
prompt --commit for refresh group modifications
prompt commit;;

declare
st clob;
BEGIN
st:='
begin
sys.dbms_scheduler.create_job(
job_name => ''&owner_rg_target..j_&owner_rg_target.'',
job_type => ''PLSQL_BLOCK'',
job_action => ''begin
execute immediate ''''alter session set REMOTE_DEPENDENCIES_MODE=SIGNATURE'''';
oraexploit.pkg_duree.init(''''REPLI_&schema_on_slave.'''');
oraexploit.pkg_duree.SNAP_INIT_TIME;
dbms_refresh.refresh(''''&owner_rg_target..r_&owner_rg_target.'''');
oraexploit.pkg_duree.SNAP_END_TIME;
oraexploit.pkg_duree.HISTO(true);
end;'',
repeat_interval => ''FREQ=HOURLY;INTERVAL=&frequence.'',
start_date => systimestamp at time zone ''Europe/Paris'',
job_class => ''"DEFAULT_JOB_CLASS"'',
comments =>
''&schema_on_slave. refresh job'',
auto_drop => FALSE,
enabled => FALSE);
sys.dbms_scheduler.set_attribute( name => ''&owner_rg_target..j_&owner_rg_target.'',
attribute => ''raise_events'', value => dbms_scheduler.job_failed + dbms_scheduler.job_broken);
sys.dbms_scheduler.enable( ''&owner_rg_target..j_&owner_rg_target.'');
EXCEPTION
WHEN OTHERS THEN
  if sqlcode<>-27477 then
   dbms_output.put_line(''Exception SQLCODE='' || SQLCODE || ''  SQLERRM='' || SQLERRM);
  end if;
end;
/';
dbms_output.put_line(st);
End;
/
spool off

sqlplus /nolog
var dry_run varchar2(255);
exec :dry_run := '&dry_run.';

prompt connecting as dba on master to execute @&output_folder./&master_sql_file.
connect &dba_user./&dba_pwd.@&master_tns_entry.
@&output_folder./&master_sql_file.

whenever sqlerror EXIT SQL.SQLCODE
connect &db_link_owner./&db_link_owner_pwd.@&master_tns_entry.

prompt connecting as dba on slave to execute @&output_folder./&slave_sql_file.
connect &dba_user./&dba_pwd.@&slave_tns_entry.
@&output_folder./&slave_sql_file.

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt --Finished.....................
exit
