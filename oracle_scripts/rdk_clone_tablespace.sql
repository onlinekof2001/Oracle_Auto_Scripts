--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_clone_tablespace.sql
-- SYNOPSIS:  hold flows
-- USAGE:      sqlplus -S -L /nolog @rdk_clone_tablespace.sql tech02_rtdkz2odb30 chega0032 tetrix02_rtdkz2odb01 0 d:\temp
-- PARAMETERS:
-- 
-- 
-- 
-- 
-- 

--  Actions performed 
--   1. create a identical tbs

--	Version
-- 	1.0  Creation	21/01/2016
--------------------------------------------------------------------------------


whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set lines 512

define server_tns_entry=&1
define reference_tns_entry=&2
define source_tbs_name=&3
define target_tbs_name=&4
define output_folder=&5

--testing connection part
whenever sqlerror EXIT SQL.SQLCODE
prompt testing connection /@&reference_tns_entry._oraexploit
connect /@&reference_tns_entry._oraexploit
column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
whenever sqlerror CONTINUE

set serveroutput on
set feed off
set echo off
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS_AS_ALTER',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',FALSE);

set trimspool on
prompt generating &output_folder./to_del_clone_tbs_&horodateur..sql
spool &output_folder./to_del_clone_tbs_&horodateur..sql
declare
st clob;
begin
 dbms_output.put_line('set serveroutput on');
 for c in (select distinct tablespace_name from dba_tablespaces where tablespace_name=upper('&source_tbs_name'))
 loop
  select dbms_metadata.get_ddl('TABLESPACE', c.tablespace_name) into st FROM DUAL;
  st:=replace(replace(lower(st),lower('&source_tbs_name.'),lower('&target_tbs_name.')),'"','');
  dbms_output.put_line('begin');
  while instr(st,';')>0 loop 
   dbms_output.put_line('execute immediate (q''['||substr(st,1,instr(st,';')-1)||']'');');
   st:=substr(st,instr(st,';')+1,length(st)-instr(st,';')-1);
  end loop;
  dbms_output.put_line('execute immediate (q''['||substr(st,instr(st,';')+1,length(st)-instr(st,';')-1)||']'');');
  dbms_output.put_line('EXCEPTION');
  dbms_output.put_line('WHEN OTHERS THEN');
  dbms_output.put_line('IF SQLCODE<>-1543 then');
  dbms_output.put_line('dbms_output.put_line(''Exception SQLCODE='' || SQLCODE || ''  SQLERRM='' || SQLERRM);');
  dbms_output.put_line('end if;');
  dbms_output.put_line('END;');
  dbms_output.put_line('/');
 end loop;
end;
/
spool off


connect /@&server_tns_entry._oraexploit
prompt execute @&output_folder./to_del_clone_tbs_&horodateur..sql
@&output_folder./to_del_clone_tbs_&horodateur..sql
