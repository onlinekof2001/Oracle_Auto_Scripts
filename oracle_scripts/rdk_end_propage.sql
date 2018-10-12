--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_end_propage.sql
-- SYNOPSIS:   rdk_end_propage
-- USAGE:      sqlplus -S -L @/tetrix02_rtdka2odbXX @rdk_prepare_propage.sql 
-- PARAMETERS: 
-- 
-- 
-- 
-- 

--	Version
-- 	1.0  Creation	01/03/2016
--------------------------------------------------------------------------------


define slave_schema=&1
define table_name=&2

whenever sqlerror continue
prompt execute rdk_end_propage


set serveroutput on
set verify off
set head off
set feed off

set lines 750
col ddl for A750 wor
set pages 0
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',FALSE);
set long 10000
set trimspool on
set wrap on

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
whenever sqlerror CONTINUE

set term off
col param_table_short_name noprint new_value table_short_name 
select substr('&table_name.',1,24) param_table_short_name from dual ;
set term on

prompt --recreate index on &table_name.
declare 
begin
  for c in (select replace(dbms_metadata.get_dependent_ddl('INDEX','TMP_&table_short_name.','&slave_schema.'),'TMP_','') cmd from dual)
  loop
   dbms_output.put_line(c.cmd);
   execute immediate(c.cmd);
  end loop;
  exception 
when others then null;  
end;
/

prompt --add grants to &table_name.
declare 
begin
  for c in (select replace(dbms_metadata.get_dependent_ddl('OBJECT_GRANT','TMP_&table_short_name.','&slave_schema.'),'TMP_','') cmd from dual)
  loop
   dbms_output.put_line(c.cmd);
   execute immediate(c.cmd);
  end loop;
  exception 
when others then null;  
end;
/

prompt --recreate synonyms for &table_name.
declare 
begin
  for c in (
  select 'create or replace synonym '
				||owner
				||'.'
				||synonym_name||' for ' 
				||table_owner||'.&table_name.' cmd				 
	from dba_synonyms 
	where table_name = upper('tmp_&table_short_name.') 
	and owner = upper('&slave_schema'))
  loop
   dbms_output.put_line(c.cmd);
   execute immediate(c.cmd);
  end loop;
  exception 
  when others then null;  
end;
/

drop table &slave_schema..tmp_&table_short_name.;


