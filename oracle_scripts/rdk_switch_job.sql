--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_hold_flows.sql
-- SYNOPSIS:  hold flows
-- USAGE:      sqlplus -S -L /nolog @rdk_switch_job.sql
-- PARAMETERS:

--	Version
-- 	1.0  Creation	29/05/2016
--------------------------------------------------------------------------------

whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set head off
set feed off

define jobname=&1
define enable=&2

set lines 512

prompt &enable. job 
declare
st long;
begin
    for c in (select 'begin sys.dbms_scheduler.stop_job( '''||owner||'.'||job_name||''' ); end;' cmd from dba_scheduler_jobs where job_name=upper('&jobname.') and state='RUNNING')
	loop
	st:=c.cmd;
	dbms_output.put_line('executed: '||st);
	execute immediate st;
	end loop;
    for c in (select 'begin sys.dbms_scheduler.&enable.( '''||owner||'.'||job_name||''' ); end;' cmd from dba_scheduler_jobs where job_name=upper('&jobname.'))
	loop
	st:=c.cmd;
	dbms_output.put_line('executed: '||st);
	execute immediate st;
	end loop;
EXCEPTION
WHEN OTHERS THEN
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM || 'qry='||st);
 end;
/
