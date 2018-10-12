-- grant alter any materialized view to dba;
-- grant select on sys.user$ to dba;
set verify off
set serveroutput on
set head off
set feed off
set lines 512

define mduser=&1
define owner_rg_target=&2
define rg_target=&3
define frequence=&4


column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;

prompt creating Refresh group,job,grants...
BEGIN
        DBMS_REFRESH.MAKE(name => '&owner_rg_target..&rg_target.',
        list => '',
        next_date => null,
        interval =>'',
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
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  end if;
End;
/

BEGIN
sys.dbms_scheduler.create_job(
job_name => '&owner_rg_target..J_&owner_rg_target.',
job_type => 'PLSQL_BLOCK',
job_action => 'begin
execute immediate ''alter session set REMOTE_DEPENDENCIES_MODE=SIGNATURE'';
oraexploit.pkg_duree.init(''REPLI_&mduser.'');
oraexploit.pkg_duree.SNAP_INIT_TIME;
dbms_refresh.refresh(''&owner_rg_target..R_&owner_rg_target.'');
oraexploit.pkg_duree.SNAP_END_TIME;
oraexploit.pkg_duree.HISTO(true);
end;',
repeat_interval => 'FREQ=HOURLY;INTERVAL=&frequence.',
start_date => systimestamp at time zone 'Europe/Paris',
job_class => '"DEFAULT_JOB_CLASS"',
comments =>
'&mduser. refresh job',
auto_drop => FALSE,
enabled => FALSE);
sys.dbms_scheduler.set_attribute( name => '&owner_rg_target..J_&owner_rg_target.',
attribute => 'raise_events', value => dbms_scheduler.job_failed + dbms_scheduler.job_broken);
sys.dbms_scheduler.enable( '&owner_rg_target..J_&owner_rg_target.');
EXCEPTION
WHEN OTHERS THEN
  if sqlcode<>-27477 then
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  end if;
End;
/


declare
st long;
begin
		dbms_output.put_line('removing mviews from RG');
        for c in (select rowner,owner,rname,name from dba_refresh_children where owner=upper('&mduser.'))
        loop
                st:='begin DBMS_REFRESH.SUBTRACT('''||c.rowner||'.'||c.rname||''','''||c.owner||'.'|| c.name ||''',TRUE); end;';
                dbms_output.put_line(st);
                execute immediate st;
        end loop;
        dbms_output.put_line('adding mviews to RG');
        for c in (select owner,mview_name from dba_mviews where owner=upper('&mduser.'))
        loop
                st:='begin DBMS_REFRESH.ADD(''&owner_rg_target..&rg_target.'','''||c.owner||'.'|| c.mview_name ||''',TRUE); end;';
                dbms_output.put_line(st);
                execute immediate st;
        end loop;
        commit;
end;
/



