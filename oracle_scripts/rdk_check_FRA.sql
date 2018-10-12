set feedback off

set verify off
set term off
alter session set nls_date_format='YYYY-MM-DD HH24:MI:SS';
col param1 noprint new_value current_date
col param2 noprint new_value db_unique
select to_char(sysdate,'YYYYMMDDHH24') param1 from dual;
select lower('&1') param2 from dual;
set term on

prompt Check_unique_name => &&db_unique.
prompt Check_mview_time => &&current_date.
set trim off
set head off
set wrap off
set line 130 long 999 pages 999
COL name FOR a45
COL maxfra FOR 999999999
COL usage FOR 999999999
spool /tmp/EXTTBS/FRA_&&db_unique._&&current_date..lst
select name,(space_limit/1048576) maxfra,round(space_used/space_limit*100,2) usage from V$RECOVERY_FILE_DEST;
spool off
quit;
