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
COL rfcmd FOR A72
COL LAST_REFRESH_DATE FOR A37
spool /tmp/EXTTBS/&&db_unique._&&current_date..list
SELECT 'exec dbms_mview.refresh('''||OWNER||'.'||MVIEW_NAME||''');' as rfcmd,LAST_REFRESH_DATE FROM DBA_MVIEWS WHERE LAST_REFRESH_DATE < SYSDATE - 1;
spool off
quit;
