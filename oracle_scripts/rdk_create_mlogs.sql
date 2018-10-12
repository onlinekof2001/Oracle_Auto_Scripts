whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set head off
set feed off
set lines 512

define mduser=&1

whenever sqlerror CONTINUE

column db_name heading "db_name" new_value db_name;

select lower(name) as db_name from v$database;

prompt creating tbs...
begin
execute immediate 'CREATE SMALLFILE TABLESPACE &mduser._SLOG_DATA DATAFILE ''/u01/app/oracle/oradata/data1/&db_name./&mduser._slog_data.data1'' SIZE 1024M REUSE autoextend on next 128M maxsize 5G';
EXCEPTION
WHEN OTHERS THEN
  if sqlcode<>-1543 then
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  end if;
end;
/

alter user &mduser. quota unlimited on &mduser._SLOG_DATA;

declare
  st clob;
begin
  for c in (select mview_name from dba_mviews where upper(owner)=upper('&mduser.')) loop
   st:='create materialized view log on &mduser..'||c.mview_name||' tablespace &mduser._slog_data';
   execute immediate(st);
   dbms_output.put_line(st);
  end loop;
end;
/



