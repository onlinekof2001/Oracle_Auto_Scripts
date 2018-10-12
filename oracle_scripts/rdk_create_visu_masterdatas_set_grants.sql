define pool=&1
define mduser=&2
define mdpwd=&3
set lines 512
set head off
set feed off
prompt creating the user visu_md&pool. if not exists
declare
st long;
begin
st:='create user visu_&mduser. identified by "&mdpwd." default tablespace dbtools temporary tablespace TEMP profile DEFAULT';
execute immediate st;
EXCEPTION
WHEN OTHERS THEN
  if sqlcode<>-1920 then
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM || 'QRY='||st);
  end if;
end;
/
grant alter any materialized view to visu_&mduser.;
grant connect to visu_&mduser.;
prompt setting the grants for user visu_&mduser.
begin
 for c in (select owner,table_name from dba_tables where upper(owner) = upper('&mduser.'))
 loop
  execute immediate 'grant select on '||c.owner||'.'||c.table_name||' to visu_&mduser.';
 end loop;
end;
/
prompt create synonyms from visu_&mduser. to MD&pool.
begin
 for c in (select owner,mview_name from dba_mviews where upper(owner) = upper('&mduser.'))
 loop
  execute immediate 'create or replace synonym visu_&mduser..'||c.mview_name||' for &mduser..'||c.mview_name;
 end loop;
end;
/
