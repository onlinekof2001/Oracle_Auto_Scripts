define synonym_owner=&1
define table_owner=&2
set lines 400
set verify off
set feed off
set serveroutput on

begin
execute immediate 'create role select_&table_owner.';
exception
when others then null;
end;
/

declare
cmd clob;
begin
for c in
(
select 'create or replace synonym &synonym_owner..'||table_name||' for &table_owner..'||table_name cmd
from
dba_tables
where
owner =upper('&table_owner.')
) 
loop
 dbms_output.put_line(c.cmd||';');
 execute immediate c.cmd;
end loop;
for c in
(
select 'grant select on &table_owner..'||table_name||' to select_&table_owner.' cmd
from
dba_tables
where
owner =upper('&table_owner.')
) 
loop
 dbms_output.put_line(c.cmd||';');
 execute immediate c.cmd;
end loop;
exception
  when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/

grant select_&table_owner. to &synonym_owner.;
