whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set head off
set feed off
set lines 512


whenever sqlerror CONTINUE
create role select_masterdatas;


declare
  st clob;
begin
  for c in (select owner,mview_name from dba_mviews where upper(owner) like 'MD%') loop
   st:='grant select on '||c.owner||'.'||c.mview_name||' to select_masterdatas';
   execute immediate(st);
   dbms_output.put_line(st);
   st:='grant select on '||c.owner||'.'||c.mview_name||' to stcom with grant option';
   execute immediate(st);
   dbms_output.put_line(st);
  end loop;
  st:='grant select_masterdatas to stcom';
  execute immediate(st);
  st:='grant select_masterdatas to nbo';
  execute immediate(st);
  st:='alter user stcom default role all';
  execute immediate(st);
  st:='alter user nbo default role all';
  execute immediate(st);
end;
/



