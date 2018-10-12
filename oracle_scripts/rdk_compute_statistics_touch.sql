define schema=&1
define output_folder=&2
set feed off
column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
set verify off
set serveroutput on
set echo off
spool &output_folder./to_del_compute_statistics_&horodateur..sql
declare
st clob;
begin
for c in (select  distinct table_owner, table_name,
'begin dbms_stats.gather_table_stats('''||table_owner||''', '''||table_name||''', granularity => ''ALL'', degree=>SYS.DBMS_STATS.AUTO_DEGREE,force=>true); end;' cmd
    from dba_tab_partitions
    where
    (LAST_ANALYZED is null or table_name in (select table_name from dba_tab_statistics where STALE_STATS='YES')) and table_owner=upper('&schema.')
    order by 1, 2 desc nulls last)
loop
 st:=c.cmd;
 dbms_output.put_line('prompt executing stats on  '||c.table_name);
 dbms_output.put_line(st);
 dbms_output.put_line('/');
 --execute immediate st;
end loop;
end;
/
spool off
set echo on
spool &output_folder./to_del_compute_statistics_&horodateur..log
@&output_folder./to_del_compute_statistics_&horodateur..sql
spool off

  