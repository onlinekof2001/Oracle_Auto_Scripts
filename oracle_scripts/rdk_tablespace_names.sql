set line 160
set pages 300
set heading off
set term off
set feedback off
spool /tmp/tablespacename.lst
select tablespace_name from dba_tablespaces where tablespace_name not in ('SYSTEM','SYSAUX','TEMP','UNDO');
spool off
quit
