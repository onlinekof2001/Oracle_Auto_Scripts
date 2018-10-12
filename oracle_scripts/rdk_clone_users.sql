prompt ORACLE_DB/rdk_clone_users.sql

set feedback off
col param1 noprint new_value clonuser
col param2 noprint new_value createuser

set verify off
set term off
select upper('&1') param1 from dual;
set term on
set verify off
set term off
select upper('&2') param2 from dual;
set term on

prompt CLONE USER       => &&clonuser.
prompt REFERENCE USER   => &&createuser.

col param3 noprint new_value systemdate
select to_char(sysdate,'YYYYMMDDHH24MISS') param3 from dual;

set head off
set line 260
set long 9999
set pages 999
spool /tmp/CLONEU/clone_user&&systemdate..sql
SELECT regexp_replace(replace(DBMS_METADATA.GET_DDL('USER','&&clonuser.')||';','&&clonuser.','&&createuser.'),'IDENTIFIED BY VALUES ''S:.*''','IDENTIFIED BY decathlon1') FROM DUAL;
SELECT replace(DBMS_METADATA.GET_GRANTED_DDL('ROLE_GRANT','&&clonuser.'),'"&&clonuser."','"&&createuser.";') FROM DUAL;
spool off
whenever sqlerror EXIT SQL.SQLCODE
