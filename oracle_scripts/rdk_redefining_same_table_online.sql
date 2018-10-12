set serveroutput on 
set verify off
alter session set ddl_lock_timeout=3600;
prompt size before for schema:&1 tablename:&2 source tbs:&3 target tbs:&4
select sum(bytes)/1024/1024 tb_size_mo from dba_segments where segment_name=upper('&2') and owner=upper('&1');
declare
errnum number:=0;
rc PLS_INTEGER;
interim_tablename varchar2(30);
redef_flag BINARY_INTEGER ;
table_ddl clob;
schema varchar2(30):=upper('&1');
tablename varchar2(30):=upper('&2');
target_tbs varchar2(30):=upper('&4');
source_tbs varchar2(30):=upper('&3');
begin
select 'IN$_'||substr(tablename,4,length(tablename)-3) into interim_tablename from dual;
dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS',FALSE);
dbms_metadata.set_transform_param(dbms_metadata.session_transform,'REF_CONSTRAINTS',FALSE);
dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',FALSE);
dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',FALSE);
dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',TRUE);
dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',TRUE);
table_ddl:=replace(dbms_metadata.get_ddl(object_type=>'TABLE',schema=>schema,name=>tablename),tablename,interim_tablename);
errnum:=5;
dbms_output.put_line('create interim table '||interim_tablename);
if target_tbs<>'-' then
 table_ddl:=replace(table_ddl,source_tbs,target_tbs);
end if;
begin
execute immediate (table_ddl);
exception
WHEN OTHERS THEN
  IF (SQLCODE=-955) THEN
   return;
  ELSE
    dbms_output.put_line(SQLERRM);
  END IF;
end;
errnum:=10;
redef_flag:=DBMS_REDEFINITION.CONS_USE_PK;
dbms_output.put_line('check if redef can be done');
begin
DBMS_REDEFINITION.CAN_REDEF_TABLE(UNAME=>schema,TNAME=>tablename,OPTIONS_FLAG=>redef_flag);
exception
 WHEN OTHERS THEN
  IF (SQLCODE=-12089) THEN
    redef_flag:=DBMS_REDEFINITION.CONS_USE_ROWID;
  ELSE
    dbms_output.put_line(SQLERRM);
  END IF;
END;

errnum:=15;
dbms_output.put_line('start redef');
DBMS_REDEFINITION.START_REDEF_TABLE(uname=>schema,orig_table=>tablename,int_table=>interim_tablename,OPTIONS_FLAG=>redef_flag);
errnum:=20;
dbms_output.put_line('copy dependents (idx,stats,triggers,constraints....');
DBMS_REDEFINITION.COPY_TABLE_DEPENDENTS( num_errors=>rc, uname=>schema, orig_table=>tablename, int_table=>interim_tablename, copy_indexes=>1, copy_triggers=>TRUE, copy_constraints=>TRUE, copy_privileges=>TRUE, copy_statistics=>TRUE, ignore_errors=>TRUE);
errnum:=25;
dbms_output.put_line('sync with mview');
DBMS_REDEFINITION.SYNC_INTERIM_TABLE(uname=>schema,orig_table=>tablename,int_table=>interim_tablename);
errnum:=30;
dbms_output.put_line('finish redef');
DBMS_REDEFINITION.FINISH_REDEF_TABLE(UNAME=>schema,ORIG_TABLE=>tablename,INT_TABLE=>interim_tablename);
dbms_output.put_line('drop interim table '||interim_tablename);
execute immediate ('drop table '||schema||'.'||interim_tablename||' cascade constraints');
EXCEPTION
WHEN OTHERS THEN
  dbms_output.put_line('REDEFINE_ONLINE=> '||schema||'.'||tablename||' '||to_char(errnum)||'=> '||sqlerrm);
  DBMS_REDEFINITION.ABORT_REDEF_TABLE(uname=>schema,orig_table=>tablename,int_table=>interim_tablename);
  execute immediate ('drop table '||schema||'.'||interim_tablename||' cascade constraints');
/*  execute immediate ('drop materialized view log on '||schema||'.'||tablename||'');
  execute immediate ('drop materialized view '||schema||'.'||interim_tablename||''); */
end;
/
prompt size after
select sum(bytes)/1024/1024 tb_size_mo from dba_segments where segment_name=upper('&2') and owner=upper('&1');
