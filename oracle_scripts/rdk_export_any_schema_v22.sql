--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR:
-- SCRIPT:     rdk_export_any_schema.sql
-- SYNOPSIS:  hold flows
-- USAGE:      sqlplus -S -L /nolog @rdk_export_any_schema.sql
-- PARAMETERS:
-- source = tns_source
-- cible = tns_target
-- publi c= Y/N
--

--	Version
-- 	1.0  Creation	10/03/2016
--------------------------------------------------------------------------------


set serveroutput on
set long 10000
set verify off
define schema_to_export=&1
define location=&2
define dumpfile=&3
define logfile=&4
define table_list=&5
define content=&6

set serveroutput on
set feed off

prompt export &schema_to_export. in &location./&dumpfile. see logs in &logfile.
prompt due to pl/sql process you won''t have any messages until the end of the export
prompt you can follow directly with tail -f on &logfile. on the server

column lockFileName new_value lockFileName
--select rtrim(ltrim('&schema_name.','MOV_'),'_&mag.') tgt_schema from dual;
select replace('&dumpfile.','dmp','LCK') lockFileName from dual;

column schema_to_export_upper new_value schema_to_export_upper
--select rtrim(ltrim('&schema_name.','MOV_'),'_&mag.') tgt_schema from dual;
select upper('&schema_to_export.') schema_to_export_upper from dual;


DECLARE
Eerror_export EXCEPTION;
l_dp_handle      NUMBER;
fHandle UTL_FILE.FILE_TYPE;
l_job_state      VARCHAR2(30) := 'UNDEFINED';
DumpLocation         VARCHAR2(50) := '&location.';
DumpDirectory         VARCHAR2(50);
DumpFileName VARCHAR2(50):='&dumpfile.';
LogFileName VARCHAR2(50):='&logfile.';
LockFileName VARCHAR2(50):='&lockFileName.';
Content VARCHAR2(50):='&content.';
TableList clob:='&table_list.';
scn_value      NUMBER;
ex             BOOLEAN;
flen           NUMBER;
bsize          NUMBER;
ind                   NUMBER;       -- loop index
pct_done              NUMBER;       -- percentage complete
job_state             VARCHAR2(30); -- track job state
le ku$_LogEntry;                    -- WIP and error messages
js ku$_JobStatus;                   -- job status from get_status
jd ku$_JobDesc;                     -- job description from get_status
sts ku$_Status;                     -- status object returned by get_status
v_row  PLS_INTEGER;
f utl_file.file_type;
s CLOB;
st CLOB;
numerror integer:=0;
BEGIN

begin
st:='drop directory DR_&schema_to_export_upper.';
dbms_output.put_line(st);
execute immediate st;
exception
 when others then null;
end;

begin
st:='create directory DR_&schema_to_export_upper. as ''&location.''';
dbms_output.put_line(st);
execute immediate st;
exception
 when others then null;
end;

DumpDirectory:='DR_&schema_to_export_upper.';
numerror:=10;

sys.utl_file.fgetattr(DumpDirectory, DumpFileName, ex, flen, bsize);
numerror:=20;
IF ex THEN
    sys.utl_file.fremove(DumpDirectory, DumpFileName);
END IF;
numerror:=30;
ex:=false;
sys.utl_file.fgetattr(DumpDirectory, LogFileName, ex, flen, bsize);
numerror:=40;
IF ex THEN
    sys.utl_file.fremove(DumpDirectory, LogFileName);
END IF;
numerror:=50;
l_dp_handle := sys.DBMS_DATAPUMP.open( operation => 'EXPORT', job_mode => 'SCHEMA');
numerror:=60;
sys.DBMS_DATAPUMP.add_file( handle => l_dp_handle, filename => DumpFileName, directory => DumpDirectory, filetype => sys.DBMS_DATAPUMP.ku$_file_type_dump_file);
numerror:=70;
sys.DBMS_DATAPUMP.add_file( handle => l_dp_handle, filename => LogFileName, directory => DumpDirectory, filetype => sys.DBMS_DATAPUMP.ku$_file_type_log_file);
numerror:=80;
SELECT current_scn INTO scn_value FROM v$database;
dbms_datapump.set_parameter(handle => l_dp_handle, name => 'FLASHBACK_SCN', value => scn_value);
numerror:=90;
sys.DBMS_DATAPUMP.SET_PARALLEL (handle=>l_dp_handle,DEGREE=>4);
numerror:=100;
--dbms_datapump.set_parameter(handle => l_dp_handle, name => 'COMPRESSION', value =>'ALL');
--numerror:=105;
if (content='METADATA_ONLY') then
 dbms_datapump.DATA_FILTER(handle => l_dp_handle, name => 'INCLUDE_ROWS', value =>0);
end if;
numerror:=110;
sys.DBMS_DATAPUMP.metadata_filter( handle => l_dp_handle, name => 'SCHEMA_EXPR',  value => '= ''&schema_to_export_upper.''');
numerror:=115;
if TableList<>'-'  then
 st:='IN ('''||replace(TableList,',',''',''')||''')';
 dbms_output.put_line(st);
 DBMS_DATAPUMP.METADATA_FILTER( HANDLE => l_dp_handle, NAME => 'NAME_EXPR', VALUE => st, OBJECT_PATH => 'TABLE' );
end if;
numerror:=120;
sys.DBMS_DATAPUMP.start_job(l_dp_handle);
numerror:=130;
sys.DBMS_DATAPUMP.WAIT_FOR_JOB(l_dp_handle,l_job_state);
numerror:=140;


sys.utl_file.fgetattr(DumpDirectory , LockFileName, ex, flen, bsize);
IF ex THEN
    sys.utl_file.fremove(DumpDirectory, LockFileName);
END IF;
fHandle:=sys.UTL_FILE.FOPEN(DumpDirectory ,LockFileName,'w',32767);
sys.UTL_FILE.PUT_LINE(fHandle, 'READY_TO_IMPORT',TRUE);
sys.UTL_FILE.FCLOSE(fHandle);

f := utl_file.fopen(DumpDirectory,LogFileName,'R',32767);
loop
	utl_file.get_line(f,s);
	dbms_output.put_line(s);
end loop;
utl_file.fclose(f);


begin
st:='drop directory DR_&schema_to_export_upper.';
dbms_output.put_line(st);
execute immediate st;
exception
 when others then null;
end;

EXCEPTION
WHEN NO_DATA_FOUND then NULL;
WHEN OTHERS THEN
	dbms_output.put_line('Exception:EXPORT '||to_char(numerror)||' SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
END;
/

exit
