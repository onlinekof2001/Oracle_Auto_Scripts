--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_import_any_schema_v20.sql
-- SYNOPSIS: 
-- USAGE:      sqlplus -S -L /nolog @rdk_import_any_schema_v20.sql 
-- PARAMETERS:


--	Version
-- 	1.0  Creation	10/03/2016
--------------------------------------------------------------------------------


set serveroutput on
set long 10000
set verify off

define directory=&1
define dumpfile=&2
define logfile=&3
set serveroutput on
set feed off

prompt import from directory &directory. dumpfile &dumpfile. logs in &logfile.
prompt due to pl/sql process you won''t have any messages until the end of the export
prompt you can follow directly with tail -f on &logfile. on the server

DECLARE
l_dp_handle      NUMBER;
l_job_state      VARCHAR2(30) := 'UNDEFINED';
DumpDirectory         VARCHAR2(255) := '&directory.';
DumpFileName VARCHAR2(255):='&dumpfile.';
LogFileName VARCHAR2(255):='&logfile.';
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
Error_export boolean:=false;
v_line_no number:=0;
f utl_file.file_type;
s CLOB;
BEGIN
ex:=false;
sys.utl_file.fgetattr('DATA_PUMP_DIR', LogFileName, ex, flen, bsize);
IF ex THEN
    sys.utl_file.fremove('DATA_PUMP_DIR', LogFileName);
END IF;
l_dp_handle := sys.DBMS_DATAPUMP.open( operation => 'IMPORT', job_mode => 'FULL');
v_line_no :=10;
sys.DBMS_DATAPUMP.add_file( handle => l_dp_handle, filename => DumpFileName, directory => DumpDirectory, filetype => sys.DBMS_DATAPUMP.ku$_file_type_dump_file);
v_line_no :=20;
sys.DBMS_DATAPUMP.add_file( handle => l_dp_handle, filename => LogFileName, directory => 'DATA_PUMP_DIR', filetype => sys.DBMS_DATAPUMP.ku$_file_type_log_file);
v_line_no :=30;
sys.DBMS_DATAPUMP.SET_PARALLEL(handle=>l_dp_handle,DEGREE=>4);
v_line_no :=40;
sys.DBMS_DATAPUMP.SET_PARAMETER( HANDLE => l_dp_handle, NAME => 'TABLE_EXISTS_ACTION', VALUE => 'REPLACE');
v_line_no :=50;
sys.DBMS_DATAPUMP.start_job(l_dp_handle);
v_line_no :=95;
sys.DBMS_DATAPUMP.WAIT_FOR_JOB(l_dp_handle,l_job_state);
v_line_no :=100;
f := utl_file.fopen('DATA_PUMP_DIR',LogFileName,'R',32767);
loop
	utl_file.get_line(f,s);
	dbms_output.put_line(s);
end loop;
utl_file.fclose(f);
EXCEPTION
WHEN NO_DATA_FOUND then NULL;
WHEN OTHERS THEN
	dbms_output.put_line('Exception:IMPORT Debug Line '||TO_CHAR(v_line_no)||' SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
END;
/

exit



  