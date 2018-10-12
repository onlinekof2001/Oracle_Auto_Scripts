--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_import_on_target.sql
-- SYNOPSIS:   import table on preprod
-- USAGE:      sqlplus -S -L @/tetrix02_rtdka2odbXX @rdk_import_on_target.sql 
-- PARAMETERS: 
-- 
-- 
-- 
-- 

--	Version
-- 	1.0  Creation	01/03/2016
--------------------------------------------------------------------------------

set feedback off
set serveroutput on
set verify off
prompt due to pl/sql process you won''t have any messages until the end of the export
WHENEVER SQLERROR EXIT SQL.SQLCODE
declare
fHandle UTL_FILE.FILE_TYPE;
file_to_import varchar2(255):='&1';
ldirectory varchar2(255):='&2';
lfilename varchar2(255):=file_to_import||'.LCK';
ldumpfile varchar2(255):=file_to_import||'.dmp';
llogfile varchar2(255):=file_to_import||'.log';
DumpDirectory VARCHAR2(255) := ldirectory;
DumpFileName VARCHAR2(255):=ldumpfile;
LogFileName VARCHAR2(255):=llogfile;
st varchar2(250);
schema_source varchar2(250);
ex boolean;
flen number;
bsize number;
err_num number;
Error_job Exception;
l_dp_handle      NUMBER;
l_job_state      VARCHAR2(30) := 'UNDEFINED';
v_line_no number:=0;
f utl_file.file_type;
s CLOB;
begin
  utl_file.fgetattr(ldirectory , lfilename, ex, flen, bsize);
  if ex then
    FHANDLE:=UTL_FILE.FOPEN(ldirectory ,lfilename,'r',32767);
    UTL_FILE.GET_LINE (FHANDLE, st,255);
    dbms_output.put_line('st='||st);
    if st='READY_TO_IMPORT' then
	  schema_source:=substr(ltrim(lfilename,'RFRSH'),2,instr(ltrim(lfilename,'RFRSH_'),'_')-1);
	  dbms_output.put_line('schema_source='||schema_source);
      repo.pack_oxy.service(schema_source,'stop');
      repo.pack_oxy.service(schema_source,'disconnect');
      ex:=FALSE;
	  for c in (select owner,table_name from dba_tab_comments where (comments not like '%Refresh=N;%' or comments is null) and owner=schema_source)
	  loop
	     begin
		   dbms_output.put_line('Table '||c.owner||'.'||c.table_name||' droppped');
	       execute immediate 'drop table '||c.owner||'.'||c.table_name||' cascade constraint';
		 EXCEPTION
	  	 WHEN OTHERS THEN
		   dbms_output.put_line('Exception:IMPORT drop table SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
	     END;
	  end loop;

      BEGIN
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
      repo.pack_oxy.service(schema_source,'start');
    else
     dbms_output.put_line('File '||lfilename||' found, but READY_TO_IMPORT not found => nothing to import');
     --raise Error_job;
    end if;
    if st='EXPORT_RUNNING' then
      dbms_output.put_line('Export Running...');
    end if;
  else
    dbms_output.put_line('file '||lfilename||' not found => nothing to import');
    raise Error_job;
  end if;
exception
when Error_job then
    raise_application_error (-20001,'import failed');
    raise;
end;
/