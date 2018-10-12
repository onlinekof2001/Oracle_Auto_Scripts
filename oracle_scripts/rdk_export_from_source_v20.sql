set verify off
prompt due to pl/sql process you won''t have any messages until the end of the export
set serveroutput on
declare
fHandle UTL_FILE.FILE_TYPE;
lschema varchar2(50):='&1';
srvcible varchar2(50):='&3';
lfilename varchar2(255):='RFRSH_'||lschema||'_'||srvcible||'_'||to_char(sysdate,'YYYYMMDD_HH24_MI_SS')||'.LCK';
ldirectory varchar2(20):='&2';
ldumpfile varchar2(255):='RFRSH_'||lschema||'_'||srvcible||'_'||to_char(sysdate,'YYYYMMDD_HH24_MI_SS');
le ku$_LogEntry;                    -- WIP and error messages
js ku$_JobStatus;                   -- job status from get_status
jd ku$_JobDesc;                     -- job description from get_status
sts ku$_Status;                     -- status object returned by get_status
st varchar2(50);
ex boolean;
flen number;
bsize number:=0;
table_list clob:='&4';
cmd clob:='';
ind                   NUMBER;       -- loop index
pct_done              NUMBER;       -- percentage complete
v_line_no number:=0;
l_dp_handle    NUMBER;
l_job_state    VARCHAR2(30) := 'UNDEFINED';
scn_value      NUMBER;
cmd clob;
Error_export boolean:=false;
begin
  table_list:=replace(table_list,',',''',''');
  dbms_output.put_line('list='||table_list);
  v_line_no:=10;
  utl_file.fgetattr(ldirectory , lfilename, ex, flen, bsize);
  if not ex then
    FHANDLE:=UTL_FILE.FOPEN(ldirectory ,lfilename,'w',32767);
    UTL_FILE.PUT_LINE(FHANDLE, 'OK',TRUE);
    UTL_FILE.FCLOSE(FHANDLE);
  end if;
  v_line_no:=20;
  dbms_output.put_line('lock filename='||ldirectory||'/'||lfilename);
  utl_file.fgetattr(ldirectory , lfilename, ex, flen, bsize);
  if ex then
  FHANDLE:=UTL_FILE.FOPEN(ldirectory ,lfilename,'r',32767);
  UTL_FILE.GET_LINE (FHANDLE, st,30);
  dbms_output.put_line('lock txt='||st);
  if st='OK' then
    v_line_no:=40;
    UTL_FILE.FCLOSE(FHANDLE);
    UTL_FILE.FREMOVE(ldirectory , lfilename);
    FHANDLE:=UTL_FILE.FOPEN(ldirectory,lfilename,'a',32767);
    UTL_FILE.PUT_LINE(FHANDLE, 'EXPORT_RUNNING',TRUE);
    UTL_FILE.FCLOSE(FHANDLE);
    l_dp_handle := DBMS_DATAPUMP.open( operation => 'EXPORT', job_mode => 'SCHEMA');
    v_line_no   :=10;
    DBMS_DATAPUMP.add_file( handle => l_dp_handle, filename => ldumpfile||'.log', directory => ldirectory, filetype => sys.DBMS_DATAPUMP.ku$_file_type_log_file);
    v_line_no:=20;
    DBMS_DATAPUMP.add_file( handle => l_dp_handle, filename => ldumpfile||'.dmp', directory => ldirectory, filetype => sys.DBMS_DATAPUMP.ku$_file_type_dump_file);
    v_line_no:=30;
    DBMS_DATAPUMP.SET_PARALLEL (handle=>l_dp_handle,DEGREE=>4);
    v_line_no:=40;
    DBMS_DATAPUMP.METADATA_FILTER( HANDLE => l_dp_handle, NAME => 'SCHEMA_EXPR', VALUE => 'IN('''||upper(lschema)||''')' );
	v_line_no:=42;
	if table_list='-' then
		DBMS_DATAPUMP.METADATA_FILTER( HANDLE => l_dp_handle, NAME => 'NAME_EXPR', VALUE => 'IN (select table_name from dba_tab_comments where (comments not like ''%Refresh=N;%'' or comments is null) and owner='''||lschema||''')', OBJECT_PATH => 'TABLE' );
    else
		v_line_no:=45;
		DBMS_DATAPUMP.METADATA_FILTER( HANDLE => l_dp_handle, NAME => 'NAME_EXPR', VALUE => 'IN ('''||upper(table_list)||''')', OBJECT_PATH => 'TABLE' );
    end if;
	v_line_no:=47;
    DBMS_DATAPUMP.METADATA_FILTER(l_dp_handle,'INCLUDE_PATH_EXPR','IN (''SEQUENCE'', ''TABLE'')');
  --  v_line_no:=48;
  --  dbms_datapump.set_parameter(handle => l_dp_handle, name => 'COMPRESSION', value =>'ALL');
    SELECT current_scn INTO scn_value FROM v$database;
    dbms_datapump.set_parameter(handle => l_dp_handle, name => 'FLASHBACK_SCN', value => scn_value);
    v_line_no:=50;
    sys.DBMS_DATAPUMP.start_job(l_dp_handle);
	--dbms_datapump.wait_for_job( handle => l_dp_handle,job_state => l_job_state);
    WHILE (l_job_state != 'COMPLETED') AND (l_job_state != 'STOPPED') LOOP
    begin
	dbms_datapump.get_status(l_dp_handle, dbms_datapump.ku$_status_job_error + dbms_datapump.ku$_status_job_status + dbms_datapump.ku$_status_wip, -1, l_job_state, sts);
	exception
	 when others then
	 if sqlcode=-31626 then exit;end if;
	end;
    js := sts.job_status;
    -- If the percentage done changed, display the new value
    IF js.percent_done != pct_done THEN
      dbms_output.put_line('*** Job percent done = ' ||
      to_char(js.percent_done));
      pct_done := js.percent_done;
    END IF;

    -- If any work-in-progress (WIP) or error messages
    -- were received for the job, display them.
    IF (BITAND(sts.mask,dbms_datapump.ku$_status_wip) != 0) THEN
      le := sts.wip;
    ELSE
      IF (BITAND(sts.mask,dbms_datapump.ku$_status_job_error) != 0) THEN
        le := sts.error;
      ELSE
        le := NULL;
      END IF;
    END IF;

    IF le IS NOT NULL THEN
      ind := le.FIRST;
      WHILE ind IS NOT NULL LOOP
	    if instr(le(ind).LogText,'ORA-0')>0 then error_export:=true; end if;
        dbms_output.put_line(le(ind).LogText);
        ind := le.NEXT(ind);
      END LOOP;
    END IF;
	END LOOP;

    v_line_no:=80;
    UTL_FILE.FREMOVE(ldirectory,lfilename);
    FHANDLE:=UTL_FILE.FOPEN(ldirectory,lfilename,'a',32767);
    UTL_FILE.PUT_LINE(FHANDLE, 'READY_TO_IMPORT',TRUE);
    UTL_FILE.FCLOSE(FHANDLE);
  end if;
  if st='IMPORT_RUNNING' then
    dbms_output.put_line('Import Running...');
  end if;
  if st='READY_TO_IMPORT' then
    dbms_output.put_line('Waiting for import to start...');
  end if;
end if;
EXCEPTION
WHEN OTHERS THEN
 dbms_output.put_line('Exception:EXPORT_SCHEMA Debug Line '||TO_CHAR(v_line_no)||' SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
 RAISE;
end;
/

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
exit
