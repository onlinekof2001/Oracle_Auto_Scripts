--------------------------------------------------------------------------------
-- AUTHOR:  N.MACKE
-- VALIDATOR: 
-- SCRIPT:     rdk_add_datafile.sql
-- SYNOPSIS:   rdk_add_datafile
-- USAGE:      sqlplu@/xxxxx02_xxxxx2odbXX @rdk_propage.sql 
-- PARAMETERS: 
--     ${option.20_Tns_Server}
--     ${option.30_Dba_User}
--     ${option.40_Dba_Pwd}
--     ${option.50_Schema}
--     ${option.60_Datafile_Type}
--     ${option.200_Dryrun}
--     /tmp
--
--	Version
-- 	1.0  Creation	01/03/2016
--------------------------------------------------------------------------------
--. /RUNDECK/rundeck_scripts/ORACLE_DB/bash_profile && /usr/bin/sqlplus64 -S -L /nolog @rdk_add_datafile ${option.20_Tns_Server} ${option.30_Dba_User} ${option.40_Dba_Pwd} ${option.50_Schema} ${option.60_Datafile_Type} ${option.200_Dryrun} /tmp
set serveroutput on
set long 10000

define scriptname=rdk_add_datafile
define tns_entry=&1
define username=&2
define pwd=&3
define schema=&4
define type=&5
define dryrun=&6
define output_folder=&7

prompt testing connection on @&tns_entry.
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect &username./&pwd.@&tns_entry.
set serveroutput on
whenever sqlerror CONTINUE

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
define logfile=to_del_&scriptname._&horodateur..log

col ddl for A380 wor
set long 10000
set heading off
set pages 0
set feedback off
set verify off
set wrap on

set lines 1024
spool &output_folder./&logfile.

declare
st clob;
dryrun varchar(10):='&dryrun.';
begin
     	select 'alter tablespace ' || tablespace_name || ' add datafile ''' || filename ||  numdatafile || ''' size 128m autoextend on next 128m maxsize 30g' into st
	from (
		select rank() over (order by substr(file_name,instr(file_name,'.data')+5) desc) rang,tablespace_name,substr(file_name,1,instr(file_name,'.data')+4) filename,substr(file_name,instr(file_name,'.data')+5)+1 numdatafile
		from DBA_DATA_FILES
		where tablespace_name=upper('&schema._&type.'))
	where rang=1;

        dbms_output.put_line(st);
	if (dryrun='true') then
        	dbms_output.put_line('not executed ' || st || ';');
	else
        	execute immediate(st);
        	dbms_output.put_line('executed');
	end if;

	exception	
	 WHEN NO_DATA_FOUND THEN
	  raise_application_error (-20001,' Tablespace does not exists on &tns_entry.');
end;
/
spool off


declare
dryrun varchar(10):='&dryrun.';
begin
if (dryrun='true') then
        rollback;
        dbms_output.put_line('rollbacked');
else
        commit;
        dbms_output.put_line('commited');
end if;
end;
/



exit
