--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR:
-- SCRIPT:     rdk_mov_pos_execute_cmd_v20.sql
-- SYNOPSIS:   rdk_mov_pos_execute_cmd_v20.sql
-- USAGE:      sqlplus -S -L repo_user/repo_pwd@tns_entry @rdk_mov_pos_execute_cmd.sql
-- PARAMETERS:
-- . /RUNDECK/rundeck_scripts/ORACLE_DB/bash_profile && /usr/bin/sqlplus64 -S -L /@${option.10_repo_tns_entry}_repo @rdk_mov_pos_execute_cmd ${option.50_target_tns_entry} ${option.20_schema_temp} ${option.40_cmd_type} /tmp
--	Version
-- 	1.0  Creation	21/01/2016
--  2.0  Modification 10/10/2016 Remove wallet connection and extend to highway usage
--------------------------------------------------------------------------------
whenever sqlerror exit
alter session set current_schema=repo;
define schema_name=&1
define type_cmd=&2
define output_folder=&3



set verify off
set serveroutput on
set lines 512
set feed off
set trimspool on

--column exec_on_tns_entry heading "exec_on_tns_entry"  new_value exec_on_tns_entry;
--select distinct exec_on_tns_entry exec_on_tns_entry from cmd_movpos_rdk where cmd_type=upper('&type_cmd.') and schema_temp=upper('MOV_&schema_name._&numtiers.') and status=0;
--whenever sqlerror CONTINUE

column horodateur heading "horodateur"  new_value horodateur;
select to_char(systimestamp,'YYYY_MM_DD_HH24_MI_SSFF3') as horodateur from dual;

column mag heading "mag"  new_value mag;
select substr('&schema_name.',instr('&schema_name.','_',1,2)+1,1+length('&schema_name.')-instr('&schema_name.','_',1,2)) mag from dual;

column tgt_schema new_value tgt_schema
--select rtrim(ltrim('&schema_name.','MOV_'),'_&mag.') tgt_schema from dual;
select rtrim(substr('&schema_name.',instr('&schema_name.','_')+1,length('&schema_name.')-instr('&schema_name.','_')),'_&mag.') tgt_schema from dual;

prompt executing command &type_cmd.
define outputfile=&output_folder./to_del_&schema_name._&type_cmd._&horodateur..sql

prompt generating &outputfile.
spool &outputfile.
exec repo.pack_oxy.EXEC_CMD_MOVPOS_RDK('&tgt_schema.','&mag.','&type_cmd.');
spool off


set serveroutput on
set lines 1024
set feed off

whenever sqlerror continue
@&outputfile.

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
exit
