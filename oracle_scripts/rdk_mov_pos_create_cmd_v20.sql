--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR:
-- SCRIPT:     rdk_mov_pos_create_cmd_v20.sql
-- USAGE:      sqlplus -S -L user/pwd@rdk_mov_pos_create_cmd_v20.sql numtiers schema type_cmd output_foldef
-- PARAMETERS:
--  Actions performed
--  Create command for movpos in the table CMD_MOVPOS_RDK_LOCAL

--	Version
-- 	1.0  Creation	21/01/2016
--  2.0  Modification 10/10/2016 remove walet connections
--------------------------------------------------------------------------------
alter session set current_schema=repo;
define numtiers=&1
define schema_name=&2
define type_cmd=&3
define output_folder=&4

set verify off
set serveroutput on
set lines 512
set serveroutput on

--testing connection part
whenever sqlerror EXIT SQL.SQLCODE

column mag heading "mag"  new_value mag;
select lpad('&numtiers.',5,'0') mag from dual;

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
SET FEED OFF

exec repo.pack_oxy.export_pos_cmd(application=>'STORES',schema=>'&schema_name.',POS=>'(007,&mag.,&mag.)',schema_cible=>'&schema_name.',type_cmd=>'&type_cmd.');

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
exit
