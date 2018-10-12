--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_mov_pos_sat_create_cmd.sql
-- SYNOPSIS:   rdk_mov_pos_sat_create_cmd.sql
-- USAGE:      sqlplus -S -L /nolog @rdk_mov_pos_sat_create_cmd.sql numtiers source_tns_entry target_tns_entry 
-- PARAMETERS: 
-- . /RUNDECK/rundeck_scripts/ORACLE_DB/bash_profile && /usr/bin/sqlplus64 -S -L /@${option.20_source_tns_entry}_repo @rdk_mov_pos_sat_create_cmd.sql ${option.30_numtiers} ${option.20_source_tns_entry} ${option.50_target_tns_entry} ${option.55_schema} ALL /tmp
--  Actions performed 
--  . /RUNDECK/rundeck_scripts/ORACLE_DB/bash_profile && /usr/bin/sqlplus64 -S -L /nolog @rdk_mov_pos_sat_create_cmd.sql ${option.30_numtiers} ${option.20_source_tns_entry} ${option.50_target_tns_entry} ${option.55_schema} ${option.60_cmdtype} /tmp 

--	Version
-- 	1.0  Creation	21/01/2016
--------------------------------------------------------------------------------

define numtiers=&1
define source_tns_entry=&2
define target_tns_entry=&3
define schema_name=&4
define type_cmd=&5
define output_folder=&6
define numpays=&7
define schema_cible=&8
define user_repo=&9
define pwd_repo=&10


set verify off
set serveroutput on
set lines 512

--testing connection part
whenever sqlerror EXIT SQL.SQLCODE
prompt testing connection &user_repo on @&target_tns_entry.
connect &user_repo./&pwd_repo.@&target_tns_entry.
prompt testing connection @&source_tns_entry.
connect &user_repo./&pwd_repo.@&source_tns_entry.

column mag heading "mag"  new_value mag;
select lpad('&numtiers',4,'0') mag from dual;

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
SET FEED OFF

prompt connecting to &source_tns_entry. to generate movpos cmd 
var type_cmd varchar2(20);
exec :type_cmd:='&type_cmd.';

set serveroutput on

begin
 if instr(:type_cmd,'SRC')>0 then
  dbms_output.put_line('creating cmd for '||:type_cmd||' on src');
  pack_oxy.export_pos_sat_cmd(application=>'STORES',schema=>'&schema_name.',POS=>'&mag',schema_cible=>'&schema_cible.',type_cmd=>:type_cmd,pays=>'&numpays.');
 end if;
end;
/

prompt connecting to &target_tns_entry. to generate movpos cmd
connect &user_repo./&pwd_repo.@&target_tns_entry.
SET FEED OFF
set serveroutput on
begin
 if instr(:type_cmd,'TGT')>0 then
  dbms_output.put_line('creating cmd for '||:type_cmd||' on tgt');
  pack_oxy.export_pos_sat_cmd(application=>'STORES',schema=>'&schema_name.',POS=>'&mag.',schema_cible=>'&schema_cible.',type_cmd=>:type_cmd,pays=>'&numpays.');
 end if;
end;
/



select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
commit;
exit
