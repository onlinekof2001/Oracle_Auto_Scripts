--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_hold_flows.sql
-- SYNOPSIS:  hold flows
-- USAGE:      sqlplus -S -L /nolog @rdk_switch_interactif.sql
-- PARAMETERS:

--	Version
-- 	1.0  Creation	20/04/2016
--------------------------------------------------------------------------------


whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set lines 512

--. /RUNDECK/rundeck_scripts/ORACLE_DB/bash_profile && /usr/bin/sqlplus64 -S -L /nolog @rdk_disable_interactif.sql ${option.20_source_tns} ${option.40_target_tns} ${option.50_numtiers} ${option.10_pool_source} ${option.30_pool_target} /tmp ${option.60_dryrun}
define numtiers=&1
define pool=&2
define status=&3
define output_folder=&4
define dryrun=&5


--testing connection part
whenever sqlerror EXIT SQL.SQLCODE
column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
whenever sqlerror CONTINUE

set serveroutput on
set feed off


set head on
column stcom_schema heading "stcom_schema"  new_value stcom_schema;
select owner stcom_schema from dba_tables where table_name=upper('id_schema_stcom') and rownum=1 and (owner='STCOM' or owner ='STCOM&pool.');

update &stcom_schema..parametres_detail
set   par_valeur_parametre_alpha = '&status.', par_date_upd = sysdate, par_user_upd = 'movpos'
where par_code_application = 'IHM_STORES'
and   par_code_parametre = 'ACCES'
and   tir_num_tiers_tir = &numtiers.
and   tir_sous_num_tiers_tir = &numtiers.
and   tti_num_type_tiers_tir = 7
;
set lines 500
select TIR_NUM_TIERS_TIR,PAR_VALEUR_PARAMETRE_ALPHA from &stcom_schema..parametres_detail p
where par_code_application = 'IHM_STORES'
and   par_code_parametre = 'ACCES'
and   tir_num_tiers_tir =  &numtiers.
and   tir_sous_num_tiers_tir =  &numtiers.
and   tti_num_type_tiers_tir = 7
;

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;

set echo off
set serveroutput on
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


prompt Finished.....................
exit

