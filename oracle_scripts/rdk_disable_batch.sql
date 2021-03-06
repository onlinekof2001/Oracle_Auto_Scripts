--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_pool_table_insert.sql
-- SYNOPSIS:   insert pool table on all ptf
-- USAGE:      sqlplus -S -L @/tetrix02_rtdka2odbXX @rdk_pool_table_insert.sql 
-- PARAMETERS: 
-- numtiers : tiers to move
-- pool_target : pool target
--  Actions performed 
--  insert the new pool ,

--	Version
-- 	1.0  Creation	21/01/2016
--------------------------------------------------------------------------------


whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set lines 512

define numtiers=&1
define userupdate=&2
define dryrun=&3

--declare variables part
set serveroutput on
set feed off
set echo off

/*Disabling the batches in NBO*/
update nbo.parametres_detail pd set
       pd.par_valeur_parametre_alpha = 'N', pd.par_date_upd = sysdate,
       pd.par_user_upd = '&userupdate.' --mettre le groupe concerné pour retrouver facilement les batchs concernés par la suite
where pd.tti_num_type_tiers_tir=7 and pd.par_valeur_parametre_alpha='Y'
and pd.tir_num_tiers_tir in (&numtiers.)
and pd.par_code_application='ActivationBatch';

/*Disabling the batches in STCOM*/
update stcom.parametres_detail pd set
       pd.par_valeur_parametre_alpha = 'N', pd.par_date_upd = sysdate,
       pd.par_user_upd = '&userupdate.' --mettre le groupe concerné pour retrouver facilement les batchs concernés par la suite
where pd.tti_num_type_tiers_tir=7 and pd.par_valeur_parametre_alpha='Y'
and pd.tir_num_tiers_tir in (&numtiers.)
and pd.par_code_application='ActivationBatch';

prompt NBO
select count(*) nb_batch_disabled from nbo.parametres_detail pd where 
pd.tti_num_type_tiers_tir=7 and pd.par_valeur_parametre_alpha='N'
and pd.tir_num_tiers_tir in (&numtiers.)
and pd.par_code_application='ActivationBatch';

prompt STCOM
select count(*) nb_batch_disabled from stcom.parametres_detail pd where 
pd.tti_num_type_tiers_tir=7 and pd.par_valeur_parametre_alpha='N'
and pd.tir_num_tiers_tir in (&numtiers.)
and pd.par_code_application='ActivationBatch';

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

exit

