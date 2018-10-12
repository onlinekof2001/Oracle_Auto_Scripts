--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_pool_table_delete.sql
-- SYNOPSIS:   update pool table on all ptf
-- USAGE:      sqlplus -S -L @/tetrix02_rtdka2odbXX @rdk_pool_table_delete.sql 
-- PARAMETERS: 
-- numtiers : tiers to move
-- pool_target : pool target
--  Actions performed 
--  delete from nbo & stcom pool table 

--	Version
-- 	1.0  Creation	21/01/2016
--------------------------------------------------------------------------------


whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set lines 512

define numtiers=&1
define pool_target=&2
define dryrun=&3

--declare variables part
set serveroutput on
set feed off
set echo off

set head on
column stcom_schema heading "stcom_schema"  new_value stcom_schema;
column nbo_schema heading "nbo_schema"  new_value nbo_schema;
select owner stcom_schema,replace(owner,'STCOM','NBO') nbo_schema from dba_tables where table_name=upper('id_schema_stcom') and rownum=1 and (owner='STCOM' or owner ='STCOM&pool_target.');

set verify on

delete from &stcom_schema..pool where type_tiers=7 and NUM_TIERS=&numtiers. and SOUS_NUM_TIERS=&numtiers.;
delete from &nbo_schema..pool where type_tiers=7 and NUM_TIERS=&numtiers. and SOUS_NUM_TIERS=&numtiers.;
delete from &stcom_schema..timezone_tiers t where t.tti_num_type_tiers_tir = 7 and t.tir_num_tiers_tir = &numtiers.;
delete from &nbo_schema..timezone_tiers t where t.tti_num_type_tiers_tir = 7 and t.tir_num_tiers_tir = &numtiers.;


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

