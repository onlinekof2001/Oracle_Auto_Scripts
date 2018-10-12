--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_pool_table_insert.sql
-- SYNOPSIS:   insert pool table on all ptf
-- USAGE:      sqlplus -S -L @/xxxxx02_xxxxx2odbXX @rdk_pool_table_insert.sql 
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
define pool_target=&2
define dryrun=&3
define tzo=&4
define tzo_real=&5

--declare variables part
set serveroutput on
set feed off
set echo off

set head on
column stcom_schema heading "stcom_schema"  new_value stcom_schema;
column nbo_schema heading "nbo_schema"  new_value nbo_schema;
select owner stcom_schema,replace(owner,'STCOM','NBO') nbo_schema from dba_tables where table_name=upper('id_schema_stcom') and rownum=1 and (owner='STCOM' or owner ='STCOM&pool_target.');

set verify on

insert into &stcom_schema..pool values (7,&numtiers.,&numtiers.,'jdbc/stcom','jdbc/stcom&pool_target.','', '', sysdate, 'move_pos');
insert into &stcom_schema..pool values (7,&numtiers.,&numtiers.,'jdbc/nbo','jdbc/nbo&pool_target.','', '', sysdate, 'move_pos');
insert into &stcom_schema..pool values (7,&numtiers.,&numtiers.,'jdbc/masterdatas','jdbc/masterdatas&pool_target.','', '', sysdate, 'move_pos');
insert into &nbo_schema..pool values (7,&numtiers.,&numtiers.,'jdbc/stcom','jdbc/stcom&pool_target.','', '', sysdate, 'move_pos');
insert into &nbo_schema..pool values (7,&numtiers.,&numtiers.,'jdbc/nbo','jdbc/nbo&pool_target.','', '', sysdate, 'move_pos');
insert into &nbo_schema..pool values (7,&numtiers.,&numtiers.,'jdbc/masterdatas','jdbc/masterdatas&pool_target.','', '', sysdate, 'move_pos');

insert into &stcom_schema..timezone_tiers (TTI_NUM_TYPE_TIERS_TIR, TIR_NUM_TIERS_TIR, TIR_SOUS_NUM_TIERS_TIR, TZO_TIMEZONE_ID, TZO_DATE_UPD, TZO_USER_UPD, TZO_TIMEZONE_ID_REEL)
values (7, &numtiers., &numtiers., '&tzo.', sysdate, 'move_pos', '&tzo_real.');
insert into &nbo_schema..timezone_tiers (TTI_NUM_TYPE_TIERS_TIR, TIR_NUM_TIERS_TIR, TIR_SOUS_NUM_TIERS_TIR, TZO_TIMEZONE_ID, TZO_DATE_UPD, TZO_USER_UPD, TZO_TIMEZONE_ID_REEL)
values (7, &numtiers., &numtiers., '&tzo.', sysdate, 'move_pos', '&tzo_real.');

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

