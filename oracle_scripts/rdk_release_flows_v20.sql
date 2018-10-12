--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_release_flows.sql
-- SYNOPSIS:  hold flows
-- USAGE:      sqlplus -S -L /nolog @rdk_release_flows.sql xxxx02_xxxxz2odb30 chega0032 xxxxx02_xxxxz2odb01 0 d:\temp
-- PARAMETERS:
-- chega_tns_entry= chef de gare tns
-- chega_schema_name= chef de are schema
-- server_tns_entry= server tns entry
-- numtiers= numtiers to hold (0 for all)
-- output_folder= working dir

--  Actions performed 
--   1. test connection on boith servers 
--   2. generate a script to hold flows executed on chef de gare schema
-- . /RUNDECK/rundeck_scripts/ORACLE_DB/bash_profile && /usr/bin/sqlplus64 -S -L /nolog @rdk_release_flows.sql ${option.20_chega_tns_entry} ${option.30_chega_schema_name} ${option.40_server_tns_entry} ${option.50_numtiers} ${option.10_pool} /tmp
--	Version
-- 	1.0  Creation	20/01/2016
--------------------------------------------------------------------------------


whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set lines 512

define chega_schema_name=&1
define numtiers=&2
define output_folder=&3
define dryrun=&4

--testing connection part
whenever sqlerror EXIT SQL.SQLCODE

column horodateur heading "horodateur"  new_value horodateur;
select to_char(systimestamp,'YYYY_MM_DD_HH24_MI_SS_FF') as horodateur from dual;

--declare variables part
set serveroutput on
set feed off

set verify off
set serveroutput on
set lines 512
set feed off
set verify off
set trimspool on
set head off
set pages 400
prompt generating &output_folder./to_del_rdk_release_flows_&horodateur..sql

spool &output_folder./to_del_rdk_release_flows_&horodateur..sql
select 'alter session set current_schema=&chega_schema_name.;' from dual;
select 'delete code_destinataire_holde where cds_name=''007' || lpad('&numtiers.',5,'0') || lpad('&numtiers.',5,'0') || ''' or '
|| ' cds_name=''' || '&numtiers.' || ''' or '
|| ' cds_name=''' || 'RAC' || lpad('&numtiers.',5,'0') || ''';'
from dual;
spool off

prompt executing &output_folder./to_del_rdk_release_flows_&horodateur..sql
@&output_folder./to_del_rdk_release_flows_&horodateur..sql

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

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
exit

