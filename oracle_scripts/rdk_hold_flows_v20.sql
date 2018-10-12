--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_hold_flows.sql
-- SYNOPSIS:  hold flows
-- USAGE:      sqlplus -S -L /nolog @rdk_hold_flows.sql tech02_rtdkz2odb30 chega0032 tetrix02_rtdkz2odb01 0 d:\temp
-- PARAMETERS:
-- chega_tns_entry= chef de gare tns
-- chega_schema_name= chef de are schema
-- server_tns_entry= server tns entry
-- numtiers= numtiers to hold (0 for all)
-- output_folder= working dir

--  Actions performed 
--   1. test connection on boith servers 
--   2. generate a script to hold flows executed on chef de gare schema

--	Version
-- 	1.0  Creation	20/01/2016
--------------------------------------------------------------------------------


whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set lines 512

define chega_schema_name=&1
define server_tns_entry=&2
define numtiers=&3
define pool=&4
define output_folder=&5
define dryrun=&6

--testing connection part
whenever sqlerror EXIT SQL.SQLCODE
column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
whenever sqlerror CONTINUE

--declare variables part
set serveroutput on
set feed off

var numtiers number;
exec :numtiers:=&numtiers;


set head on
column stcom_schema heading "stcom_schema"  new_value stcom_schema;
select owner stcom_schema from dba_tables where table_name=upper('id_schema_stcom') and rownum=1 and (owner='STCOM' or owner ='STCOM&pool.');
column pool heading "pool"  new_value pool;
select lpad(id_schema_stcom,4,'0') pool from &stcom_schema..id_schema_stcom;

set verify off
set serveroutput on
set lines 512
set feed off
set verify off
set trimspool on
set head off
set pages 400
define stcom_schema=stcom
prompt generating &output_folder./to_del_rdk_hold_flows_&horodateur..sql
spool &output_folder./to_del_rdk_hold_flows_&horodateur..sql
select 'alter session set current_schema=&chega_schema_name.;' from dual;
select 'insert into code_destinataire_holde (cds_name, cds_date_demande) values (''' || p.sous_num_tiers || ''',to_date('''||(to_char(sysdate,'dd/mm/yyyy HH24:mi'))||''',''dd/MM/yyyy HH24:mi''));'
from &stcom_schema..pool p
where p.pool_logical_name = 'jdbc/&stcom_schema.' and (p.num_tiers=:numtiers or :numtiers=0) 
and p.pool_jndi_name = ( select 'jdbc/&stcom_schema.' || lpad(id_schema_stcom, 4, '0') from &stcom_schema..id_schema_stcom)
UNION
select 'insert into code_destinataire_holde (cds_name, cds_date_demande) values (''007' || lpad(p.sous_num_tiers,5,'0') || lpad(p.sous_num_tiers,5,'0') || ''',to_date(''' || (to_char(sysdate,'dd/mm/yyyy HH24:mi')) || ''', ''dd/MM/yyyy HH24:mi''));'
from &stcom_schema..pool p
where p.pool_logical_name = 'jdbc/&stcom_schema.' and (p.num_tiers=:numtiers or :numtiers=0) 
and p.pool_jndi_name = ( select 'jdbc/&stcom_schema.' || lpad(id_schema_stcom, 4, '0') from &stcom_schema..id_schema_stcom)
UNION
select 'insert into code_destinataire_holde (cds_name, cds_date_demande) values (''RAC' || lpad(p.sous_num_tiers,5,'0') || ''',to_date(''' || (to_char(sysdate,'dd/mm/yyyy HH24:mi')) || ''', ''dd/MM/yyyy HH24:mi''));'
from &stcom_schema..pool p
where p.pool_logical_name = 'jdbc/&stcom_schema.' and (p.num_tiers=:numtiers or :numtiers=0) 
and p.pool_jndi_name = ( select 'jdbc/&stcom_schema.' || lpad(id_schema_stcom, 4, '0') from &stcom_schema..id_schema_stcom);
spool off
prompt connection to /@&chega_tns_entry._oraexploit
connect /@&chega_tns_entry._oraexploit
prompt executing &output_folder./to_del_rdk_hold_flows_&horodateur..sql
@&output_folder./to_del_rdk_hold_flows_&horodateur..sql

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

