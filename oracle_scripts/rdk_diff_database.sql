whenever sqlerror EXIT SQL.SQLCODE
set verify off
set serveroutput on
set head on
set feed off
col ddl for A780 wor
set long 10000
set lines 350

define srv_to_check=&1
define remote_srv=&2
define output_folder=&3


exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',FALSE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',FALSE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',FALSE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',TRUE);

column horodateur heading "horodateur"  new_value horodateur;
column reference_srv heading "reference_srv"  new_value reference_srv;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur,'&srv_to_check.' as "Server to check",db_unique_name as reference_srv from dual,v$database;

col name for a40
col &reference_srv. for a50
col &srv_to_check. for a50
select srvref.name,substr(srvref.value,1,45) as "&reference_srv.",substr(srvremote.value,1,45) as "&srv_to_check."
from v$parameter srvref
inner join v$parameter@&remote_srv. srvremote
on srvref.name=srvremote.name
where srvref.isdefault='FALSE' and srvref.value<>srvremote.value
order by 1;

col name for a30
col &reference_srv. for a30
col &srv_to_check. for a30
col HOST for a50
col DB_LINK for a30
col job_name for a30
col owner for a30

select 'LogMode' as "-",substr(srvref.log_mode,1,45) as "&reference_srv.",substr(srvremote.log_mode,1,45) as "&srv_to_check."
from v$database srvref , v$database@&remote_srv. srvremote;

select 'FlshBck' as "-",substr(srvref.FLASHBACK_ON,1,45) as "&reference_srv.",substr(srvremote.FLASHBACK_ON,1,45) as "&srv_to_check."
from v$database srvref,v$database@&remote_srv. srvremote;

select 'UNDO' as "TBS UNDO",'sz_mo='||sum(srvref.bytes/1024/1024)||' maxsz_mo='||sum(srvref.maxbytes/1024/1024) as "&reference_srv.",
'sz_mo='||sum(srvremote.bytes/1024/1024)||' maxsz_mo='||sum(srvremote.maxbytes/1024/1024) as "&srv_to_check."
from dba_data_files srvref, dba_data_files@&remote_srv. srvremote
where srvref.TABLESPACE_NAME in ('UNDO') and srvremote.TABLESPACE_NAME in ('UNDO')
group by srvref.tablespace_name,srvremote.tablespace_name;

select 'TEMP' as "TBS TEMP",'sz_mo='||sum(srvref.bytes/1024/1024)||' maxsz_mo='||sum(srvref.maxbytes/1024/1024) as "&reference_srv.",
'sz_mo='||sum(srvremote.bytes/1024/1024)||' maxsz_mo='||sum(srvremote.maxbytes/1024/1024) as "&srv_to_check."
from dba_temp_files srvref, dba_temp_files@&remote_srv. srvremote
where srvref.TABLESPACE_NAME in ('TEMP') and srvremote.TABLESPACE_NAME in ('TEMP')
group by srvref.tablespace_name,srvremote.tablespace_name;

prompt
prompt --dblinks
select '&reference_srv' as srv,DB_LINK as "DB_LINK",host as "HOST" from dba_db_links order by 1,2;
select '&srv_to_check.' as srv,DB_LINK as "DB_LINK",host as "HOST" from dba_db_links@&remote_srv. order by 1,2;

prompt
prompt --job actifs
select '&reference_srv' as srv,owner as "owner",job_name as "job_name" from dba_scheduler_jobs where enabled='TRUE' and owner not in ('SYS','ORACLE_OCM') order by 1,2;
select '&srv_to_check.' as srv,owner as "owner",job_name as "job_name" from dba_scheduler_jobs@&remote_srv. where enabled='TRUE' and owner not in ('SYS','ORACLE_OCM') order by 1,2;

prompt
prompt --checking for last backup for last 8 days
select '&reference_srv.' as "&reference_srv.",object_type as "Backup Type",count(1) as Nb from v$rman_status srvref 
where object_type is not null
and START_TIME>sysdate-8
group by object_type
order by 2;

select '&srv_to_check.' as "&srv_to_check.",object_type as "Backup Type",count(1) as Nb from v$rman_status@&remote_srv. srvremote 
where object_type is not null
and  START_TIME>sysdate-8
group by object_type
order by 2;


set head off

--db_link
--job_actif


prompt
prompt --checking for missing profiles
select dbms_metadata.get_ddl('PROFILE', profile) ddl from (select profile from dba_profiles
minus
select profile from dba_profiles@&remote_srv.);


prompt
prompt --checking for missing tablespaces
with v1 as (select dbms_metadata.get_ddl('TABLESPACE', name) ddl from (select name from v$tablespace
minus
select name from v$tablespace@&remote_srv.))
select substr(ddl,1,instr(ddl,'ALTER')-1) as "ddl" from v1;

prompt
prompt --checking for missing roles
select 'create role '||role||';' from (select role from dba_roles@&remote_srv.
minus
select role from dba_roles);

prompt
prompt --checking for missing users
select dbms_metadata.get_ddl('USER', username) ddl from (select username from dba_users
where account_status='OPEN'
minus
select username from dba_users@&remote_srv.);

prompt
prompt --checking for role grant
select dbms_metadata.get_granted_ddl('ROLE_GRANT', username) ddl from (select username from dba_users u
inner join dba_role_privs r on r.grantee=u.username
where account_status='OPEN' 
minus
select username from dba_users@&remote_srv.);

prompt
prompt --checking for system grant (may be executed as system or / as sysdba)
select dbms_metadata.get_granted_ddl('SYSTEM_GRANT', username) ddl from (select username from dba_users u
inner join dba_sys_privs r on r.grantee=u.username
where account_status='OPEN'
minus
select username from dba_users@&remote_srv.);

prompt
prompt --checking for object grant
select dbms_metadata.get_granted_ddl('OBJECT_GRANT', username) ddl from (select username from dba_users u
inner join dba_tab_privs r on r.grantee=u.username
where account_status='OPEN'
minus
select username from dba_users@&remote_srv.);

prompt
prompt --checking for tablespace quota
select dbms_metadata.get_granted_ddl('TABLESPACE_QUOTA', username) ddl from (select u.username from dba_users u
inner join dba_ts_quotas r on r.username=u.username
where account_status='OPEN'
minus
select username from dba_users@&remote_srv.);

prompt
prompt --setting default role to YES
select 'alter user '||username||' default role all;' ddl from (select username from dba_users
where account_status='OPEN'
minus
select username from dba_users@&remote_srv.);

	
prompt --drop db link &remote_srv. 
declare
st long;
begin
	st:='drop database link &remote_srv.';
	execute immediate st;
EXCEPTION
WHEN OTHERS THEN
   dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM || 'qry='||st);
 end;
/



