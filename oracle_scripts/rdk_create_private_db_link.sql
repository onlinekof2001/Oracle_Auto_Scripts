--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_create_private_db_link.sql
-- SYNOPSIS:  hold flows
-- USAGE:      sqlplus -S -L /nolog @rdk_create_private_db_link.sql name source target target_user target_passwd replace dryrun port service_name
-- PARAMETERS:
-- source = tns_source
-- cible = tns_target
-- publi c= Y/N
-- 

--	Version
-- 	1.0  Creation	23/02/2016
--. /RUNDECK/rundeck_scripts/ORACLE_DB/bash_profile && /usr/bin/sqlplus64 -S -L /nolog @rdk_create_db_link.sql ${option.60_db_link_name} ${option.10_source_tns} ${option.20_target_tns} ${option.30_trgt_user} ${option.40_trgt_pwd} ${option.70_replace} ${option.50_dryrun} ${option.25_port} ${option.70_service_name}

--------------------------------------------------------------------------------


set serveroutput on
set long 10000
set verify off
define db_link_name=&1
define source_tns_entry=&2
define target_tns_entry=&3
define public=&4
define replace=&5
define dryrun=&6
define port=&7
define service_name=&8
define trgt_user=&9
define trgt_pwd=&10


prompt testing connection with external authentication on /@&target_tns_entry._oraexploit.
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect /@&target_tns_entry._oraexploit
column db_domain  heading "db_domain"  new_value db_domain;
select value db_domain from v$parameter where name='db_domain';

prompt testing connection with external authentication on /@&source_tns_entry._oraexploit.
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect /@&source_tns_entry._oraexploit

set serveroutput on
set feed off

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;



declare
cmd clob;
link_type varchar2(10);
dryrun varchar(10):='&dryrun.';
drop_if_exists varchar(10):='&replace.';
begin
 --select decode('&public.','YES','public') into link_type from dual;
 if drop_if_exists='TRUE' then
  begin
  execute immediate 'drop '||link_type||' database link &db_link_name.';
  exception
  when others then 
  dbms_output.put_line('Exception during drop SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  end;
 end if;
 cmd:='create '||link_type||' database link &db_link_name. connect to &trgt_user. identified by "&trgt_pwd." using ''(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = '||substr('&target_tns_entry.',instr('&target_tns_entry.','_')+1,length('&target_tns_entry.')-instr('&target_tns_entry.','_'))||'.&db_domain.)(PORT = &port.))) (CONNECT_DATA =(SERVICE_NAME = &service_name..&db_domain.)))''';
 if (dryrun='true') then 
	dbms_output.put_line('DRYRUN=TRUE: '||cmd);
 else
	dbms_output.put_line(cmd);
	execute immediate(cmd);
 end if;
exception
when others then
dbms_output.put_line('Exception creating SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/

--test db_link
declare
cmd clob;
begin
cmd:='select * from dual@&db_link_name.';
execute immediate cmd;
dbms_output.put_line('Database Link is ok');
exception
when others then 
dbms_output.put_line('Exception Testing Database link &db_link_name. SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/

