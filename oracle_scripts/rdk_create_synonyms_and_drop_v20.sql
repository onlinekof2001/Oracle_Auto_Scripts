--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_create_synonyms_and_drop.sql
-- SYNOPSIS:  hold flows
-- USAGE:      sqlplus -S -L /nolog @rdk_create_synonyms_and_drop.sql pool slace_tns master_tns dryrun
-- PARAMETERS:
-- pool 

--	Version
-- 	1.0  Creation	04/03/2016
--------------------------------------------------------------------------------
define pool=&1
define slave_tns=&2
define master_tns=&3
define dryrun=&4
define output_folder=&5
define dba_user=&6
define dba_pwd=&7
set lines 400
set verify off
set feed off
set serveroutput on


column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;

column schema_stcom heading "schema_stcom" new_value schema_stcom;
select username as schema_stcom from dba_users where username='STCOM'; 
column schema_nbo heading "schema_nbo" new_value schema_nbo;
select username as schema_nbo from dba_users where username='NBO'; 

var schema_stcom varchar2(15);

declare 
one_user exception;
begin
for c in (select username,COUNT(*) over () tot_rows from dba_users where username like  'MD&pool.%')
loop
 if c.tot_rows=1 then
   RAISE one_user;
 end if;
end loop;
exception
 WHEN one_user THEN
      raise_application_error (-20001,'select username,COUNT(*) over () tot_rows from dba_users where username like  MD&pool.% : return only one 1 row=> exit');
end;
/

column old_md heading "old_md" new_value old_md;
select old_md from (
select username old_md from dba_users where username like 'MD&pool.%' order by created) where rownum=1;

column new_md heading "new_md" new_value new_md;
select new_md from (
select username new_md from dba_users where username like 'MD&pool.%' order by created desc) where rownum=1;


prompt connect @&slave_tns.
connect &dba_user./&dba_pwd.@&slave_tns.
set serveroutput on
set feed off

exec :schema_stcom := '&schema_stcom.';
begin
if :schema_stcom='STCOM' then
for c in
(
select 'create or replace synonym masterdatas.'||mview_name||' for '||owner||'.'||mview_name cmd
from
dba_mviews
where
owner =upper('&new_md.')
) 
loop
 begin
  dbms_output.put_line(c.cmd||';');
  execute immediate c.cmd;
 exception
 when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
 end;
end loop;
else
for c in
(
select 'create or replace synonym md&pool..'||mview_name||' for '||owner||'.'||mview_name cmd
from
dba_mviews
where
owner =upper('&new_md.') and mview_name not in ('PARAMETRES_DETAIL')
) 
loop
 begin
  dbms_output.put_line(c.cmd||';');
  execute immediate c.cmd;
 exception
 when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
 end;
end loop;
end if;
end;
/

begin
if :schema_stcom='STCOM' then
for c in
(
select 'create or replace synonym '||username||'.'||mview_name||' for masterdatas.'||mview_name cmd
from
dba_mviews,dba_users
where
owner =upper('&new_md.') and mview_name not in ('PARAMETRES_DETAIL') and (username = 'NBO&pool.' or username='NBO' or username = 'STCOM&pool.' or username='STCOM' or username='ESTCOM&pool.' or username='ESTCOM0000')
)
loop
 begin
  dbms_output.put_line(c.cmd||';');
  execute immediate c.cmd;
 exception
 when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
 end;
end loop;
else
for c in
(
select 'create or replace synonym '||username||'.'||mview_name||' for md&pool..'||mview_name cmd
from
dba_mviews,dba_users
where
owner =upper('&new_md.') and mview_name not in ('PARAMETRES_DETAIL') and (username = 'NBO&pool.' or username='NBO' or username = 'STCOM&pool.' or username='STCOM' or username='ESTCOM&pool.' or username='ESTCOM0000')
) 
loop
 begin
  dbms_output.put_line(c.cmd||';');
  execute immediate c.cmd;
 exception
 when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
 end;
end loop;
end if;
end;
/


begin
if :schema_stcom='STCOM' then
for c in
(
select 'grant select on '||owner||'.'||mview_name||' to estcom0000' cmd
from
dba_mviews,dba_users
where
owner =upper('&schemaname.') and (username ='ESTCOM&pool.' or username='ESTCOM0000')
) 
loop
 begin
  dbms_output.put_line(c.cmd||';');
  execute immediate c.cmd;
 exception
 when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
 end;
end loop;
else
for c in
(
select 'grant select on '||owner||'.'||mview_name||' to estcom&pool.' cmd
from
dba_mviews,dba_users
where
owner =upper('&schemaname.') and (username='ESTCOM&pool.' or username='ESTCOM0000')
) 
loop
 begin
  dbms_output.put_line(c.cmd||';');
  execute immediate c.cmd;
 exception
 when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
 end;
end loop;
end if;
end;
/

prompt connecting to &slave_tns. to execute @rdk_drop_schema &old_md. &dryrun.
connect &dba_user./&dba_pwd.@&slave_tns.
set serveroutput on
@rdk_drop_schema &old_md. &dryrun.

prompt connecting to &master_tns. 
connect &dba_user./&dba_pwd.@&master_tns.
set serveroutput on
set feed off
set head off
column old_md_master heading "old_md_master" new_value old_md_master;
select rtrim('&old_md.','STCOM') old_md_master from dual;
prompt executing @rdk_drop_schema &old_md_master. &dryrun.
@rdk_drop_schema &old_md_master. &dryrun.
