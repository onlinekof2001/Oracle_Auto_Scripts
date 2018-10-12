--ORACLE_PATH=/RUNDECK/rundeck_scripts/ORACLE_DB ORACLE_HOME=/usr/lib/oracle/12.1/client64 TNS_ADMIN=$ORACLE_HOME LD_LIBRARY_PATH=$ORACLE_HOME/lib /usr/bin/sqlplus64 -S -L /nolog @/RUNDECK/rundeck_scripts/ORACLE_DB/rdk_create_mviews_md00XX.sql 0002 tetrix02_rtdkz2odb02 tetrix02_infrz1odb20 /tmp

--ORACLE_PATH=/RUNDECK/rundeck_scripts/ORACLE_DB ORACLE_HOME=/usr/lib/oracle/12.1/client64 TNS_ADMIN=$ORACLE_HOME LD_LIBRARY_PATH=$ORACLE_HOME/lib /usr/bin/sqlplus64 -S -L /nolog @/RUNDECK/rundeck_scripts/ORACLE_DB/rdk_create_mviews_on_slave.sql 0002 tetrix02_rtdkz2odb02 tetrix02_infrz1odb20 /tmp

-- grant alter any materialized view to dba;
-- grant select on sys.user$ to dba;
--0022 pic_rtstz1odb22 tetrix02_infrz1odb20 /tmp true oraexploit oraexploit
--select 'drop materialized view md0000stcom.'||mview_name||';' from dba_mviews where owner='MD0000STCOM' intersect select 'drop materialized view md0000stcom.'||mview_name||';' from dba_mviews where owner like'MD00___STCOM' order by 1;

set verify off

whenever sqlerror EXIT SQL.SQLCODE

set lines 512

define pool=&1
define slave_tns_entry=&2
define master_tns_entry=&3
define master_schema=&4
define visu_master_schema_pwd=&5
define master_tns=master_&4
define rowner=&6
define rname=&7
define outputfolder=&8
define dba_user=&9
define dba_pwd=&10
define force=&11
define frequence=4


define mduserpwd=Decathlon0147

define visu_master_schema=visu_&master_schema.

prompt testing connection with external authentication on /@&slave_tns_entry.
whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
connect &dba_user./&dba_pwd.@&slave_tns_entry.


column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
define outputfile=&outputfolder/to_del_create_mviews_md00XX_master_&horodateur..sql


select DB_UNIQUE_NAME from v$database;
define force=false
define schema_stcom=stcom
define refresh_method=DEMAND


declare
msg varchar2(30);
begin
	select owner into msg from dba_tables where table_name='BLOCAGE_SUIVI' and rownum=1;
	exception
	when no_data_found then
	 begin
	 execute immediate('drop synonym stcom.blocage_suivi');
	 exception
	 WHEN OTHERS THEN NULL;
	 end;
	 execute immediate('create table &schema_stcom..blocage_suivi as select 0 as TTI_NUM_TYPE_TIERS_TIR,0 as TIR_NUM_TIERS_TIR from dual');
end;
/

whenever sqlerror exit 0
set serveroutput on
declare
several_user exception;
creation_not_needed exception;
type_tiers number;
cpt number:=0;
msg varchar2(1024):='';
mview_ddl clob;
force boolean:=&force.;
chk_numtiers number:=0;
begin
	begin
	select dbms_metadata.get_ddl('MATERIALIZED_VIEW',mview_name,owner) into mview_ddl from dba_mviews where mview_name='BLOCAGE_SUIVI'
	and owner in (select table_owner from dba_synonyms where synonym_name='BLOCAGE_SUIVI' and owner='MASTERDATAS');
	exception
	when no_data_found then cpt:=1;
	dbms_output.put_line('no stores');
	end;

	for c in (select tr.tir_num_tiers tiers
	from &schema_stcom..tiers_ref tr
	inner join &schema_stcom..pool po
	on po.num_tiers = tr.tir_num_tiers
	where tr.tti_num_type_tiers_tti  = 7
	and po.pool_logical_name = 'jdbc/stcom'
	and po.pool_jndi_name = ( select 'jdbc/stcom' || lpad(id_schema_stcom, 4, '0') from &schema_stcom..id_schema_stcom))
	loop
    chk_numtiers:= instr(mview_ddl,'='||c.tiers)+instr(mview_ddl,'=0'||c.tiers);
		if chk_numtiers=0 then
     msg:=msg||to_char(c.tiers)||' ';
     cpt:=cpt+1;
    end if;
	end loop;
	if cpt<>0 then dbms_output.put_line('--Recreation needed for stores '||msg);end if;
	if (cpt=0 and not force) then RAISE creation_not_needed; end if;
	exception
	WHEN several_user THEN
		  raise_application_error (-20001,'select username,COUNT(*) over () tot_rows as schema_stcom from dba_users where username=''STCOM'' return more than 1 row');
	WHEN creation_not_needed THEN
		  raise_application_error (-20001,'ddl for blocage_suivi ok: recreation is not needed');
end;
/

set feed off
column pool heading "pool" new_value pool;
select lpad(ID_SCHEMA_STCOM,4,'0') as pool from stcom.id_schema_stcom;
column old_mduser heading "old_mduser" new_value old_mduser;
select owner old_mduser from dba_tables where table_name='BLOCAGE_SUIVI';
var old_mduser varchar2(15);
exec :old_mduser := '&old_mduser.';



column mduser heading "mduser" new_value mduser;
col mduser for a30
col username for a30
select upper('MD&pool.'||decode(rtrim(ltrim(lower(username),'md&pool.'),'stcom'),'a','b','b','a')||'STCOM') as mduser,username
from dba_users where username in (select owner from dba_mviews where rtrim(mview_name,'123456789')='JM_RANKING_EN') and rownum=1;

var mdexist varchar2(15);
exec :mdexist := upper('&mduser.');

--if mduser is null then default=MD&pool.A
select upper(decode(:mdexist,'','md&pool.astcom',:mdexist)) mduser from dual;

var master_schema varchar2(30);
exec :master_schema := '&master_schema.';


column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
define logfile=to_del_create_mviews_md00XX_master_&horodateur..log
set head on
set feedback off
column db_name heading "db_name" new_value db_name;
select lower(name) as db_name from v$database;

set head off


spool &outputfile.
prompt --creating tbs if needed...

set serveroutput on
prompt --creating tbs &mduser._SNAP_DATA &mduser._SNAP_INDEX

declare
st clob;
nb number:=0;
begin
select count(*) into nb from dba_tablespaces where upper(tablespace_name)=upper('&mduser._SNAP_DATA');
if nb=0 then
  st:='CREATE SMALLFILE TABLESPACE &mduser._SNAP_DATA DATAFILE ''/u01/app/oracle/oradata/data1/&db_name./&mduser._snap_data.data1'' SIZE 16M REUSE autoextend on next 128M maxsize 30G;';
  dbms_output.put_line(st);
end if;
end;
/
declare
st clob;
nb number:=0;
begin
select count(*) into nb from dba_tablespaces where upper(tablespace_name)=upper('&mduser._SNAP_INDEX');
if nb=0 then
  st:='CREATE SMALLFILE TABLESPACE &mduser._SNAP_INDEX DATAFILE ''/u01/app/oracle/oradata/index1/&db_name./&mduser._snap_index.data1'' SIZE 16M REUSE autoextend on next 128M maxsize 30G;';
  dbms_output.put_line(st);
end if;
end;
/

prompt --creating user &mduser.
declare
st clob;
nb number:=0;
begin
select count(*) into nb from dba_users where upper(username)=upper('&mduser.');
if nb=0 then
  st:= 'create user &mduser. identified by "&mduserpwd."
  default tablespace &mduser._SNAP_DATA temporary tablespace temp
  profile DEFAULT;';
  dbms_output.put_line(st);
  select count(*) into nb from dba_roles where role=upper('select_&mduser.');
  if nb=0 then
   st:= 'create role select_&mduser.;';
   dbms_output.put_line(st);
  end if;
end if;
end;
/

prompt --settings role to the user &mduser.
prompt grant RESOURCE to &mduser.;;
prompt grant CONNECT to &mduser.;;
prompt grant CREATE DATABASE LINK to &mduser.;;
prompt grant create job to &mduser.;;
prompt grant RESUMABLE to &mduser. ;;
prompt grant CREATE MATERIALIZED VIEW to &mduser. ;;
prompt grant create table to  &mduser. ;;
prompt grant execute on ORAEXPLOIT.PKG_DUREE to &mduser.;;
prompt alter user &mduser. quota unlimited on &mduser._SNAP_DATA;;
prompt alter user &mduser. quota unlimited on &mduser._SNAP_INDEX;;

prompt --creating database link master_&master_schema. to &master_schema. if needed
prompt set echo off
prompt connect &mduser./&mduserpwd.@&slave_tns_entry.

declare
st clob;
nb number :=0;
begin
select count(*) into nb from  dba_db_links where db_link like upper('master_&master_schema.%') and owner=upper('&mduser.');
if nb=0 then
dbms_output.put_line('set feed off');
dbms_output.put_line('set head off');
st:='create database link master_&master_schema. connect to &visu_master_schema. identified by &visu_master_schema_pwd.  using ''&master_tns_entry.'';';
dbms_output.put_line(st);
end if;
end;
/

spool off

set verify off
set serveroutput on
set feedback off
set trimspool on
set lines 550

whenever sqlerror EXIT SQL.SQLCODE --CONTINUE
--connect / as sysdba
spool &outputfile. append

set serveroutput on
set head on
set feedback off

declare
  liste_vends clob;
  liste_mag clob;
  liste_pays clob;
  liste_pere clob;
  st clob;
  date_prix_unit_cession_mag varchar2(15);
  date_prix_unit_histo varchar2(15);
  date_now varchar2(15);
  numerror number:=0;
  refresh_method varchar2(20):='&refresh_method.';

  FUNCTION get_pere(
  pPOOL VARCHAR2)
  RETURN CLOB
  IS st clob:=null;
  begin
   for myCursor in (SELECT lt2.tir_num_tiers_pere pere
FROM &schema_stcom..lien_tiers lt
INNER JOIN &schema_stcom..lien_tiers lt2
      ON lt2.tti_num_type_tiers_fils = lt.tti_num_type_tiers_pere
      AND lt2.tir_num_tiers_fils = lt.tir_num_tiers_pere
      AND lt2.tir_sous_num_tiers_fils = lt.tir_sous_num_tiers_pere
      AND lt2.tyl_type_lien_tiers_tyl = 400
WHERE lt.tyl_type_lien_tiers_tyl = 401
and lt.tir_num_tiers_fils in (select f.tir_num_tiers_chn from &schema_stcom..filiere_tiers f
  where f.tyl_type_lien_tiers_tyl=100
  and f.tti_num_type_tiers_mag=7
  and f.tti_num_type_tiers_chn=15
  and f.tir_num_tiers_chn<>0
  and f.tir_num_tiers_mag in (select p.num_tiers from &schema_stcom..pool p where p.pool_jndi_name in (select 'jdbc/stcom'||lpad(i.id_schema_stcom,4,0) from &schema_stcom..id_schema_stcom i))
  group by f.tir_num_tiers_chn)
group by lt2.tir_num_tiers_pere
order by lt2.tir_num_tiers_pere)
   loop
    st:=st||myCursor.pere||',';
   end loop;
  if st is not null then
		st:=rtrim(st,',');
  else
		st:='0';
  end if;
	return st;
  EXCEPTION
  WHEN no_data_found then st:='0';
  WHEN OTHERS THEN
    dbms_output.put_line('Exception:get_pere  SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  end get_pere;

  FUNCTION get_pays(
  pPOOL VARCHAR2)
  RETURN CLOB
  IS st clob:=null;
  begin
   for myCursor in (select f.tir_num_tiers_chn pays from &schema_stcom..filiere_tiers f
where f.tyl_type_lien_tiers_tyl=100
and f.tti_num_type_tiers_mag=7
and f.tti_num_type_tiers_chn=15
and f.tir_num_tiers_chn<>0
and f.tir_num_tiers_mag in (select p.num_tiers from &schema_stcom..pool p where p.pool_jndi_name in ( select 'jdbc/stcom' || lpad(id_schema_stcom, 4, '0') from &schema_stcom..id_schema_stcom))
group by f.tir_num_tiers_chn
order by f.tir_num_tiers_chn)
   loop
    st:=st||myCursor.pays||',';
   end loop;
	if st is not null then
	 st:=rtrim(st,',');
	else
	 st:='0';
	end if;
	return st;
  EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('Exception:get_pays  SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  end get_pays;

  FUNCTION get_vends(
      pPOOL VARCHAR2)
    RETURN CLOB
  IS
    st CLOB:=null;
  BEGIN
    FOR myCursor IN
    (select distinct ft.tir_num_tiers_chn tiers
  from   &schema_stcom..pool p,
  &schema_stcom..filiere_tiers ft
  where  p.pool_logical_name ='jdbc/stcom'
  and    p.type_tiers = ft.tti_num_type_tiers_mag
  and    p.num_tiers  = ft.tir_num_tiers_mag
  and    p.sous_num_tiers = ft.tir_sous_num_tiers_mag
  and    ft.tyl_type_lien_tiers_tyl = 100
  and    ft.tti_num_type_tiers_chn = 15
  and    ft.tir_num_tiers_chn <> 0
    )
    LOOP
      st:=st||myCursor.tiers||',';
    END LOOP;

	if st is not null then
	 st:=rtrim(st,',');
	else
	 st:='0';
	end if;

	RETURN st;
  EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('Exception:get_vends  SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  END get_vends;

  FUNCTION get_tiers(
      pPOOL VARCHAR2)
    RETURN CLOB
  IS
    st CLOB:=null;
  BEGIN
    FOR myCursor IN
    (/*SELECT DISTINCT p.num_tiers tiers
    FROM &schema_stcom..pool p
    WHERE p.pool_logical_name = 'jdbc/stcom'*/
select tr.tir_num_tiers tiers
from &schema_stcom..tiers_ref tr
inner join &schema_stcom..pool po
on po.num_tiers = tr.tir_num_tiers
where tr.tti_num_type_tiers_tti  = 7
and po.pool_logical_name = 'jdbc/stcom'
and po.pool_jndi_name = ( select 'jdbc/stcom' || lpad(id_schema_stcom, 4, '0') from &schema_stcom..id_schema_stcom)
    )
    LOOP
      st:=st||myCursor.tiers||',';
    END LOOP;
	if st is not null then
	 st:=rtrim(st,',');
	else
	 st:='0';
	end if;
	RETURN st;
  EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('Exception:get_tiers  SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
  END get_tiers;

  begin

  numerror:=10;
  liste_vends:=get_vends(&pool.);
  numerror:=20;
  liste_mag:=get_tiers(&pool.);
  numerror:=30;
  liste_pays:=get_pays(&pool.);
  numerror:=35;
  liste_pere:=get_pere(&pool.);
  numerror:=35;
  select to_char(sysdate-730,'DD/MM/YYYY') into date_prix_unit_cession_mag from dual;
  select to_char(sysdate-810,'DD/MM/YYYY') into date_prix_unit_histo from dual;
  select to_char(sysdate,'DD/MM/YYYY') into date_now from dual;
  numerror:=40;
  dbms_output.put_line('--Filter for liste_vend='||liste_vends);
  dbms_output.put_line('--Filter for liste_tiers='||liste_mag);
  dbms_output.put_line('--Filter for liste_pays='||liste_pays);
  dbms_output.put_line('--Filter for date_prix_unit_cession_mag='||date_prix_unit_cession_mag);
  dbms_output.put_line('--Filter for date_prix_unit_histo='||date_prix_unit_histo);

  dbms_output.put_line(' prompt -- if where predicat filter does not return any rows then table won''t be created except with this alter session');
  dbms_output.put_line('alter session set REMOTE_DEPENDENCIES_MODE=SIGNATURE;');

  st:='create materialized view BLOCAGE_SUIVI noparallel tablespace &mduser._snap_data pctfree 0 build immediate using index tablespace &mduser._snap_index pctfree 0 refresh force on &refresh_method. as select * from &master_schema..BLOCAGE_SUIVI@master_&master_schema. where tir_num_tiers_tir in ('||liste_mag||') order by tir_num_tiers_tir';
  dbms_output.put_line('prompt --executing '||substr(st,1,45)||'...');
  dbms_output.put_line(st||';');


  st:='create materialized view ELT_GESTION_ECHANGE noparallel tablespace &mduser._snap_data pctfree 0 build immediate using index tablespace &mduser._snap_index pctfree 0 refresh force on &refresh_method. as select * from &master_schema..ELT_GESTION_ECHANGE@master_&master_schema where TTI_NUM_TYPE_TIERS_VEND=15 and TIR_NUM_TIERS_VEND in ('||liste_pays||') order by TIR_NUM_TIERS_VEND';
  dbms_output.put_line('prompt --executing '||st||'...');
  dbms_output.put_line(st||';');

  st:='create materialized view JM_BLOCAGE_SUIVI noparallel tablespace &mduser._snap_data pctfree 0 build immediate using index tablespace &mduser._snap_index pctfree 0 refresh force on &refresh_method. as select * from &master_schema..JM_BLOCAGE_SUIVI@master_&master_schema where TTI_NUM_TYPE_TIERS_TIR=7 and tir_num_tiers_tir in ('||liste_mag||') order by jm_id';
  dbms_output.put_line('prompt --executing '||st||'...');
  dbms_output.put_line(st||';');

  st:='create materialized view JM_RANKING_EN noparallel tablespace &mduser._snap_data pctfree 0 build immediate using index tablespace &mduser._snap_index pctfree 0 refresh force on &refresh_method. as select * from &master_schema..JM_RANKING_EN@master_&master_schema where TTI_NUM_TYPE_TIERS_TIR=7 and tir_num_tiers_tir in ('||liste_mag||') order by jm_id';
  dbms_output.put_line('prompt --executing '||st||'...');
  dbms_output.put_line(st||';');

  st:='create materialized view PRIX_UNIT_CESSION_MAG noparallel tablespace &mduser._snap_data pctfree 0 build immediate using index tablespace &mduser._snap_index pctfree 0 refresh force on &refresh_method. as select * from &master_schema..PRIX_UNIT_CESSION_MAG@master_&master_schema where TIR_NUM_TIERS_VEND in ('||liste_pays||') and pri_date_fin > to_date('''||date_prix_unit_cession_mag||''',''DD/MM/YYYY'') order by TIR_NUM_TIERS_VEND';
  dbms_output.put_line('prompt --executing '||st||'...');
  dbms_output.put_line(st||';');

  st:='create materialized view PRIX_UNIT_HISTO noparallel tablespace &mduser._snap_data pctfree 0 build immediate using index tablespace &mduser._snap_index pctfree 0 refresh force on &refresh_method. as select * from &master_schema..PRIX_UNIT_HISTO@master_&master_schema where TIR_NUM_TIERS_VEND in ('||liste_pays||') and pri_date_fin > to_date ('''||date_prix_unit_histo||''',''DD/MM/YYYY'') order by TIR_NUM_TIERS_VEND';
  dbms_output.put_line('prompt --executing '||st||'...');
  dbms_output.put_line(st||';');

  st:='create materialized view PRIX_UNIT_VENTE noparallel tablespace &mduser._snap_data pctfree 0 build immediate using index tablespace &mduser._snap_index pctfree 0 refresh force on &refresh_method. as select * from &master_schema..PRIX_UNIT_VENTE@master_&master_schema where TIR_NUM_TIERS_VEND in ('||liste_pays||') order by TIR_NUM_TIERS_VEND';
  dbms_output.put_line('prompt --executing '||st||'...');
  dbms_output.put_line(st||';');

  st:='create materialized view DISPO_F_AND_R noparallel tablespace &mduser._snap_data pctfree 0 build immediate using index tablespace &mduser._snap_index pctfree 0 refresh force on &refresh_method. as select * from &master_schema..DISPO_F_AND_R@master_&master_schema where TIR_NUM_TIERS_TIR_FNR in ('||liste_mag||') order by TIR_NUM_TIERS_TIR_FNR';
  dbms_output.put_line('prompt --executing '||st||'...');
  dbms_output.put_line(st||';');

EXCEPTION
WHEN OTHERS THEN
    dbms_output.put_line('Exception:create to_del_run_on_master '||numerror||'  SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/
spool off
@&outputfile.

--prompt connect / as sysdba
prompt settings grants for &mduser.
connect &dba_user./&dba_pwd.@&slave_tns_entry.
begin
for c in (select 'grant select on '||owner||'.'||table_name||' to select_&mduser.' as cmd from dba_tables where owner=upper('&mduser.')) 
loop
execute immediate (c.cmd);
end loop;
end;
/


@rdk_move_mviews_to_rgroup.sql &mduser. &rowner. &rname. &frequence. &outputfolder.

begin
execute immediate 'drop table stcom.blocage_suivi';
exception
when others then null;
end;
/

exit
