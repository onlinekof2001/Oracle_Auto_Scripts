--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_propage.sql
-- SYNOPSIS:   rdk_propage
-- USAGE:      sqlplus -S -L @/tetrix02_rtdka2odbXX @rdk_propage.sql 
-- PARAMETERS: 
-- 
-- 
-- 
-- 

--	Version
-- 	1.0  Creation	01/03/2016
--------------------------------------------------------------------------------


define slave_schema=&1
define target_tns=&2
define ddl_table=&3
define slave_schema_pwd=Decathlon0147
define output_folder=&4
define tgt_ddl_table=&5


whenever sqlerror EXIT SQL.SQLCODE
prompt testing connection /@&target_tns._oraexploit
connect /@&target_tns._oraexploit

set serveroutput on
set verify off
set head off
set feed off

set lines 750
col ddl for A750 wor
set pages 0
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',TRUE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',FALSE);
set long 10000
set trimspool on
set wrap on

column target_ddl_table new_value target_ddl_table;
set term off
select decode('&tgt_ddl_table.','-','&ddl_table.','&tgt_ddl_table.') target_ddl_table from dual;
set term on

select 'new_mview_name=&target_ddl_table.' from dual;

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
whenever sqlerror CONTINUE

set term off
col param_table_short_name noprint new_value table_short_name 
select substr('&3',1,24) param_table_short_name from dual ;
set term on

grant create materialized view to &slave_schema;
grant connect to &slave_schema;
grant resumable to &slave_schema;
grant create synonym to &slave_schema;
alter user &slave_schema. quota unlimited on &slave_schema._SNAP_DATA ;
alter user &slave_schema. quota unlimited on &slave_schema._SNAP_index ;
alter user &slave_schema. account unlock;
alter user &slave_schema. identified by &slave_schema_pwd.;


rem ********************************************************************************
rem ********************************************************************************

create or replace function oraexploit.tmp_colonnes (powname varchar2, pidxname varchar2) return varchar2 is
    cursor c_col is
           select column_name from sys.dba_ind_columns
           where index_name = pidxname
           and table_owner= powname
           order by column_position;

    c dba_ind_columns.column_name%type;
    str_result varchar2 (255);
begin
     str_result := '(';
     open c_col;
     loop
         fetch c_col into c;
         exit when c_col%notfound;
         str_result := str_result || c || ',';
     end loop;
     close c_col;
     str_result := rpad(str_result,length(str_result)-1)||')';
     return str_result;
exception
   when others then
      return 'Erreur dans la procedure colonnes';
end;
/

spool &output_folder./to_del_propage_as_dba_&slave_schema._&ddl_table._&horodateur..sql

prompt rem ******************************************************************************************
prompt rem ******************************************************************************************
prompt rem                                DEBUT DU SCRIPT GENERE
prompt rem ******************************************************************************************
prompt rem ******************************************************************************************
prompt whenever sqlerror EXIT SQL.SQLCODE
prompt 
prompt prompt CREATION TABLE TAMPON tmp_&ddl_table + DROITS PUBLIC
prompt prompt *************************************
prompt 
prompt alter session enable resumable 
prompt /
prompt alter session set ddl_lock_timeout=3600
prompt /
prompt begin
prompt execute immediate ('drop table &slave_schema..tmp_&table_short_name');;
prompt exception
prompt when others then null;;
prompt end;;
prompt /
prompt create table &slave_schema..tmp_&table_short_name tablespace tda_&horodateur. nologging as select * from &slave_schema..&ddl_table 
prompt /

prompt 
prompt prompt INDEXES SUR LA TABLE TEMPORAIRE
prompt prompt *******************************
select 'create index &slave_schema..TMP_'
	||substr(index_name,1,25)
	||' on &slave_schema..TMP_&ddl_table '
	||oraexploit.tmp_colonnes(upper('&slave_schema'),I.index_name)
	||' tablespace tix_&horodateur. ;'
from dba_indexes I
       where upper(table_owner) = upper('&slave_schema')
       and upper(table_name) = upper('&ddl_table')
       and index_name not like 'PK%'
       and index_name not like '%PK'
/

prompt 
prompt prompt CALCUL STATS SUR TABLE TAMPON
prompt prompt *************************************
select 'exec DBMS_STATS.GATHER_TABLE_STATS ( ownname => '''
	||upper('&slave_schema')
	||''', tabname => ''TMP_'
	||upper('&table_short_name')
	||''', estimate_percent => 5, cascade => true );' from dual
/

prompt 
prompt prompt MAJ DROITS EN LECTURE DE LA TABLE TAMPON
prompt prompt *************************************
select 'grant '
				||privilege
				||' on &slave_schema'
				||'.tmp_&table_short_name to '
				||grantee
				||';' 
	from dba_tab_privs 
	where table_name = upper('&ddl_table') 
	and owner = upper('&slave_schema')
/

prompt 
prompt prompt SUPPRESSION DES ANCIENS SYNONYMES VERS L'OBJET D'ORIGINE &ddl_table
prompt prompt *************************************
select 'drop synonym '
				||owner
				||'.'
				||OBJECT_NAME
				||';' 
	from dba_objects 
	where object_name = upper('&ddl_table') 
	and object_type = 'SYNONYM'
	and owner <> upper('&slave_schema')
/


prompt create public synonym &ddl_table for &slave_schema..tmp_&table_short_name
prompt /
prompt grant select on &slave_schema..tmp_&table_short_name to public
prompt /


prompt 
prompt prompt TOUS LES UTILISATEURS POINTENT DESORMAIS SUR LE TAMPON tmp_&ddl_table
prompt prompt SUPPRESSION DE LA MWIEW &slave_schema..&ddl_table 
prompt prompt *************************************
prompt drop materialized view &slave_schema..&ddl_table;;
prompt alter user &slave_schema. account unlock;;
prompt alter user &slave_schema. identified by &slave_schema_pwd.;;
spool off

spool &output_folder./to_del_propage_as_user_&slave_schema._&ddl_table._&horodateur..sql

prompt alter session enable resumable
prompt /

prompt 
prompt whenever sqlerror EXIT SQL.SQLCODE
prompt prompt CREATE MATERIALIZED_VIEW &target_ddl_table.
prompt prompt *************************************
SELECT rtrim(ltrim(replace(dbms_metadata.get_ddl('MATERIALIZED_VIEW',''||MVIEW_NAME||'',''||OWNER||''),upper('&slave_schema..&ddl_table.'),upper('&slave_schema..&target_ddl_table.')),'"'),'"') DDL FROM dba_mviews where mview_name = upper('&ddl_table.') and owner=upper('&slave_schema.');


prompt 
prompt prompt RECREATION DES INDEXES ASSOCIES ON THE NEW MVIEW
prompt prompt *************************************
rem prompt host pause
SELECT replace(replace(replace(dbms_metadata.get_ddl('INDEX',INDEX_NAME,OWNER),'"',''),'&ddl_table.','&target_ddl_table.'),index_name,rtrim(index_name,'123456789')) DDL FROM dba_indexes  
	where table_name = upper('&ddl_table') 
	and owner=upper('&slave_schema')
	and uniqueness<>'UNIQUE'
/

prompt prompt conneting with connect /@&target_tns._oraexploit
prompt connect /@&target_tns._oraexploit

prompt 
prompt prompt RESTAURATION DES ANCIENS DROITS VERS L'OBJET D'ORIGINE &ddl_table
prompt prompt *************************************
rem prompt host pause
select 'grant '
				||privilege
				||' on '
				||owner
				||'.'
				||replace(table_name,'&ddl_table.','&target_ddl_table.')
				||' to '
				||grantee
				||';' 
		from dba_tab_privs 
		where table_name = upper('&ddl_table')
		and owner = upper('&slave_schema')
/


prompt 
prompt prompt CALCUL DES STATS
prompt prompt *************************************
prompt 
select 'exec DBMS_STATS.GATHER_TABLE_STATS ( ownname => '''
	||upper('&slave_schema')
	||''', tabname => '''
	||upper('&target_ddl_table.')
	||''', estimate_percent => 5, cascade => true );' from dual
/

prompt 
prompt prompt RECREATION DES SYNONYMES VERS L''ANCIEN OBJET &ddl_table
prompt prompt LES UTILISATEURS REPOINTENT VERS CET OBJET
prompt prompt *************************************
rem prompt host pause
select 'create or replace synonym '
				||owner
				||'.'
				||synonym_name
				||' for '
				||table_owner
				||'.'
				||replace(table_name,'&ddl_table.','&target_ddl_table.')
				||';' 
		from dba_synonyms 
		where synonym_name = upper('&ddl_table')
/


prompt 
prompt prompt SUPPRESSION DEFINITIVE DE L''OBJET TAMPON tmp_&ddl_table
prompt prompt LES UTILISATEURS REPOINTENT VERS CET OBJET
prompt prompt *************************************

prompt drop public synonym &ddl_table
prompt /
prompt drop table &slave_schema..tmp_&table_short_name
prompt /


prompt 
prompt prompt REMETTRE LA MVIEW DANS LE REFRESH GROUP
prompt prompt *************************************
prompt 
select 'exec DBMS_REFRESH.ADD('''||ROWNER||'.'||RNAME||''','''||OWNER||'.'||upper('&target_ddl_table')||''',TRUE);'
	from dba_refresh_children where name = upper('&ddl_table.') and owner = upper('&slave_schema.');

prompt 
prompt commit;;
prompt alter user &slave_schema. account lock;;
prompt set define on
prompt 

prompt rem ********************************************************
prompt rem FIN DU SCRIPT GENERE
prompt rem ********************************************************
spool off

connect /@&target_tns._oraexploit
set serveroutput on
prompt creating tmp_&horodateur.
declare
sz_table number:=5000;
sz_index number:=5000;
cmd clob;
db_name_on_slave varchar2(30);
begin
	select lower(name) into db_name_on_slave from v$database;
	select sum(bytes/1024) into sz_table from dba_segments where segment_name=upper('&ddl_table.') and segment_type='TABLE' and owner=upper('&slave_schema.');
	select sum(bytes/1024) into sz_index from dba_segments where segment_name in 
	(select index_name from dba_indexes where table_name=upper('&ddl_table.') and owner=upper('&slave_schema.'))
	and segment_type='INDEX';
	sz_table:=sz_table+round(sz_table*20/100,0);
	sz_index:=sz_table+round(sz_index*20/100,0);
	cmd:='CREATE TABLESPACE tda_&horodateur. DATAFILE ''/u01/app/oracle/oradata/data1/'||db_name_on_slave||'/tda_&horodateur..data1'' SIZE 16M autoextend on next 256M maxsize '||to_char(nvl(sz_table,5000)+5000)||'K';
	dbms_output.put_line(cmd);
	execute immediate cmd;
	cmd:='CREATE TABLESPACE tix_&horodateur. DATAFILE ''/u01/app/oracle/oradata/index1/'||db_name_on_slave||'/tix_&horodateur..data1'' SIZE 16M autoextend on next 256M maxsize '||to_char(nvl(sz_index,5000)+5000)||'K';
	dbms_output.put_line(cmd);
	execute immediate cmd;
EXCEPTION
when others then
dbms_output.put_line('Exception in create tablespace:'||cmd||' SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/

prompt executing &output_folder./to_del_propage_as_dba_&slave_schema._&ddl_table._&horodateur..sql
@&output_folder./to_del_propage_as_dba_&slave_schema._&ddl_table._&horodateur..sql

connect &slave_schema./&slave_schema_pwd.@&target_tns.
prompt executing &output_folder./to_del_propage_as_user_&slave_schema._&ddl_table._&horodateur..sql
@&output_folder./to_del_propage_as_user_&slave_schema._&ddl_table._&horodateur..sql

set serveroutput on
prompt deleting tmp_&horodateur.
declare
cmd clob;
begin
	cmd:='drop TABLESPACE tda_&horodateur. including contents and datafiles';
	dbms_output.put_line(cmd);
	execute immediate cmd;
	cmd:='drop TABLESPACE tix_&horodateur. including contents and datafiles';
	dbms_output.put_line(cmd);
	execute immediate cmd;
EXCEPTION
when others then
dbms_output.put_line('Exception in drop tablespace:'||cmd||' SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/

exit
