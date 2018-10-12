--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_prepare_propage.sql
-- SYNOPSIS:   rdk_prepare_propage
-- USAGE:      sqlplus -S -L @/xxxxx02_xxxxx2odbXX @rdk_prepare_propage.sql 
-- PARAMETERS: 
-- 
-- 
-- 
-- 

--	Version
-- 	1.0  Creation	01/03/2016
--------------------------------------------------------------------------------


define slave_schema=&1
define table_name=&2
define output_folder=&3



whenever sqlerror EXIT SQL.SQLCODE
prompt execute rdk_prepare_propage


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

column horodateur heading "horodateur"  new_value horodateur;
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
whenever sqlerror CONTINUE

set term off
col param_table_short_name noprint new_value table_short_name 
select substr('&table_name.',1,24) param_table_short_name from dual ;
set term on

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

spool &output_folder./to_del_propage_as_dba_&slave_schema._&table_name._&horodateur..sql

prompt rem ******************************************************************************************
prompt rem ******************************************************************************************
prompt rem                                DEBUT DU SCRIPT GENERE
prompt rem ******************************************************************************************
prompt rem ******************************************************************************************
prompt whenever sqlerror EXIT SQL.SQLCODE
prompt 
prompt prompt CREATION TABLE TAMPON tmp_&table_name + DROITS PUBLIC
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
prompt create table &slave_schema..tmp_&table_short_name tablespace &slave_schema._snap_data nologging as select * from &slave_schema..&table_name 
prompt /

prompt 
prompt prompt INDEXES SUR LA TABLE TEMPORAIRE
prompt prompt *******************************
select 'create index &slave_schema..tmp_'
	||substr(index_name,1,25)
	||' on &slave_schema..tmp_&table_name. '
	||oraexploit.tmp_colonnes(upper('&slave_schema'),I.index_name)
	||' tablespace &slave_schema._snap_index ;'
from dba_indexes I
       where upper(table_owner) = upper('&slave_schema')
       and upper(table_name) = upper('&table_name')
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
	where table_name = upper('&table_name') 
	and owner = upper('&slave_schema')
/

prompt 
prompt prompt SYNONYMS TABLE TAMPON
prompt prompt *************************************
select 'create or replace synonym '
				||owner
				||'.'
				||synonym_name||' for ' 
				||table_owner||'.tmp_&table_short_name'
				||';' 
	from dba_synonyms 
	where table_name = upper('&table_name') 
	and owner = upper('&slave_schema')
/

prompt drop materialized view &slave_schema..&table_name
prompt /
spool off
@&output_folder./to_del_propage_as_dba_&slave_schema._&table_name._&horodateur..sql
exit
