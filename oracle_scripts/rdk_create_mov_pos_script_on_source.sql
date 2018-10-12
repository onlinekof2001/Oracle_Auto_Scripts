--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:     rdk_create_mov_pos_script_on_source.sql
-- SYNOPSIS:   generate sql & .sh script to execute on source and create interim schema MOV_STCOM_XXXX & MOV_NBO_XXXX
-- USAGE:      sqlplus -S -L @/tetrix02_rtdka2odbXX @rdk_create_mov_pos_script_on_source.sql
-- PARAMETERS: 
-- 
--  Actions performed 
--  

--	Version
-- 	1.0  Creation	21/01/2016
--------------------------------------------------------------------------------

set serveroutput on
set ECHO off
SET FEEDBACK OFF 
set SHOWmode OFF
set autop off
set verify off
set define on
--VARIABLE num_mag NUMBER;
define nom_schema=&1;
define num_mag=&2;
define nom_target=&3;
define dba_user=&4;
define dba_pwd=&5;
define schema_cible=&6;
--define spool_out=mov_pos_script.&num_mag..sql

DECLARE
DIR varchar2(50) default 'DATA_PUMP_DIR';
mag VARCHAR(10) default '&num_mag';
schema_name VARCHAR(10) default '&nom_schema';
schema_cible VARCHAR(10) default '&schema_cible';
target VARCHAR(50) default '&nom_target';
dba_user VARCHAR(50) default '&dba_user';
dba_pwd VARCHAR(50) default '&dba_pwd';
mag2 VARCHAR(10);
DIRNAME VARCHAR2(255);
file_name varchar2(255);
file_name_sh varchar2(255);
file_name_impdp varchar2(255);
file_name_exec_proc varchar2(255);
file_name_compte varchar2(255);
st long;

PROCEDURE PRINT_FILE(string IN varchar2,filename IN varchar2,dirname IN varchar2) is
  fHandle  UTL_FILE.FILE_TYPE;
BEGIN
  --set serveroutput on;
  FHANDLE := UTL_FILE.FOPEN(DIRNAME, FILENAME, 'a',32767);
  UTL_FILE.PUT_LINE(FHANDLE, STRING,TRUE);
  UTL_FILE.FCLOSE(FHANDLE);
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Exception:PRINT_FILE SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
    return;
END PRINT_FILE;


begin
  
  
  schema_name:=upper(schema_name);
  
  mag:=trim(leading '0' from mag );

  mag2:=mag;
  while length(mag2)<3 loop
     mag2:='0'||mag2;
  END loop;
    
	begin  
    SELECT Username INTO St FROM Dba_Users WHERE Upper(Username)='MOV_'||schema_name||'_'||mag;
    EXCEPTION
    WHEN No_Data_Found THEN
      null;
	end;
	if st is not null then 
    dbms_output.put_line('user '||st||' exists drop before:  drop user '||st||' cascade;');
   -- return;
  end if;
    
     
  select DIRECTORY_PATH into DIRNAME from dba_directories where DIRECTORY_NAME=DIR;
  
   
  PACK_OXY.MOVE_POS(APPLICATION => 'STORES',SCHEMA_SOURCE => schema_name,SCHEMA_CIBLE => schema_cible,POS => '(007,00'||mag2||',00'||mag2||')',EMPLACEMENT_DUMP=>DIR,VDEBUG=>0);
  
  commit; 
  file_name:='mov_'||schema_name||'_'||mag||'.sql';
  begin
    UTL_FILE.FREMOVE(DIR, FILE_NAME);
  EXCEPTION
  WHEN OTHERS THEN NULL;
  END;
  
  file_name_sh:='mov_'||schema_name||'_'||mag||'.sh';
  begin
    UTL_FILE.FREMOVE(DIR, file_name_sh);
  EXCEPTION
  WHEN OTHERS THEN NULL;
  END;
  PRINT_FILE('SET ECHO ON ',file_name,DIR);
  PRINT_FILE('spool /tmp/mov_'||schema_name||'_'||mag||'.log',file_name,DIR);
 -- PRINT_FILE('drop public database link movepos_link;',file_name,DIR);
 -- PRINT_FILE('CREATE public DATABASE LINK movepos_link CONNECT TO oraexploit IDENTIFIED BY judehey USING '''||target||'''',file_name,DIR);

      --dbms_output.put_line(DIRNAME);
  --PRINT_FILE('set echo off',file_name,DIR);
  if schema_name='STCOM' then
    PRINT_FILE('delete from '||schema_name||'.w_scheduler where tir_num_tiers_tir='||mag||' and tti_num_type_tiers_tir=7;',file_name,DIR);
    PRINT_FILE('commit;',file_name,DIR);
  end if;
  
  FOR c IN (SELECT NAME FROM v$tablespace WHERE NAME LIKE ''||schema_name||'_00700'||mag2||'00'||mag2||'%')
  loop
   PRINT_FILE('alter tablespace '||c.NAME||' read only;',file_name,DIR);
  end loop;  
 -- PRINT_FILE('spool off',file_name,DIR);
  
  PRINT_FILE('@'||DIRNAME||'export_on_source_MOV_'||schema_name||'_'||mag||'.sql',file_name,DIR);
  --PRINT_FILE('@'||DIRNAME||'export_on_source_MOV_NBO_'||mag||'.sql',file_name,DIR);
  
  --PRINT_FILE('create table mov_'||schema_name||'_'||mag||'.cmd_movpos as select * from repo.cmd_movpos where upper(schema_temp)=''MOV_'||schema_name||'_'||mag||''' and status=0;',file_name,DIR);
  --  PRINT_FILE('create table mov_nbo_'||mag||'.cmd_movpos as select * from repo.cmd_movpos where upper(schema_temp)=''MOV_NBO_'||mag||''' and status=0;',file_name,DIR);
  
  
  PRINT_FILE('@'||DIRNAME||'create_constraints_on_source_MOV_'||schema_name||'_'||mag||'.sql',file_name,DIR);
  --PRINT_FILE('@'||DIRNAME||'create_constraints_on_source_MOV_NBO_'||mag||'.sql',file_name,DIR);

  PRINT_FILE('exit',file_name,DIR);
  
  
st:='DECLARE
l_dp_handle      NUMBER;
l_job_state      VARCHAR2(30) := ''UNDEFINED'';
DumpDirectory         VARCHAR2(50) := ''DATA_PUMP_DIR'';
FileName_LogDumpedSchema VARCHAR2(50):=''IMPDP_MOV_'||schema_name||'_'||mag||'.log'';
BEGIN
l_dp_handle := sys.DBMS_DATAPUMP.open( operation => ''IMPORT'', job_mode => ''SCHEMA'', remote_link => ''movepos_link'');
sys.DBMS_DATAPUMP.add_file( handle => l_dp_handle, filename => FileName_LogDumpedSchema, directory => DumpDirectory, filetype => sys.DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);
sys.DBMS_DATAPUMP.SET_PARALLEL (handle=>l_dp_handle,DEGREE=>4);
sys.DBMS_DATAPUMP.metadata_filter( handle => l_dp_handle, name => ''SCHEMA_EXPR'',  value => ''= ''''MOV_'||schema_name||'_'||mag||''''''');
sys.DBMS_DATAPUMP.start_job(l_dp_handle);
sys.DBMS_DATAPUMP.WAIT_FOR_JOB(l_dp_handle,l_job_state) ;
sys.DBMS_DATAPUMP.detach(l_dp_handle);
END;
/';

  
  file_name_impdp:='impdp_mov_'||schema_name||'_'||mag||'.sql';
  begin
    UTL_FILE.FREMOVE(DIR, file_name_impdp);
  EXCEPTION
  WHEN OTHERS THEN NULL;
  END;
  
  PRINT_FILE('SPOOL /tmp/impdp_mov_'||schema_name||'_'||mag||'.log',file_name_impdp,DIR);
  PRINT_FILE(st,file_name_impdp,DIR);
  PRINT_FILE('SPOOL off',file_name_impdp,DIR);
  PRINT_FILE('exit',file_name_impdp,DIR);  
  
  PRINT_FILE('#--CREATE SCHEMA TEMP ----------------',file_name_sh,DIR);
  st:='sqlplus -S -L '||dba_user||'/'||dba_pwd||' @'||DIRNAME||file_name;
  PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  PRINT_FILE('echo chek log: tail -f /tmp/mov_'||schema_name||'_'||mag||'.log',file_name_sh,DIR);
  PRINT_FILE(st,file_name_sh,DIR);
  
  
  PRINT_FILE('#--EXPORT SCHEMA TEMP TO '||target||'-------',file_name_sh,DIR);
  st:='sqlplus -S -L '||dba_user||'/'||dba_pwd||'@'||target||' @'||DIRNAME||file_name_impdp;
  PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  PRINT_FILE('echo chek log on /tmp/impdp_mov_'||schema_name||'_'||mag||'.log',file_name_sh,DIR);
  PRINT_FILE(st,file_name_sh,DIR);
  
  
  --PRINT_FILE('#--COUNT LINES ON SOURCE FOR '||schema_name||'------',file_name_sh,DIR);
  --st:='#sqlplus -S -L MOV_'||schema_name||'_'||mag||'/MOV_'||schema_name||'_'||mag||' @'||DIRNAME||'verif_on_source_MOV_'||schema_name||'_'||mag||'.sql';
  --PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  --PRINT_FILE('echo check log on /tmp/verif_on_both_MOV_'||schema_name||'_'||mag||'*..log',file_name_sh,DIR);
  --PRINT_FILE(st,file_name_sh,DIR);
  
  file_name_exec_proc:='generate_insert_file_mov_'||schema_name||'_'||mag||'.sql';
  begin
    UTL_FILE.FREMOVE(DIR, file_name_exec_proc);
  EXCEPTION
  WHEN OTHERS THEN NULL;
  END;
  PRINT_FILE('spool /tmp/generate_insert_file_mov_'||schema_name||'_'||mag||'.log',file_name_exec_proc,DIR);
  PRINT_FILE('exec repo.pack_oxy.exec_cmd_movpos(schema_name=>'''||schema_name||''',mag=>'''||mag||''',target=>'''||target||''',vcmd_type=>''INSERT'');',file_name_exec_proc,DIR);
  PRINT_FILE('spool off',file_name_exec_proc,DIR);
  PRINT_FILE('exit',file_name_exec_proc,DIR);
  st:='sqlplus -S -L '||dba_user||'/'||dba_pwd||' @'||DIRNAME||file_name_exec_proc;
  PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  PRINT_FILE('echo check log on /tmp/generate_insert_file_mov_'||schema_name||'_'||mag||'.log',file_name_sh,DIR);
  PRINT_FILE(st,file_name_sh,DIR);
  
  
  
  PRINT_FILE('#--INSERT ON TARGET FOR '||schema_name||'------',file_name_sh,DIR);
  st:='sqlplus -S -L '||dba_user||'/'||dba_pwd||'@'||target||' @'||DIRNAME||'insert_mov_'||schema_name||'_'||mag||'.sql';
  PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  PRINT_FILE('echo check log on /tmp/insert_mov_'||schema_name||'_'||mag||'*..log',file_name_sh,DIR);
  PRINT_FILE(st,file_name_sh,DIR);

  
  PRINT_FILE('#--MAJ SEQUENCE ON TARGET FOR '||schema_name||'------',file_name_sh,DIR);
  st:='sqlplus -S -L '||dba_user||'/'||dba_pwd||'@'||target||' @'||DIRNAME||'maj_sequence_on_target_MOV_'||schema_name||'_'||mag||'.sql';
  PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  PRINT_FILE('echo check log on /tmp/maj_sequence_on_target_MOV_'||schema_name||'_'||mag||'*..log',file_name_sh,DIR);
  PRINT_FILE(st,file_name_sh,DIR);
  PRINT_FILE('exit',file_name_sh,DIR);  
  
  
  --PRINT_FILE('#--COUNT LINES ON '||target||' FOR '||schema_name||'------',file_name_sh,DIR);
  --st:='sqlplus -S -L MOV_'||schema_name||'_'||mag||'/MOV_'||schema_name||'_'||mag||'@'||target||' @'||DIRNAME||'verif_on_target_MOV_'||schema_name||'_'||mag||'.sql';
  --PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  --PRINT_FILE('echo check log on /tmp/verif_on_both_MOV_'||schema_name||'_'||mag||'*..log',file_name_sh,DIR);
  --PRINT_FILE(st,file_name_sh,DIR);
       
 /* file_name_compte:='controle_lignes_mov_'||schema_name||'_'||mag||'.sql';
  begin
    UTL_FILE.FREMOVE(DIR, file_name_compte);
  EXCEPTION
  WHEN OTHERS THEN NULL;
  END;
  PRINT_FILE('set linesize 800',file_name_compte,DIR);
  PRINT_FILE('select '''||mag||''', '''||schema_name||''', s.nom, s.nb source, d.nb destination from mov_'||schema_name||'_'||mag||'.check_move@MOVEPOS_LINK.HOSTING.EU s',file_name_compte,DIR);
  PRINT_FILE(' inner join mov_'||schema_name||'_'||mag||'.check_move d on s.nom = d.nom ',file_name_compte,DIR);
  PRINT_FILE(' where s.nb<>d.nb; ',file_name_compte,DIR);
  PRINT_FILE('exit',file_name_compte,DIR);
  st:='sqlplus -S -L MOV_'||schema_name||'_'||mag||'/MOV_'||schema_name||'_'||mag||'@'||target||' @'||DIRNAME||file_name_compte;
  PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  PRINT_FILE('echo check log on /tmp/controle_lignes_MOV_'||schema_name||'_'||mag||'*..log',file_name_sh,DIR);
  PRINT_FILE(st,file_name_sh,DIR);*/
   

  file_name_exec_proc:='generate_delete_file_mov_'||schema_name||'_'||mag||'.sql';
  begin
    UTL_FILE.FREMOVE(DIR, file_name_exec_proc);
  EXCEPTION
  WHEN OTHERS THEN NULL;
  END;
  PRINT_FILE('spool /tmp/generate_delete_file_mov_'||schema_name||'_'||mag||'.log',file_name_exec_proc,DIR);
  PRINT_FILE('exec repo.pack_oxy.exec_cmd_movpos(schema_name=>'''||schema_name||''',mag=>'''||mag||''',target=>'''||target||''',vcmd_type=>''DELETE'');',file_name_exec_proc,DIR);
  PRINT_FILE('spool off',file_name_exec_proc,DIR);
  PRINT_FILE('exit',file_name_exec_proc,DIR);
  --st:='sqlplus -S -L '||dba_user||'/'||dba_pwd||' @'||DIRNAME||file_name_exec_proc;
  --PRINT_FILE('echo executing: '||st,file_name_sh,DIR);
  --PRINT_FILE('echo check log on /tmp/generate_delete_file_mov_'||schema_name||'_'||mag||'.log',file_name_sh,DIR);
  --PRINT_FILE(st,file_name_sh,DIR);
  
   
  dbms_output.put_line('--execute on source (ssh) : ');
  dbms_output.put_line('chmod +x '||DIRNAME||''||file_name_sh);
  dbms_output.put_line('. '||DIRNAME||''||file_name_sh);

   
   
    
END;
/

set feedback on