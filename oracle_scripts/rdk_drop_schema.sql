

define schemaname=&1
define dryrun=&2
set lines 512
set verify off
set feed off
set serveroutput on
whenever sqlerror EXIT SQL.SQLCODE

--declare
--nb number:=0;
--synonym_exists exception;
--begin
	--select count(*) into nb from dba_synonyms where table_owner=upper('&schemaname.') and (owner like 'MD%' or owner='MASTERDATAS');
	--if nb>0 then 
	--	RAISE synonym_exists;
	--end if;
--exception
 --WHEN synonym_exists THEN
  --    raise_application_error (-20001,'select count(*) nb from dba_synonyms where table_owner=upper(''&schemaname.'') shows that synonyms exists ! schema not dropped !');
--end;
--/


declare
dryrun varchar(10):='&dryrun.';
begin
if (dryrun='true') then 
	dbms_output.put_line('not executed drop user &schemaname. cascade;');
else
	execute immediate('drop user &schemaname. cascade');
	dbms_output.put_line('executed');
end if;
end;
/

