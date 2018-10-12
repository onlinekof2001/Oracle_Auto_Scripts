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
rolename varchar(256);
CURSOR C_role(par_name varchar2) IS SELECT * FROM dba_roles where role like par_name;
R_role C_role%rowtype;

begin
  select decode('&dryrun.','','false','&dryrun.') into dryrun from dual;

  if (dryrun='true') then 
	dbms_output.put_line('not executed drop user &schemaname. cascade;');
        dbms_output.put_line('not executed drop tablespace (&schemaname._data including contents and datafiles;');
        dbms_output.put_line('not executed drop tablespace &schemaname._index including contents and datafiles;');
  else
	--user
        begin
	execute immediate('drop user &schemaname. cascade');
 	-- tbs
	execute immediate('drop user tablespace &schemaname._data including contents and datafiles');
	execute immediate('drop user tablespace &schemaname._index including contents and datafiles');
        exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
	end;
  end if;

  rolename := '%' || upper('&schemaname.') || '%';
  open C_role(rolename);
  loop
        FETCH C_role into R_role;
	EXIT WHEN C_role%NOTFOUND;
        if  (dryrun='true') then
          dbms_output.put_line('not executed drop role ' || R_role.role );
        else
          begin
           execute immediate('drop role ' || R_role.role );
          exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
        end;

	end if;
        close C_role;
  end loop;

  if (dryrun='true') then
	dbms_output.put_line('rollback');
  else
        dbms_output.put_line('executed');
  end if;
end;
/


