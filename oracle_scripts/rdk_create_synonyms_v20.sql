define synonym_owner=&1
define table_owner=&2
define target_type=&3

set lines 400
set verify off
set feed off
set serveroutput on

begin
execute immediate ('create role select_&table_owner.');
exception
when others then null;
end;
/

declare
st clob;
target_type varchar2(10):='&target_type.';
begin
if upper(target_type)='TABLE' then
 select distinct owner into st from dba_tables where owner =upper('&table_owner.');
end if;
exception
when no_data_found then 
dbms_output.put_line('Exception:create synonyms no table to &table_owner.');
raise;
end;
/

declare
st clob;
target_type varchar2(10):='&target_type.';
begin
if upper(target_type)='SYNONYM' then
 select distinct owner into st from dba_synonyms where owner =upper('&table_owner.');
end if;
exception
when no_data_found then 
dbms_output.put_line('Exception:create synonyms no synonyms to &table_owner.');
raise;
end;
/

declare
st clob;
target_type varchar2(10):='&target_type.';
begin
if upper(target_type)='TABLE' then
 select distinct owner into st from dba_tables where owner =upper('&table_owner.');
end if;
exception
when no_data_found then 
dbms_output.put_line('Exception:create synonyms no table to &table_owner.');
end;
/


declare
target_type varchar2(10):='&target_type.';
begin
	if upper(target_type)='TABLE' then
		for c in
		(
		select 'create or replace synonym &synonym_owner..'||table_name||' for &table_owner..'||table_name cmd,
		'grant select on &table_owner..'||table_name||' to select_&table_owner.' cmd_grants
		from
		dba_tables
		where
		owner =upper('&table_owner.')
		) 
		loop
		 begin
		  dbms_output.put_line(c.cmd||';');
		  execute immediate c.cmd;
		  execute immediate c.cmd_grants;
		 exception
		 when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
		 end;
		end loop;
	end if;
	if upper(target_type)='SYNONYM' then
		for c in
		(
		select 'create or replace synonym &synonym_owner..'||synonym_name||' for &table_owner..'||synonym_name cmd,
		'grant select on &table_owner..'||table_name||' to select_&table_owner.' cmd_grants
		from
		dba_synonyms
		where
		owner =upper('&table_owner.')
		) 
		loop
		 begin
		  dbms_output.put_line(c.cmd||';');
		  execute immediate c.cmd;
		  execute immediate c.cmd_grants;
		 exception
		 when others then dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
		 end;
		end loop;
	end if;
end;
/

grant select_&table_owner. to &synonym_owner.;

