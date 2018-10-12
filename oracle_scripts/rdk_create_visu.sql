prompt rdk_create_visu.sql

set feedback off
col param1 noprint new_value username
col param2 noprint new_value password
col param3 noprint new_value dryrun

set verify off
set term off
select upper('&1') param1 from dual;
set term on

set verify off
set term off
select '&2' param2 from dual;
set term on
 
set verify off
set term off
select '&3' param3 from dual;
set term on

define dryrun=&3

prompt USERNAME        => VISU_&&username.
prompt PASSWORD        => xxxxxxxxxx
prompt DRYRUN          => &&dryrun.

col param4 noprint new_value upper_username
col param5 noprint new_value instance_name

select upper('&&username.') param4 from dual;
select lower(''||instance_name||'') param5 from v$instance;

set serveroutput on

declare
 st clob;
 dryrun varchar(10):='&dryrun.';
begin
   

	st:='CREATE USER VISU_&upper_username. IDENTIFIED BY "&password." DEFAULT TABLESPACE DBTOOLS TEMPORARY TABLESPACE TEMP';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('create user executed');
	end if;

    st:='alter user VISU_&upper_username. QUOTA UNLIMITED ON DBTOOLS';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed ' || st || ';');
	end if;

    
--ROLES
  
    st:='create role select_&username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed ' || st || ';');
	end if;
	
	for c in (select table_name from 
	dba_tables where 
	owner=upper('&username.') 
	and table_name not like 'MLOG$%' 
	and table_name not like 'RUPD$%' 
	and table_name not like 'BIN$%')
	loop
		st:='grant select on &username..'||c.table_name||' to select_&username.';
		if (dryrun='true') then
			dbms_output.put_line('not executed ' || st || ';');
		else
			begin
				execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
			dbms_output.put_line('executed ' || st || ';');
		end if;
    end loop;

--GRANT USER

    st:='grant create session to VISU_&upper_username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed ' || st || ';');
	end if;

    st:='grant select_&username. to VISU_&upper_username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed ' || st || ';');
	end if;

end;
/



select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
whenever sqlerror EXIT SQL.SQLCODE

