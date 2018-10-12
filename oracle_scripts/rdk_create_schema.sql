prompt rdk_create_schema.sql


set feedback off
col param1 noprint new_value username
col param2 noprint new_value password
col param3 noprint new_value dryrun

prompt USER : 
set verify off
set term off
select upper('&1') param1 from dual;
set term on
prompt PASSWORD : 
set verify off
set term off
select '&2' param2 from dual;
set term on
prompt DRYRUN : 
set verify off
set term off
select '&3' param3 from dual;
set term on

define dryrun=&3

prompt USERNAME        => &&username.
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
    st:='CREATE TABLESPACE &&upper_username._DATA LOGGING DATAFILE ''/u01/app/oracle/oradata/data1/&&instance_name./&&username._data.data1'' SIZE 128m autoextend on next 128m maxsize unlimited';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;
        
	st:='CREATE TABLESPACE &&upper_username._INDEX LOGGING DATAFILE ''/u01/app/oracle/oradata/index1/&&instance_name./&&username._index.data1'' SIZE 128m autoextend on next 128m maxsize unlimited';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;

	st:='CREATE USER &&username. IDENTIFIED BY "&&password." DEFAULT TABLESPACE &&upper_username._DATA TEMPORARY TABLESPACE TEMP';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;

    st:='alter user  &&username. QUOTA UNLIMITED ON &&upper_username._DATA';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;

    st:='alter user  &&username. QUOTA UNLIMITED ON &&upper_username._INDEX';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;


--ROLES
  
    st:='create role select_&&username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;

    st:='create role modify_&&username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;

--GRANT USER

    st:='grant create session to &&username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;

    st:='grant create table to &&username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;

    st:='grant create view to &&username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;


    st:='grant create sequence to &&username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;

    st:='grant create procedure to &&username.';
	if (dryrun='true') then
		dbms_output.put_line('not executed ' || st || ';');
	else
        	begin
			execute immediate(st);
			exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
			end;
	        dbms_output.put_line('executed');
	end if;
end;
/



select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
whenever sqlerror EXIT SQL.SQLCODE

