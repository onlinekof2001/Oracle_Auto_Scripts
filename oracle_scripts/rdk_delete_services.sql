set serveroutput on
set long 10000

define username=&1
define dryrun=&2

set serveroutput on

column db_name heading "db_name" new_value db_name;
select lower(name) as db_name from v$database;

declare
st clob;
dryrun varchar(10):='&dryrun.';
new_service varchar2(255);
CURSOR C_service(par_name varchar2) IS SELECT * FROM dba_services where name like par_name;
R_service C_service%rowtype;
servicename varchar2(255);

begin
  
    servicename:='%svc_' || lower('&username.') || '%';    
    open C_service(servicename);
    loop 
	FETCH C_service INTO R_service;
	EXIT WHEN C_service%NOTFOUND;
        if (dryrun='true') then
	        dbms_output.put_line('not executed ' || 'dbms_service.stop_service( ' || R_service.name || ' );');
	        dbms_output.put_line('not executed ' || 'dbms_service.delete_service( ' || R_service.name || ' );');
        else
		begin
 			 dbms_service.stop_service( R_service.name );
                exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
                end;
		begin     
			dbms_service.delete_service( R_service.name );
                exception when others then DBMS_OUTPUT.PUT_LINE('Handled Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
		end;

        end if;
       		
    end loop;
    close C_service;
end;
/

exit

