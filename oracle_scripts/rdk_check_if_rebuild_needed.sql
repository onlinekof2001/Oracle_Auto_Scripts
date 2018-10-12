define pool=&1
define numtiers=&2
set serveroutput on
set verify off
set head on
set feedback off
set pages 0

prompt checking if recreation is needed
set verify off
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',FALSE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',FALSE);
exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',FALSE);
define ddl_table='BLOCAGE_SUIVI'

set serveroutput on size 50000
declare
ddl_slave clob;
already_exists exception; 
begin
	SELECT dbms_metadata.get_ddl('MATERIALIZED_VIEW',''||MVIEW_NAME||'',''||OWNER||'') into ddl_slave FROM dba_mviews where mview_name = upper('&ddl_table.') and owner like '%&pool.%' and rownum=1;
	--dbms_output.put_line('search for store '||ltrim('&numtiers.','0')||' in '||ddl_slave);
	if (instr(ddl_slave,'='||ltrim('&numtiers.','0')||' ')>0) or (instr(ddl_slave,'='||ltrim('&numtiers.','0')||';')>0) then
		raise already_exists;
	end if;
exception
   when already_exists then 
    RAISE_APPLICATION_ERROR(-20000, 'Masterdatas already filtered for the store &numtiers.');
   when no_data_found then null;
end;
/



