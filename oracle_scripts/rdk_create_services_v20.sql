set serveroutput on
set long 10000

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;

define service_name=&1
var new_service varchar2(255);
exec :new_service:='&1'

prompt create &1
exec dbms_service.create_service(:new_service,:new_service);
prompt start &1
exec dbms_service.start_service(:new_service);

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................

exit