--------------------------------------------------------------------------------
-- AUTHOR:  Pierre-Marie Petit
-- VALIDATOR: 
-- SCRIPT:    
-- SYNOPSIS:  
-- USAGE:     
-- PARAMETERS:
-- 
-- 
-- 
-- 
-- 

--  Actions performed 
--   1.
--   2.

--	Version
-- 	1.0  Creation	20/02/2016
--------------------------------------------------------------------------------


whenever sqlerror CONTINUE
set verify off
set serveroutput on
set lines 512


create or replace synonym masterdatas.lien_tiers for md0000stcom.lien_tiers; 

begin
execute immediate 'drop table masterdatas.lien_tiers_tmp'; 
exception
when others then 
  dbms_output.put_line('Exception drop table masterdatas.lien_tiers_tmp SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
commit;
exit

