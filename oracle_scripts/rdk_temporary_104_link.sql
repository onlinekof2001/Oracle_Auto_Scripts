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

define numtiers=&1
define num_platform_target=&2
define num_platform_source=&3
alter user masterdatas quota unlimited on md0000STCOM_snap_data;
alter user masterdatas quota unlimited on md0000STCOM_snap_index;
begin
execute immediate 'create table masterdatas.lien_tiers_tmp tablespace md0000STCOM_snap_data as select * from masterdatas.lien_tiers'; 
exception
when others then 
  dbms_output.put_line('Exception create table masterdatas.lien_tiers_tmp SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/
begin
execute immediate 'create index masterdatas.IDX01_LIEN_TIERS_tmp on masterdatas.LIEN_TIERS_tmp (TTI_NUM_TYPE_TIERS_FILS,TIR_NUM_TIERS_FILS,TIR_SOUS_NUM_TIERS_FILS) tablespace md0000STCOM_snap_index'; 
exception
when others then 
  dbms_output.put_line('Exception idx creation SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/


update masterdatas.lien_tiers_tmp lt
      set lt.tir_sous_num_tiers_pere = &num_platform_target.
where lt.tyl_type_lien_tiers_tyl = 104
and lt.tti_num_type_tiers_pere = 46 and lt.tir_num_tiers_pere = 3 and lt.tir_sous_num_tiers_pere = &num_platform_source.
and lt.tir_sous_num_tiers_fils in (
   select tr.tir_sous_num_tiers
   from masterdatas.tiers_ref tr
   where tr.tti_num_type_tiers_tti = 7
   and tr.tir_num_tiers = &numtiers.
   and tr.tir_sous_num_tiers = &numtiers.
  );
 
grant select on masterdatas.lien_tiers_tmp to public;
 
create or replace synonym masterdatas.lien_tiers for masterdatas.lien_tiers_tmp; 

exec dbms_stats.gather_table_stats('MASTERDATAS','LIEN_TIERS_TMP');

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
commit;
exit

