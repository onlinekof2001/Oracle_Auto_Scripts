alter system set db_recovery_file_dest_size = 307200M;
alter system set processes=600 scope=spfile;
alter system set archive_lag_target=1800;
ALTER PROFILE default LIMIT password_grace_time UNLIMITED ;
ALTER PROFILE default LIMIT password_life_time UNLIMITED ;
ALTER PROFILE LIMIT LIMIT password_grace_time UNLIMITED ;
ALTER PROFILE LIMIT LIMIT password_life_time UNLIMITED ;
ALTER PROFILE OXYAPP LIMIT password_grace_time UNLIMITED ;
ALTER PROFILE OXYAPP LIMIT password_life_time UNLIMITED ;
alter system set db_flashback_retention_target=240;
alter system reset memory_max_target;
alter system reset memory_target;
alter system set sga_max_size=25G scope=spfile;
alter system set sga_target=25G scope=spfile;
alter system set pga_aggregate_target=1G scope=spfile;
alter system set sec_case_sensitive_logon=false scope=both;
alter user SUNOPSIS identified by values 'D913CD8C7E426FE8';
exec dbms_sqltune.set_auto_tuning_task_parameter( 'ACCEPT_SQL_PROFILES', 'TRUE');


CREATE PROFILE "P_APPLICATIF" LIMIT CPU_PER_SESSION DEFAULT
CPU_PER_CALL DEFAULT
CONNECT_TIME DEFAULT
IDLE_TIME DEFAULT
SESSIONS_PER_USER DEFAULT
LOGICAL_READS_PER_SESSION DEFAULT
LOGICAL_READS_PER_CALL DEFAULT
PRIVATE_SGA DEFAULT
COMPOSITE_LIMIT DEFAULT
PASSWORD_LIFE_TIME UNLIMITED
PASSWORD_GRACE_TIME UNLIMITED
PASSWORD_REUSE_MAX UNLIMITED
PASSWORD_REUSE_TIME UNLIMITED
PASSWORD_LOCK_TIME DEFAULT
FAILED_LOGIN_ATTEMPTS 10
PASSWORD_VERIFY_FUNCTION DEFAULT;


CREATE PROFILE "P_SUPPORT" LIMIT CPU_PER_SESSION DEFAULT
CPU_PER_CALL DEFAULT
CONNECT_TIME DEFAULT
IDLE_TIME DEFAULT
SESSIONS_PER_USER DEFAULT
LOGICAL_READS_PER_SESSION DEFAULT
LOGICAL_READS_PER_CALL DEFAULT
PRIVATE_SGA DEFAULT
COMPOSITE_LIMIT DEFAULT
PASSWORD_LIFE_TIME 90
PASSWORD_GRACE_TIME 10
PASSWORD_REUSE_MAX DEFAULT
PASSWORD_REUSE_TIME DEFAULT
PASSWORD_LOCK_TIME DEFAULT
FAILED_LOGIN_ATTEMPTS 5
PASSWORD_VERIFY_FUNCTION DEFAULT;

set serveroutput on
declare
st clob;
instance varchar2(30);
begin	
select INSTANCE_NAME into instance from v$instance;
st:='CREATE SMALLFILE TABLESPACE "USER_DKT" DATAFILE ''/u01/app/oracle/oradata/data1/'||lower(instance)||'/user_dkt.data1'' SIZE 100M AUTOEXTEND ON NEXT 256M MAXSIZE 1G LOGGING EXTENT MANAGEMENT LOCAL SEGMENT SPACE MANAGEMENT AUTO';
dbms_output.put_line(st);
execute immediate st;
exception
when others then
dbms_output.put_line('Exception SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
end;
/

-----Create resource manager
exec dbms_resource_manager.clear_pending_area();
exec dbms_resource_manager.create_pending_area();
exec dbms_resource_manager.create_consumer_group(consumer_group => 'LOW_GROUP', COMMENT => 'Low', mgmt_mth => 'ROUND-ROBIN');
exec dbms_resource_manager.create_consumer_group(consumer_group => 'MEDIUM_GROUP', COMMENT => 'Medium', mgmt_mth => 'ROUND-ROBIN');
exec dbms_resource_manager.create_consumer_group(consumer_group => 'HIGH_GROUP', COMMENT => 'High', mgmt_mth => 'ROUND-ROBIN');
EXEC dbms_resource_manager.create_consumer_group(consumer_group => 'OTHER_GROUPS', COMMENT => 'Other', mgmt_mth => 'ROUND-ROBIN');

EXEC dbms_resource_manager.create_plan( 'DAY_PLAN_STORESV2','Retail daily resource plan manager');
EXEC dbms_resource_manager.create_plan_directive( plan => 'DAY_PLAN_STORESV2',group_or_subplan => 'HIGH_GROUP', COMMENT => '', mgmt_p1 => NULL, mgmt_p2 => 60 , mgmt_p3 => NULL, mgmt_p4 => NULL, mgmt_p5 => NULL, mgmt_p6 => NULL, mgmt_p7 => NULL, mgmt_p8 => NULL ,switch_group=>'MEDIUM_GROUP',switch_time_in_call=>3600,parallel_degree_limit_p1=>1);
EXEC dbms_resource_manager.create_plan_directive( plan => 'DAY_PLAN_STORESV2',group_or_subplan => 'MEDIUM_GROUP', COMMENT => '', mgmt_p1 => NULL, mgmt_p2 => 30, mgmt_p3 => NULL, mgmt_p4 => NULL, mgmt_p5 => NULL, mgmt_p6 => NULL, mgmt_p7 =>  NULL, mgmt_p8 => NULL ,switch_group=>'LOW_GROUP',switch_time_in_call=>3600,parallel_degree_limit_p1=>1);
EXEC dbms_resource_manager.create_plan_directive( plan => 'DAY_PLAN_STORESV2',group_or_subplan => 'LOW_GROUP', COMMENT => '', mgmt_p1 => NULL, mgmt_p2 => 10,  mgmt_p3 => NULL, mgmt_p4 => NULL, mgmt_p5 => NULL, mgmt_p6 => NULL, mgmt_p7 =>  NULL, mgmt_p8 => NULL ,parallel_degree_limit_p1=>1);
EXEC dbms_resource_manager.create_plan_directive( plan => 'DAY_PLAN_STORESV2',group_or_subplan => 'OTHER_GROUPS', COMMENT => '', mgmt_p1 => 20, mgmt_p2 =>  NULL, mgmt_p3 => NULL, mgmt_p4 => NULL, mgmt_p5 => NULL, mgmt_p6 => NULL, mgmt_p7  => NULL, mgmt_p8 => NULL,parallel_degree_limit_p1=>1 );
EXEC dbms_resource_manager.create_plan_directive( plan => 'DAY_PLAN_STORESV2',group_or_subplan => 'SYS_GROUP', COMMENT => '', mgmt_p1 => 80, mgmt_p2 => NULL , mgmt_p3 => NULL, mgmt_p4 => NULL, mgmt_p5 => NULL, mgmt_p6 => NULL, mgmt_p7 => NULL, mgmt_p8 => NULL,parallel_degree_limit_p1=>1 );

begin
dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_MASTERDATAS_B',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_MASTERDATAS_F',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_MASTERDATAS_I',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_MASTERDATAS_W',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_NBO_B',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_NBO_F',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_NBO_I',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_NBO_W',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_STCOM_B',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_STCOM_F',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_STCOM_I',
    'HIGH_GROUP'

);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_STCOM_W',
    'HIGH_GROUP'
);

dbms_resource_manager.set_consumer_group_mapping(
    dbms_resource_manager.service_name,
    'TETRIX02_SVC_STCOM_M',
    'LOW_GROUP'
);
end;
/



EXEC dbms_resource_manager.submit_pending_area();

EXEC DBMS_RESOURCE_MANAGER_PRIVS.GRANT_SWITCH_CONSUMER_GROUP ('public','low_group',false);
EXEC DBMS_RESOURCE_MANAGER_PRIVS.GRANT_SWITCH_CONSUMER_GROUP ('public','medium_group',false);
EXEC DBMS_RESOURCE_MANAGER_PRIVS.GRANT_SWITCH_CONSUMER_GROUP ('public','high_group',false);

alter system set RESOURCE_MANAGER_PLAN = 'DAY_PLAN_STORESV2';


