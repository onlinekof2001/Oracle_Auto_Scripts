--------------------------------------------------------------------------------
-- AUTHOR:  Pascal BEN 
-- VALIDATOR:  Pierre Marie
-- SCRIPT:     rdk_clone_user.sql
-- SYNOPSIS:   duplicate a user in the same instance with is grant.
-- USAGE:      sqlplus -S -L /nolog @rdk_clone_user.sql user_source user_target dryrun
-- PARAMETERS:
-- user_source= source username  (it must be exist)
-- user_target= new user (if already exist, copy the grant from usersource to user_target
-- dryrun= 



--  Actions
--   1. test if source_user exist, if not exist  I exit from the script with error
--   2. it the target user does not  exist I create it and initialize is password to decathlon01.
--        the new user will have  the same profile, default tablespace ... than the source_user.
--   3. either user exist or not, I copy , role, system privile, object privilege


--	Version
-- 	1.0  Creation	14/11/2016
--------------------------------------------------------------------------------



set feedback off
select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
column horodateur heading "horodateur"  new_value horodateur;


set serveroutput on
set lines 1024
set verify off 


declare
    h number;
    ddl_handle number;
    modify_handle number;
    cmd clob;
    cmd2 clob;
    dmsf       PLS_INTEGER;
    i  pls_integer;
    user_target_exist pls_integer;

    dryrun varchar(10);
    user_source varchar(15);
    user_target varchar(15);

begin
     SYS.DBMS_OUTPUT.ENABLE(1000000);
    -- I initialize the metadata
    dmsf := dbms_metadata.session_transform;
    -- at dkt the user are always in uppercase.
    user_source:=upper('&1'); 
    user_target:=upper('&2');
    dryrun:=lower('&3');
    
    --  if the user_source doesn't exist it is an error.
    begin
      select username into user_source from dba_users where username=upper(user_source);
      exception
       WHEN NO_DATA_FOUND THEN
        raise_application_error (-20001,user_source||' does not exists on erreur.');
    end;
    
    select count(1) into user_target_exist from dba_users where username=upper(user_target);
    
    if user_target_exist = 0 
    then -- if user_target already exist , I don't create it :D  
        -- I copy the USER_SOURCE to USER CIBLE
        dbms_output.put_line ('--create user '|| user_target);
        h := DBMS_METADATA.OPEN('USER');
        modify_handle := DBMS_METADATA.ADD_TRANSFORM(h,'MODIFY');
        DBMS_METADATA.SET_REMAP_PARAM(modify_handle,'REMAP_SCHEMA',user_source,user_target);
        ddl_handle := DBMS_METADATA.ADD_TRANSFORM(h,'DDL');
        DBMS_METADATA.SET_FILTER(h, 'NAME',user_source);
        DBMS_METADATA.set_count(h,1);
        i:=0;
        LOOP
            i:=i+1;
            cmd := DBMS_METADATA.FETCH_CLOB(h);
            EXIT WHEN cmd IS NULL;    -- When there are no more objects to be retrieved, FETCH_CLOB returns NULL.

            if (dryrun='true') then
                dbms_output.put_line('--not executed ');
				dbms_output.put_line( cmd || ';');
            else
                begin
                    execute immediate(cmd);
                end;
                dbms_output.put_line('--executed ');
				dbms_output.put_line( cmd || ';');
            end if;
        END LOOP;
        DBMS_METADATA.close(h);




  			cmd:='alter user '||user_target||' identified by "Decathlon01"';
        if (dryrun='true') then
            dbms_output.put_line('--not executed ');
			dbms_output.put_line( cmd || ';');
        else
            dbms_output.put_line('--executed ');
			dbms_output.put_line( cmd || ';');
            execute immediate(cmd);

        end if;

    end if;
    
    
    -- I copy ROLE GRANT privilege
    dbms_output.put_line ('--role_grant');
    h := DBMS_METADATA.OPEN('ROLE_GRANT');
    modify_handle := DBMS_METADATA.ADD_TRANSFORM(h,'MODIFY');
    DBMS_METADATA.SET_REMAP_PARAM(modify_handle,'REMAP_SCHEMA',user_source,user_target);
    ddl_handle := DBMS_METADATA.ADD_TRANSFORM(h,'DDL');
    DBMS_METADATA.SET_FILTER(h, 'GRANTEE',user_source);
    DBMS_METADATA.set_count(h,1);
        i:=0;
    LOOP
        i:=i+1;
        cmd := DBMS_METADATA.FETCH_CLOB(h);
        EXIT WHEN cmd IS NULL;    -- When there are no more objects to be retrieved, FETCH_CLOB returns NULL.
        if (dryrun='true') then
            dbms_output.put_line('--not executed ');
			dbms_output.put_line( cmd || ';');
        else
            execute immediate(cmd);
            dbms_output.put_line('--executed ');
			dbms_output.put_line( cmd || ';');
        end if;
    END LOOP;
    DBMS_METADATA.close(h);


    -- I copy SYTTEM GRANT privilege
    dbms_output.put_line ('--system_grant');
    h := DBMS_METADATA.OPEN('SYSTEM_GRANT');
    modify_handle := DBMS_METADATA.ADD_TRANSFORM(h,'MODIFY');
    DBMS_METADATA.SET_REMAP_PARAM(modify_handle,'REMAP_SCHEMA',user_source,user_target);
    ddl_handle := DBMS_METADATA.ADD_TRANSFORM(h,'DDL');
    DBMS_METADATA.SET_FILTER(h, 'GRANTEE',user_source);
    DBMS_METADATA.set_count(h,1);
    i:=0;
    LOOP
        i:=i+1;
        cmd := DBMS_METADATA.FETCH_CLOB(h);
        EXIT WHEN cmd IS NULL;    -- When there are no more objects to be retrieved, FETCH_CLOB returns NULL.
        if (dryrun='true') then
            dbms_output.put_line('--not executed ');
			dbms_output.put_line( cmd || ';');
        else
            execute immediate(cmd);
            dbms_output.put_line('--executed ');
			dbms_output.put_line( cmd || ';');
        end if;
    END LOOP;
    DBMS_METADATA.close(h);


    -- I copy OBJECT GRANT privilege
    dbms_output.put_line ('--object _grant');
    h := DBMS_METADATA.OPEN('OBJECT_GRANT');
    modify_handle := DBMS_METADATA.ADD_TRANSFORM(h,'MODIFY');
    DBMS_METADATA.SET_REMAP_PARAM(modify_handle,'REMAP_SCHEMA',user_source,user_target);
    ddl_handle := DBMS_METADATA.ADD_TRANSFORM(h,'DDL');
    DBMS_METADATA.SET_FILTER(h, 'GRANTEE',user_source);
    DBMS_METADATA.set_count(h,1);
    i:=0;
    LOOP
        i:=i+1;
        cmd := DBMS_METADATA.FETCH_CLOB(h);
        EXIT WHEN cmd IS NULL;    -- When there are no more objects to be retrieved, FETCH_CLOB returns NULL.
        if cmd not like '%"SYS"."LOW_GROUP"%'  -- bug oracle 
        then 
            if (dryrun='true') then
                dbms_output.put_line('--not executed ');
				dbms_output.put_line( cmd || ';');
            else
                execute immediate(cmd);
                dbms_output.put_line('--executed ');
			dbms_output.put_line( cmd || ';');
            end if;
        end if ;
    END LOOP;
    DBMS_METADATA.close(h);

end ;
/

select to_char(sysdate,'YYYY_MM_DD_HH24_MI_SS') as horodateur from dual;
prompt Finished.....................
whenever sqlerror EXIT SQL.SQLCODE

