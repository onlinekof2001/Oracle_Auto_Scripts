set serveroutput on;

declare 
    schema varchar2(20);
    moni_schema varchar2(20);
    moni_password varchar2(20);
    masterdatas varchar2(20);
    add_schema varchar(4000);
    cmd varchar2(4000);
    nb integer;
    dryrun boolean;
    current_log_level varchar2(20);
    text_executed varchar2(100);
    nb_err integer;
    
    -----------------------------------------------------------------
    -----------------------------------------------------------------
    -----------------------------------------------------------------
    procedure write_log (log_level in  varchar2, text in  varchar2)     
    is
    begin 
        if upper(current_log_level) = 'INFO' and upper(log_level) = 'DEBUG' then 
               null;
           else
              dbms_output.put_line (text);
        end if;
    end;

    -----------------------------------------------------------------    
    -----------------------------------------------------------------
    -----------------------------------------------------------------
    procedure execute_dryrun (cmd in  varchar2)     
    is
    begin 
        if dryrun then
           write_log('debug', 'dryrun true , the command is not executed :' ||cmd); 
        else
          execute immediate (cmd);
          write_log('debug', 'dryrun false, command  executed :' ||cmd);
        end if;
    end;    
    
begin

    schema :=upper('&1');
    moni_password :='&2';
    add_schema:=upper('&3');
    dryrun:=&4;
    current_log_level:=upper('&5');

    if current_log_level <>'INFO' then current_log_level:='DEBUG' ; end if;

    if dryrun then
         text_executed :='dryrun, the command is not executed : ';
         write_log ('info','dryrun configured, the schema will not be created');
         write_log ('info','dryrun configured, the schema will not be created');
         write_log ('info','dryrun configured, the schema will not be created');
      else
         text_executed :='the command executed is : ';
         write_log ('info','dryrun is not configured, the schema will be created');
    end if;


    
    
    ----------------------------------    
    -- the checks     
    ----------------------------------
    nb_err:=0;

    write_log ('info','the checks'||current_log_level);
    write_log ('debug','debug');

    begin
    ------------------------------
    -- check the user information
        write_log ('info','1 - check if the user ' || schema || ' exist');
        select count(1) into nb from dba_users where username=schema;
        if nb = 1 then
               write_log ('debug','OK   the user ' || schema || ' exist');
          else
               write_log ('info',' NOK  the user ' || schema || ' don''t exist');
               nb_err:=nb_err+1;
        end if;
        
        --- check if role exist
        select count(1) into nb from dba_roles where role ='SELECT_'||schema;
        if nb = 1 then
               write_log ('debug','OK   the role  SELECT_' || schema || ' exist');
          else
               write_log ('info',' NOK  the role  SELECT_' || schema || ' don''t exist');
               nb_err:=nb_err+1;
        end if;
    ----------------------------    
    -- check the password     
        write_log ('info','2 - check password ' );
        if length(moni_password)>8 and moni_password is not null then
               write_log ('debug','OK   the password seems good');
          else
               write_log ('info',' NOK  the password is null or length<10');
               nb_err:=nb_err+1;
        end if;



    ------------------------------
    -- check additionnal schema  : 
   if add_schema ='NONE' then
       write_log('info','no additional schema');
     else
        write_log ('info','3 - additionnal schema exist and select_role exist' );
        for c in (select regexp_substr(add_schema,'[^,]+', 1, level) schema from dual
           connect by regexp_substr(add_schema, '[^,]+', 1, level) is not null)
        loop
            --- check if user exist
            select count(1) into nb from dba_users where username=c.schema; 
            if nb = 1 then
                   write_log ('debug','OK   the additionnal schema ' || c.schema || ' exist');
              else
                   write_log ('info',' NOK  the additionnal schema ' || c.schema || ' don''t exist');
                   nb_err:=nb_err+1;
            end if;
            
            --- check if role exist
            select count(1) into nb from dba_roles where role ='SELECT_'||c.schema;
            if nb = 1 then
                   write_log ('debug','OK   the role  SELECT_' || c.schema || ' exist');
              else
                   write_log ('info',' NOK  the role  SELECT_' || c.schema || ' don''t exist');
                   nb_err:=nb_err+1;
            end if;
         end loop;
    
         if nb_err > 0 then 
            RAISE_APPLICATION_ERROR  (-20101, 'An error has been encountered, SEE PREVIOUS MESSAGE');
         end if;
       end if;
    end;


    ----------------------------------
    -- the creation
    ----------------------------------    
    select upper('moni_' || username)  into moni_schema from dba_users where username=schema;
    write_log ('info','-----------------------------------------------------------------');
    write_log ('info','-- the schema to create is :' ||moni_schema);
    write_log ('info','-----------------------------------------------------------------');
    select count(1) into nb from dba_users where username =upper(moni_schema);
    if nb=0 then
       cmd:='create user '||moni_schema ||' identified by "'||moni_password || '" account lock';
       execute_dryrun(cmd);   
      else
       write_log ('info','the user '||moni_schema ||' already exist');
    end if;

    
    cmd:='alter user '||moni_schema ||' identified by "'||moni_password || '" default tablespace dbtools account unlock';
    execute_dryrun(cmd);   


    cmd :='grant select_'||schema ||' TO ' || moni_schema;
    execute_dryrun(cmd);   

   -- create synonym in moni_schema for schema.xxx
   write_log('info','-- create synonym ' || schema ||'.xxxxx for ' || moni_schema);    
   for c in (SELECT 'create or replace synonym ' ||moni_schema ||'.' || table_name || ' for ' || schema || '.' || table_name cmd FROM dba_tables where owner=schema)  
   loop
       cmd:=c.cmd;
       execute_dryrun(cmd);   
   end loop;     

   -- duplicate synonym from scheam to moni_schema
   write_log('info','-- Duplicate synonym from ' ||schema || ' for ' ||moni_schema);
   for c in (SELECT 'create synonym ' ||moni_schema ||'.' || synonym_name || ' for ' || table_owner || '.' || table_name cmd, synonym_name from dba_synonyms where owner=schema)  
   loop
        select count(1) into nb from dba_synonyms where owner=moni_schema and synonym_name=c.synonym_name;
	if nb = 0 then
           cmd:=c.cmd;
           execute_dryrun(cmd);
	  else
           write_log('debug','The synonym '|| moni_schema ||'.'||c.synonym_name ||' already exist, it is not replaced.');
        end if;
  end loop;     
 
  if add_schema ='NONE' then
       write_log('info','no additional schema');
     else
      -- additionnal schema  : grant select
      write_log('info','-- Create synonym and grant on moni_schema for add_schema:'||add_schema);
      for c_add_schema in (select regexp_substr(add_schema,'[^,]+', 1, level) schema from dual
          connect by regexp_substr(add_schema, '[^,]+', 1, level) is not null)
       loop
           cmd := 'grant select_'||c_add_schema.schema ||' to ' ||moni_schema;
           execute_dryrun(cmd);         
           -- create synonym for additionnal schema
           write_log('info','---- create synonym ' || c_add_schema.schema ||'.xxxxx for ' || moni_schema);   
           for c_syn in (SELECT 'create synonym ' ||moni_schema ||'.' || table_name || ' for ' || owner || '.' || table_name cmd , table_name synonym_name
                         FROM dba_tables where owner=c_add_schema.schema
                         union
                         SELECT 'create synonym ' ||moni_schema ||'.' || synonym_name || ' for ' || table_owner || '.' || table_name cmd ,
                                synonym_name 
                         FROM dba_synonyms where owner=c_add_schema.schema
                        )     
           loop
               select count(1) into nb from dba_synonyms where owner=moni_schema and synonym_name=c_syn.synonym_name;
               if nb = 0 then
                   cmd:=c_syn.cmd;
                   execute_dryrun(cmd);      
                  else
                   write_log('debug','The synonym '|| moni_schema ||'.'||c_syn.synonym_name ||' already exist, it is not replaced');
               end if;
           end loop; 
      end loop;
   end if;
 
  DECLARE 
     host_name varchar2(500);
     instance_name varchar2(100);
     service_name varchar2(500);
     domain varchar2(500);
  begin
     select instance_name into instance_name  from v$instance;

     -- creatuib des service monistore
     service_name:=lower(instance_name||'_svc_' ||schema ||'_m');
     select count(1) into nb from v$services where lower(name) = service_name;

     write_log('info','-- Create and start service :'||service_name);
 
     if nb =0 then
        cmd :='begin DBMS_SERVICE.CREATE_SERVICE('''||service_name|| ''',''' ||service_name ||'''); end;';
        execute_dryrun(cmd);   
       else
        write_log('info','the service '||service_name|| 'already exist, it is not recreated');
     end if;
     
     select count(1) into nb from v$parameter where lower(name) like '%service_name%' and lower(value) like '%' ||service_name||'%';
     if nb = 0 then
        cmd:='begin DBMS_SERVICE.START_SERVICE('''||service_name||'''); end;';
        execute_dryrun(cmd);   
        write_log('info','service ' || service_name ||' started');
       else
        write_log('info','service ' || service_name || ' already started.');
     end if;
     
     cmd:='alter system register';
     execute_dryrun(cmd);   
      
   end;
end;
/
   