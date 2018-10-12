set line 1024
set pages 10000
set verify off

alter session set nls_timestamp_format='DD/MM/YY HH24:MI:SS';                  
alter session set nls_date_format='DD/MM/YY HH24:MI:SS';                  
break on heure skip 1
select t2.*, '@sqlinfo ' ||t2.sql_id                                                                    
from                                                                           
(                                                                              
      select  t.*,                                                           
       sum( nb)  over ( partition by heure) nbparHeure,                                   
       round(nb/ sum( nb)  over ( partition by heure)*100) pct_Activity,       
       RANK () over (partition by heure order by nb desc) rank                 
      from                                                                     
      (                                                                        
          select trunc(sample_time, 'hh')+(trunc((trunc(sample_time, 'mi') -trunc(sample_time,'hh'))*24*60/5,0)/24/60*5) heure,
          sql_id                                                              
       , count(*) nb                                                          
          from v$active_session_history                                    
          where sql_id is not null                                             
          and sample_time  between sysdate-&1/24 and sysdate
          group by trunc(sample_time, 'hh')+(trunc((trunc(sample_time, 'mi') -trunc(sample_time,'hh'))*24*60/5,0)/24/60*5), sql_id                            
          order by trunc(sample_time, 'hh')+(trunc((trunc(sample_time, 'mi') -trunc(sample_time,'hh'))*24*60/5,0)/24/60*5), count(1) desc                     
      ) t                                                                      
      order by heure, rank                                                     
) t2                                                                           
where rank<=5                                                                 
order  by heure  , rank                                                          
;
undef 1 2 3 4 5

