#!/bin/bash
################################################################################
# SCRIPT:     rdk_refresh_group_mview.sh
# SYNOPSIS:   check and refresh if necessary all need server
# USAGE:      rdk_refresh_group_mview.sh -t TNSNAMES -s SCHEMA -u USER -p PASSWORD -c CHECK -d DRYRUN
# PARAMETERS:
#  mandatory:
#   -t TnsEntry   
#   -s SCHEMA : the schema owner of the mview
#   -u USER : dba users 
#   -p PASSWORD: dba password
#   -c sql to check
#   -T TABLE
#   -d dryrun (true = rollback,false =commit)
#   -m mode : 1 = find master need refresh / 2 = refresh_where_is_need 
#  optional:
# INFOS:
#
#
#  Actions performed 
#  1 - FIND BEGIN LEVEL FOR REFRESH 
#   checklevel=-1
#   level=1
#   database[1]=TnsEntry # level 0 = client, level 1= maitre, level 2= maitre du maitre...
#   while checklevel=-1;do #while dont find level to refresh
#     Check sql on database[level]
#     if check OK, then checklevel=level-1 -- it's OK at this level, we need te start on upper level
#     else 
#      find master 
#      if master n'exite pas : EXIT ERROR , Can't validate SQL on last knwon master
#      level=level+1
#      database[[level]=master
#     end
#   done
#
#  2 - BEGIN REFRESH AT THE LOWER LEVEL NEED
#   refresh while begin at level : checklevel
#
#
################################################################################

function usage {
    # display usage
    grep -E '^# USAGE:' "$1"
}

function timestamp {
    # display the current time
    date +'%b %d %H:%M:%S'
}

function info {
    # display on stdout
    echo "$(timestamp) INFO: ${*}."
}

function warning {
    # display on stdout
    echo "$(timestamp) WARNING: ${*}."
}

function error {
    # display on stderr
    echo "$(timestamp) ERROR: ${*}." >&2
}

function check_env_vars {
    # check if given parameters are environment variables
    typeset RET=0
    while [ $# -ne 0 ]; do
        if [ -z "`eval echo \"$\"\"$1\"`" ]; then
            error "$1"" variable not set"
            RET=1
        fi
        shift
    done

    return $RET
}

function die {
    # display an error message, the command to restart at the error step then dies
    error "$1"
    echo "" >&2

    if [ "$CUR_STEP" != "0" ]; then
        echo "Fix the issue then to restart the script at the failed step use these parameters:" >&2
        echo "./rdk_refresh_group_mview.sh -t ${TNSENTRY} -d ${SCHEMA} -u ${USER} -p xxx -c "${CHECKSQL}" -d ${DRYRUN}" >&2
    fi

    exit 1
}

function press_any_key {
    # wait for the user to press Enter
    info "Press Enter to continue or Ctrl-C to cancel..."
    read
}

function get_parameters {
    # parse command line arguments
    # set the IGNORE_PARAMS which count the number of params handled
    # by getopts and to skip in the caller
    # -t TNSENTRY
    export TNSENTRY=""
    # -s SCHEMA
    export SCHEMA=""
    # -u USER
    export USER=""
    # -p PWD
    export PWD=""
    # -c
    export CHECKSQL=""
    # -d
    export DRYRUN="true"
    # -m
    export MODE=2
    export TABLE=""

    while getopts ":t:s:u:p:c:d:T:m:" opt "$@"; do
        case $opt in
            t)
                export TNSENTRY=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
                info "Use TNSENTRY ${TNSENTRY}"
                ;;
            s)
                export SCHEMA=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
                info "Use SCHEMA ${SCHEMA}"
                ;;
            u)
                export USER="$OPTARG"
                info "Use USER ${USER}"
                ;;
            p)
                export PWD="$OPTARG"
                info "Use schema password xxxxx"
                ;;
            c)
                export CHECKSQL="$OPTARG"
                info "CHECK SQL done "${CHECKSQL}" "
                ;;
            T)
                export TABLE="$OPTARG"
                info "TABLE to refresh "${TABLE}" "
                ;;
            m)
                export MODE="$OPTARG"
                info "Mode of execution"${TABLE}" "
                ;;

            d)
                export DRYRUN="$OPTARG"
                info "Use Dryrun ${DRYRUN}"
                ;;
            \?)
                warning "Invalid option: -""$OPTARG"
                ;;
            :)
                warning "Option -""$OPTARG"" requires an argument"
                ;;
        esac
    done

    export IGNORE_PARAMS=$(($OPTIND -1))
}


function sqlplus_user {
    # execute a request on the instance connected as a user, do not check the output for errors
    # ORACLE_SID must be set
    typeset TNS="$1"
    typeset SQL="$2"

    SQL_FILE=/tmp/sqlplus_user.sql
    if [ -f "${SQL}" ]; then
        cp "${SQL}" "${SQL_FILE}"
    else
        echo "$SQL" > "${SQL_FILE}"
    fi
    echo ";" >> "${SQL_FILE}"
    echo "exit;" >> "${SQL_FILE}"

    #sqlplus -S -L "${USER}/${PWD}"@${TNS} "@${SQL_FILE}"
    /usr/bin/sqlplus64 -S -L "${USER}/${PWD}"@${TNS} "@${SQL_FILE}"
    typeset RC=$?

#    rm -f "${SQL_FILE}"

    return ${RC}
}

function sqlplus_user_check_errors {
    # execute a request on the instance connected as a user, check the output for ORA-
    typeset TNS="$1"
    typeset SQL="$2"

    typeset RC=0
    typeset LOG_FILE=/tmp/sqlplus_user_check_errors.log

     sqlplus_user "$TNS" "${SQL}" 2>&1 | tee "${LOG_FILE}"

    # check sqlplus_sysdba exit code with bash specific PIPESTATUS variable
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        error "SQL*Plus exit code != 0  ${PIPESTATUS[0]} "
        RC=1
    fi

    grep -q 'ORA-12170' "${LOG_FILE}"
    if [ $? -eq 0 ]; then
        error "'ORA-12170' detected while executing sql request, it's a Connect timeout, perhaps replication master is on production host"
        RC=1
    else 
	grep -q 'ORA-' "${LOG_FILE}"
	if [ $? -eq 0 ]; then
        	error "'ORA-' detected while executing sql request"
        	RC=1
    	fi
    fi

#    rm -f "$LOG_FILE"

    return $RC
}

function return_sql {
    typeset TNS="$1"
    typeset SQL="$2"

    #echo "return sql $TNS $SQL"

    RESULT=$(sqlplus_user_check_errors "${TNS}" "${SQL}")
    RC=$?

    echo "${RESULT}"

    return ${RC}
}

function noreturn_sql {
    typeset TNS="$1"
    typeset SQL="$2"

    #echo "return sql $TNS $SQL"

    RESULT=$(sqlplus_user "${TNS}" "${SQL}")

    echo "${RESULT}"

    return 0
}

function add_sql_header {
    # set params to sql request to format its output
    typeset SQL="$1"

    echo 'set head off feedback off pagesize 0
set trimspool off linesize 512 echo off'"
$SQL"
}


function find_master {
    typeset TNS="$1"
    
    typeset SQL=$(add_sql_header "select count(*) from dba_db_links where OWNER='${SCHEMA}'")
    count=$(return_sql ${TNS} "${SQL}") || die "Can't get count from db_link on ${TNS}"
    if (( $count == 0 ));then
        # On est sur le master
        RESULT='MASTER'
        RC=0
    else
        SQL=$(add_sql_header "select host from dba_db_links where OWNER='${SCHEMA}' and rownum=1")
        host=$(return_sql ${TNS} "${SQL}") || die "Can't get host from db_link on ${TNS}"
    
        SQL=$(add_sql_header "drop database link find_master")
        create=$(return_sql "${TNS}" "${SQL}")

        SQL=$(add_sql_header "create database link find_master connect to ${USER} identified by ${PWD} using '${host}'")
        create=$(return_sql "${TNS}" "${SQL}") || die "Can't create db_link find_master on ${TNS}" 
 
        SQL=$(add_sql_header "select db_link from dba_db_links where OWNER=upper('${USER}') and DB_LINK like 'FIND_MASTER%'")
        dblink=$(return_sql "${TNS}" "${SQL}") || die "Can't get name db_link on ${TNS}"

        SQL=$(add_sql_header "select instance_name || '_' || substr(host_name,0,INSTR(host_name,'.')-1) from v\$instance@${dblink}")
        RESULT=$(return_sql "${TNS}" "${SQL}") || die "Can't get tnsnames from find_master on ${TNS}"
        RC=$?

        SQL=$(add_sql_header "drop database link find master;"
        return_sql "${TNS}" "${SQL}") || die "Can't drop database link on ${TNS}"
   fi
   echo "${RESULT}"
   return ${RC}
   
}

function refresh {
	WAITREFRESH=600
	FREQUENCE=10

   	RESULT='OK'
   	RC=0 
   
	typeset TNS="$1"
    	typeset SQL=$(add_sql_header "select case when s.synonym_name is null then o.object_name else s.table_name  end as mview_name
from dba_objects o
left join dba_synonyms s on o.object_name=s.synonym_name and o.owner=s.OWNER
where o.object_type in ('SYNONYM','TABLE') 
and o.object_name='${TABLE}' 
and o.owner='${SCHEMA}'")   
    TABLETOREFRESH=$(return_sql ${TNS} "${SQL}") || die "Can't find table to refresh on ${TNS}" 

     SQL=$(add_sql_header "select case when s.synonym_name is null then o.owner       else s.table_owner end as mview_owner
from dba_objects o
left join dba_synonyms s on o.object_name=s.synonym_name and o.owner=s.OWNER
where o.object_type in ('SYNONYM','TABLE') 
and o.object_name='${TABLE}' 
and o.owner='${SCHEMA}'")   
    OWNERTOREFRESH=$(return_sql ${TNS} "${SQL}") || die "Can't find owner to refresh on ${TNS}" 

    SQL=$(add_sql_header "select count(*) from dba_snapshots s join dba_refresh r on s.refresh_group = r.refgroup 
where s.name='$TABLETOREFRESH' and s.owner='$OWNERTOREFRESH'")
    count=$(return_sql ${TNS} "${SQL}") || die "Can't find refresh group ${TNS}" 
    if (( $count == 0 ));then
        # On a pas de group
	# Check si la vue n'est pas déjà en cours
        SQL=$(add_sql_header "select count(*) from V\$MVREFRESH v where v.currmvname='${TABLETOREFRESH}' and v.currmvowner='${OWNERTOREFRESH}'")
        count=$(return_sql ${TNS} "${SQL}") || die "Can't find if view is currently used ${TNS}"
        if (( $count == 0 ));then # pas en cours
 	       SQL=$(add_sql_header "exec dbms_mview.refresh('${OWNERTOREFRESH}.${TABLETOREFRESH}')" )
               count=$(return_sql ${TNS} "${SQL}") || die "Can't refresh mview ${OWNERTOREFRESH}.${TABLETOREFRESH}"
        else
		wait=WAITREFRESH
                RESULT='KO'
               	while (( wait > 0 ));do
			sleep FREQUENCE
			SQL=$(add_sql_header "select count(*) from V\$MVREFRESH v where v.currmvname='${TABLETOREFRESH}' and v.currmvowner='${OWNERTOREFRESH}'")
        		count=$(return_sql ${TNS} "${SQL}") || die "Can't find if view is currently used ${TNS}"
                        if (( $count > 0 ));then # Le refresh est fini dans les temps
				wait=0
                                RESULT='OK'
			fi
			wait=$((wait-FREQUENCE))			
		done
		if [ $RESULT eq 'KO' ];then
			die "Time out exceed "
		fi
	fi
    else
	#On a un group
	SQL=$(add_sql_header "select rowner from dba_snapshots s join dba_refresh r on s.refresh_group = r.refgroup 
where s.name='$TABLETOREFRESH' and s.owner='$OWNERTOREFRESH'" )
        OWNERTOREFRESH=$(return_sql ${TNS} "${SQL}") || die "Can't find owner group ${OWNERTOREFRESH}.${TABLETOREFRESH}"

	SQL=$(add_sql_header "select rname from dba_snapshots s join dba_refresh r on s.refresh_group = r.refgroup 
where s.name='$TABLETOREFRESH' and s.owner='$OWNERTOREFRESH'" )
        GROUPTOREFRESH=$(return_sql ${TNS} "${SQL}") || die "Can't find group name ${OWNERTOREFRESH}.${TABLETOREFRESH}"

        # Check si le group n'est pas déjà en cours
        SQL=$(add_sql_header "select count(*) from V\$MVREFRESH v join dba_snapshots s on s.name=v.currmvname and s.owner=v.currmvowner join dba_refresh r on s.refresh_group = r.refgroup where rname='${GROUPTOREFRESH}' and rowner='${OWNERTOREFRESH}'")
        count=$(return_sql ${TNS} "${SQL}") || die "Can't find if group is currently used ${TNS}"
        if (( $count == 0 ));then # pas en cours
	        SQL=$(add_sql_header "exec dbms_refresh.refresh('${OWNERTOREFRESH}.${GROUPTOREFRESH}')" )
        	count=$(return_sql ${TNS} "${SQL}") || die "Can't refresh group ${OWNERTOREFRESH}.${GROUPTOREFRESH}"        
	else
		wait=WAITREFRESH
                RESULT='KO'
                while (( wait > 0 ));do
                        sleep FREQUENCE
                        SQL=$(add_sql_header "select count(*) from V\$MVREFRESH v join dba_snapshots s on s.name=v.currmvname and s.owner=v.currmvowner join dba_refresh r on s.refresh_group = r.refgroup where rname='${GROUPTOREFRESH}' and rowner='${OWNERTOREFRESH}'" )
                        count=$(return_sql ${TNS} "${SQL}") || die "Can't find if group is currently used ${TNS}"
                        if (( $count > 0 ));then # Le refresh est fini dans les temps
                                wait=0
                                RESULT='OK'
                        fi
                        wait=$((wait-FREQUENCE))                        
                done
                if [ $RESULT eq 'KO' ];then
                        die "Time out exceed "
                fi
	fi
    fi 

   echo "${RESULT}"
   return ${RC}
   
}


# Main

get_parameters "$@"
shift $IGNORE_PARAMS

# check that the vars are set
if( ! check_env_vars "TNSENTRY" "SCHEMA" "USER" "PWD" "CHECKSQL" "DRYRUN"); then
    usage "$0"
    die "Missing mandatory params"
fi

if (( ${MODE} == 2 )); then
   if [ ${TABLE} = "" ];then 
    usage "$0"
    die "Missing -T Table params"
   fi
fi


# display what we're going to do
info "Actions to perform: "
if ( $DRYRUN );then
    info "Dryrun ask, nothing will be commit"
fi

#export DEBUG=0
export mktemp=test.log
export CUR_STEP=1
typeset SQL=""

export CUR_STEP=1
#if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Check where refresh need to be done and begin for ${SCHEMA} in ${TNSENTRY}"

    checklevel=-1
    level=1
    TabTnsEntry[1]=${TNSENTRY} # level 0 = client, level 1= maitre, level 2= maitre du maitre...
    
    while [[ $checklevel == -1 ]];do #while dont find level to refresh
#     
#    Check sql on database[level]
      info "Check sql on ${TabTnsEntry[$level]}"
      check=$(return_sql ${TabTnsEntry[$level]} "$(add_sql_header "$CHECKSQL" )") || die "Can't check sql ${TabTnsEntry[$level]}"
      info "Return SQL $check"         
      if [ "$check" == "OK" ] ;then 
         checklevel=$((level-1))
         info "Find a good master $checklevel"         
      else 
         info "Find on another master $level"         
         level=$((level+1))
	     info "find_master ${TabTnsEntry[$((level-1))]}"
         TabTnsEntry[$level]=$(find_master ${TabTnsEntry[$((level-1))]}) || die "Can't find master ${TabTnsEntry[$((level-1))]}"
         info "End find_master ${TabTnsEntry[$((level))]}"
         if [ "${TabTnsEntry[$((level))]}" == "MASTER" ];then
            die "Query is not valid on master, check query"
         fi
      fi
    done	
    info "checklevel $checklevel"

#fi
if (( $MODE == 2 )); then
	let CUR_STEP=${CUR_STEP}+1
	if (( $checklevel == 0 )); then
	       	info "Repli is already OK, nothing to do"
	else
		level=$((level-1))
		while (( $level > 0 ));do  
		        info "${CUR_STEP}. We need to do refresh on $level level"
			let CUR_STEP=${CUR_STEP}+1
			info "${CUR_STEP}. We will begin to refresh on ${TabTnsEntry[$((level))]}"
			Time[$level]=$(refresh ${TabTnsEntry[$((level))]}) || die "Can't refresh on  ${TabTnsEntry[$((level))]}"
                        level=$((level-1))
		done
	       	info "All Repli need must be good now "
	fi
fi
info "Successfully ending"


