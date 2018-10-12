#!/bin/bash
################################################################################
# SCRIPT:     create_mv.sh
# SYNOPSIS:   create a new materialized view on a replication server
# USAGE:      create_mv.sh -m SCHEMA -v MVIEW [-p SYS_PASSWORD -d TNS_ENTRY]
# USE CASE:   this script can be use to create a new mview on all the intermediate/slave nodes of a master
# PARAMETERS:
#  mandatory:
#   -m SCHEMA: the schema owning the mview
#   -v MVIEW: the mview to create
#  rundeck mandatory:
#   -p SYS_PASSWORD: sys password of the database
#   -d TNS_ENTRY: database to connect to
# INFOS:
#  The script add a new mview to a replication schema.
#  It tries to perform a fast refresh if available.
#  It creates a mview log only if the mview has been fast refreshed and there's mlogs for other mviews in the schema
#
#  Actions performed:
#   1. Create mview ZONE_DEVISE in MD0000.
#   2. Perform a refresh.
#   3. Compute stats on ZONE_DEVISE.
#   4. Add mview log to ZONE_DEVISE.
#   5. Add grants to grantees.
#   6. Create synonym in schema already having synonyms on the MD0000 mviews.
#   7. Add ZONE_DEVISE to refresh group R_MD0000.
#
#  Example with parameters to create JM_CATALOG mview in MD000
#   ./create_mv.sh -m MD0000 -v JM_CATALOG
#
#  If an error occures during a step the script quits and display the command to
#  execute to resume at that step once the error has been fixed.
################################################################################


function die {
    # display an error message, the command to restart at the error step, then dies
    error "$1"
    echo ""

    if [ "$CUR_STEP" != "0" ]; then
	TXT=""
        [ ${DEBUG} -eq 0 ] && TXT="${TXT} -z"
	[ ${PAUSE} -eq 0 ] && TXT="${TXT} -y"
	[ ${BATCH_MODE} -eq 0 ] && TXT="${TXT} -b"
        echo "Fix the issue then to restart the script at the failed step use these parameters:"
        echo "./create_mv.sh -m ${SCHEMA} -v ${MVIEW} -s ${CUR_STEP} ${TXT}"
    fi

    exit 1
}

function get_parameters {
    # parse command line arguments
    # -v MVIEW
    export MVIEW=""
    # -m SCHEMA
    export SCHEMA=""
    # -s STEP
    export RESUME_STEP=1
    # -y
    export PAUSE=1
    # -z
    export DEBUG=1
    # -b
    export BATCH_MODE=1
    # -p SYS_PASSWORD
    export SYS_PASSWORD=''
    # -d TNS_ENTRY
    export TNS_ENTRY=''

    while getopts ":m:v:s:yzbp:d:" opt "$@"; do
        case $opt in
	    v)
		export MVIEW=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
		info "Use mview ${MVIEW}"
		;;
            m)
                export SCHEMA=$(echo ${OPTARG} | tr '[:lower:]' '[:upper:]')
                info "Use schema ${SCHEMA}"
                ;;
            s)
                export RESUME_STEP="${OPTARG}"
                info "Resuming at step ""$RESUME_STEP"
                ;;
	    y)
		export PAUSE=0
		info "Pausing on"
		;;
            z)
                export DEBUG=0
                info "Debug on"
                ;;
	    b)
		export BATCH_MODE=0
		info "Batch mode"
		;;
            p)
                export SYS_PASSWORD="${OPTARG}"
                info "Use sys password xxxxxxxx"
                ;;
            d)
                export TNS_ENTRY="${OPTARG}"
                info "Use tns entry ${TNS_ENTRY}"
                ;;
            \?)
                warning "Invalid option: -""$OPTARG"
                ;;
            :)
                warning "Option -""$OPTARG"" requires an argument"
                ;;
        esac
    done
}

# Main
export CUR_STEP=0

# load lib
. $(pwd)/lib_dup_ora.sh || die "Can't load lib lib_dup_ora.sh"

get_parameters "$@"
shift $IGNORE_PARAMS

# check that the vars are set
if( ! check_env_vars "SCHEMA" "MVIEW"); then
    usage "$0"
    die "Missing mandatory params"
fi

if( ! check_env_vars "ORACLE_HOME" "ORACLE_SID"); then
    usage "$0"
    die "Oracle env not set (ORACLE_HOME and ORACLE_SID)"
fi

export REFRESH_GROUP="R_${SCHEMA}"

# display what we're going to do
info "Actions to perform:"
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Create mview ${MVIEW} in ${SCHEMA}"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Perform a refresh"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Compute stats on ${MVIEW}"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add mview log to ${MVIEW}"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add grants to grantees"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Create synonym in schema already having synonyms on the ${SCHEMA} mviews"
fi
let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add ${MVIEW} to refresh group ${REFRESH_GROUP}"
fi
echo ""

# wait for user confirmation if launched from a TTY
if [ ${BATCH_MODE} -eq 1 ]; then
    if(tty -s); then
	press_any_key
    fi
fi

export CUR_STEP=0

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Create mview ${MVIEW} in ${SCHEMA}"

    MASTER_DB_LINK=$(get_master_db_link "${SCHEMA}") || die "Can't get master db link:
${MASTER_DB_LINK}"

    QUERY="select * from ${MVIEW}${MASTER_DB_LINK}"

    DATA_TBS=$(get_tbs_name "${SCHEMA}" "SNAP_DATA") || die "Can't get tbs DATA name"
    INDEX_TBS=$(get_tbs_name "${SCHEMA}" "SNAP_INDEX") || die "Can't get tbs INDEX name"

    typeset SQL=$(add_sql_header "grant create table to ${SCHEMA} ;
declare
    uid number;
    l_result integer;
    myint integer;
begin
    select user_id into uid from dba_users where username = '${SCHEMA}';
    myint := dbms_sys_sql.open_cursor();
    dbms_sys_sql.parse_as_user(c => myint,
                               statement => 'CREATE MATERIALIZED VIEW ${SCHEMA}.${MVIEW}
TABLESPACE ${DATA_TBS} BUILD IMMEDIATE
USING INDEX TABLESPACE ${INDEX_TBS} refresh force on demand as
${QUERY}',
                               language_flag => dbms_sql.native,
                               userid => uid);
    l_result := dbms_sys_sql.execute(myint);
    dbms_sys_sql.close_cursor(myint);
end ;
/
")

    info "Mview ddl:
${SQL}"
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't create mview ${MVIEW}"
    info "mview created"

    info "OK::mview created"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Perform a refresh"

    # use '?' as parameter, it will try to do a fast refresh and fallback on a complete one
    typeset SQL=$(add_sql_header "exec dbms_mview.refresh('${SCHEMA}.${MVIEW}', '?');")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't refresh mview ${MVIEW}"

    info "OK::mview refreshed"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Compute stats on ${MVIEW}"

    typeset SQL=$(add_sql_header "exec dbms_stats.gather_table_stats(ownname => '${SCHEMA}', tabname => '${MVIEW}');")
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't compute stats on mview"

    info "OK::stats computed"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add mview log to ${MVIEW}"

    # check if the mview has been fast refreshed
    if(is_mview_fast_refreshed "${SCHEMA}" "${MVIEW}"); then
	# check if other mviews in the schema have an mlog
	if(are_mlogs_in_schema "${SCHEMA}"); then
	    typeset DATA_TBS=$(get_tbs_name "${SCHEMA}" "SLOG_DATA") || die "Can't get tbs SLOG_DATA name"

	    typeset SQL=$(add_sql_header "create materialized view log on ${SCHEMA}.${MVIEW} tablespace ${DATA_TBS};")
	    info "Mlog ddl:
${SQL}"
	    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't add mview log"

	    info "OK::mview log created"
	else
	    info "OK::no other mviews have an mlog for ${SCHEMA}, no mlog created"
	fi
    else
	info "OK::the mview is not fast refreshed, no mview log created"
    fi

    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add grants to grantees"

    GRANTEES=$(get_grantees "${SCHEMA}") || die "Can't get grantees:
${GRANTEES}"

    for USER in ${GRANTEES}; do
	typeset SQL=$(add_sql_header "select 'grant select on ${SCHEMA}.${MVIEW} to ${USER};' from dual;
select 'grant select on ${SCHEMA}.'||log_table||' to ${USER};' from dba_mview_logs where master = '${MVIEW}' and log_owner = '${SCHEMA}';")
	typeset GRANTS=$(sqlplus_sysdba_check_errors "${SQL}") || die "Can't generate grants: ${GRANTS}"
	info "Grants: 
${GRANTS}"
	GRANTS=$(add_sql_header "${GRANTS}")
	sqlplus_sysdba_check_errors "${GRANTS}" ${DEBUG} || die "Can't grant select on mview"
    done

    info "OK::Grant select granted"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Create synonym in schema already having synonyms on the ${SCHEMA} mviews"

    SYN_USERS=$(get_syn_users "${SCHEMA}") || die "Can't get synonyms users:
${SYN_USERS}"

    for USER in ${SYN_USERS}; do
	typeset SQL=$(add_sql_header "create or replace synonym ${USER}.${MVIEW} for ${SCHEMA}.${MVIEW};")
	info "Synonyms:
${SQL}"
	sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't create synonym"
    done

    info "OK::synonym created"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

let CUR_STEP=${CUR_STEP}+1
if(check_step ${CUR_STEP} ${RESUME_STEP}); then
    info "${CUR_STEP}. Add ${MVIEW} to refresh group ${REFRESH_GROUP}"

    typeset SQL=$(add_sql_header "exec dbms_refresh.add(name => '${SCHEMA}.${REFRESH_GROUP}', list => '${SCHEMA}.${MVIEW}');
commit;")
    info "query:
${SQL}"
    sqlplus_sysdba_check_errors "${SQL}" ${DEBUG} || die "Can't update refresh group"

    info "OK::refresh group updated"
    echo ""
    [ ${PAUSE} -eq 0 ] && press_any_key
fi

info "Successfully ending"
