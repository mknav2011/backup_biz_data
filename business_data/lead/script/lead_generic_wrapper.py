# Call python generic_wrapper.py --args '{"--APP_ID": "lstp.edw.bd.listings_latest"}'
from move_dl_common_api.glue_util import GlueUtil
from move_dl_common_api.athena_util import AthenaUtil
import boto3, sys, json, time, uuid, datetime, botocore, re
import traceback
from datetime import datetime
from datetime import timedelta
from argparse import ArgumentParser
from move_dl_common_api.move_utils import LastGoodKey, MoveConfig
from move_dl_common_api import application_locking
from move_dl_common_api.schema_generation import SchemaGeneration
from move_dl_common_api.substitution_util import SubstitutionUtil

import sys, logging, signal

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=FORMAT)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)


locked_app_id_list = []
run_id = str(uuid.uuid1())


def signal_handler(signal, frame):
    for locked_app_id in locked_app_id_list:
        application_locking.unlock(app_id = locked_app_id, run_id=run_id)
        log("Unlocked application {} SIGINT".format(locked_app_id))
    sys.exit(-1)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def log_time(f):
    def wrap(*args, **kwargs):
        time1 = time.time()
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            # called as @decorator
            ret = f(*args)
        else:
            # called as @decorator(*args, **kwargs)
            ret = f(*args, **kwargs)
        time2 = time.time()
        log('function %s took %0.1f seconds' % (f.func_name, (time2 - time1)))
        return ret

    return wrap


def log(msg, heading=False):
    if heading == False:
        logging.info("{}: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))
    else:
        logging.info("***************************************************************************************")
        logging.info("*****\t{}".format(msg))
        logging.info("***************************************************************************************")


@log_time
def check_app_locks(app_id, runid, lock_period_second):
    try:
        application_locking.lock(app_id=app_id, run_id=runid, seconds=lock_period_second)
        return True
    except Exception as e:
        log(traceback.format_exc())
        return False


@log_time
def get_env():
    substitutionUtil = SubstitutionUtil()
    myenv = substitutionUtil.get_env()
    return myenv.env


env = get_env()


def process_glue_job(glueutil, var_args, script_path, glue_dpu_instance_count):
    logging.info(var_args)

    job_name = var_args['--JOB_NAME']
    #Glue job initiation
    glueutil.execute_job(glue_job_name=job_name,
                         glue_script_location=script_path,
                         glue_dpu_instance_count=glue_dpu_instance_count,
                         script_args=var_args)





def convert_partitions_to_array(partitions):
    """ converts partition with integer year month day and year to string with padded zeros useful for constructing s3 path suffix """
    result = []
    for partition in partitions:
        year = partition[LastGoodKey.TAG_PARTITION][LastGoodKey.TAG_PARTITION_YEAR]
        year = str(year).zfill(4)
        month = partition[LastGoodKey.TAG_PARTITION][LastGoodKey.TAG_PARTITION_MONTH]
        month = str(month).zfill(2)
        day = partition[LastGoodKey.TAG_PARTITION][LastGoodKey.TAG_PARTITION_DAY]
        day = str(day).zfill(2)
        # hour is optional
        if LastGoodKey.TAG_PARTITION_HOUR in partition[LastGoodKey.TAG_PARTITION]:
            hour = partition[LastGoodKey.TAG_PARTITION][LastGoodKey.TAG_PARTITION_HOUR]
            hour = str(hour).zfill(2)
            result.append((year,month, day, hour))
        else:
            result.append((year,month,day))
    return result

def convert_list_to_lgk_partitions(app_id, partition_column_list, partition_value_list):
    lgk_partition_list = []
    logging.info("cassidy partition_column_list: {}".format(partition_column_list))
    logging.info("cassidy partition_value_list: {}".format(partition_value_list))
    for partition_value in partition_value_list:
        lgk_partition = {}
        lgk_partition[LastGoodKey.TAG_ID] = app_id
        i = 0
        lgk_partition[LastGoodKey.TAG_PARTITION_META]={}
        lgk_partition[LastGoodKey.TAG_PARTITION]={}
        for partition_column in partition_column_list:
            if partition_column in (LastGoodKey.TAG_PARTITION_YEAR, LastGoodKey.TAG_PARTITION_MONTH,
                                    LastGoodKey.TAG_PARTITION_DAY, LastGoodKey.TAG_PARTITION_HOUR):
                value = int(partition_value[i])
            else:
                value = partition_value[i]
            lgk_partition[LastGoodKey.TAG_PARTITION][partition_column]=value
            i += 1
        lgk_partition_list.append(lgk_partition)
    logging.info("cassidy lgk_partition_list: {}".format(lgk_partition_list))
    return lgk_partition_list


@log_time
def main():
    parser = ArgumentParser(description="Provide app_id")
    parser.add_argument('--app_id', help='If there is no separation between source and target app id, we will use this to provide the app id.')
    parser.add_argument('--source_app_id', help='source app_id. If multiple source. This will be pdt main app id for driving lgk.')
    parser.add_argument('--target_app_id', help='target app_id. This will be bd app id.' )
    input_arguments = parser.parse_args()
    if input_arguments.source_app_id:
        source_app_id = input_arguments.source_app_id
    if input_arguments.target_app_id:
        target_app_id = input_arguments.target_app_id
    if input_arguments.app_id:
        app_id = input_arguments.app_id
    else:
        app_id = target_app_id

    # app_id = 'cust.cdh.bd.customer_mlsset'
    log("Starting load for bd app_id %s" % (app_id))
    start_runtime = datetime.utcnow()
    # read app config
    try:
        app_config = json.loads(MoveConfig.get_application_config(application_id=app_id, key='configuration'))
        logging.info(app_config)
        # initialize glue variables
        job_name = app_config["job_name"]
        script_path = app_config["script_path"].replace('$env', env)
        sql_path_list = app_config["sql_path"]
        sql_path_list = [sql_path.replace('$env', env) for sql_path in sql_path_list]

        s3_target_path = app_config["s3_target_path"].replace('$env', env)
        if "source_partition_by_column" in app_config:
            source_partition_by_column_list = app_config["source_partition_by_column"]
        if "target_partition_by_column" in app_config:
            target_partition_by_column_list = app_config["target_partition_by_column"]
        is_snapshot = app_config["is_snapshot"]
        if "is_hive_dynamic_partition" in app_config:
            is_hive_dynamic_partition = app_config["is_hive_dynamic_partition"]
        else:
            is_hive_dynamic_partition = 'n'
        glue_dpu_instance_count = app_config["glue_dpu_instance_count"]
        data_frame_sources = app_config["source"]
        if "block_size_MB" in app_config:
            block_size_MB = app_config["block_size_MB"]
        else:
            block_size_MB = None
        if "partition_batch_size" in app_config:
            partition_batch_size = app_config["partition_batch_size"]
        else:
            partition_batch_size = 1
        if "is_full_refresh" in app_config:
            is_full_refresh = app_config["is_full_refresh"]
        else:
            is_full_refresh = "n"
        if "lock_period_hours" in app_config:
            lock_period_hours = app_config["lock_period_hours"]
        else:
            lock_period_hours = 4

        '''
        for source in app_config["source"]:
            i = {"s3_source_path": source["s3_source_path"],
                 "source_file_type_parquet_or_text": source["source_file_type_parquet_or_text"],
                 "temp_table_name": source["temp_table_name"]}
            data_frame_sources.append(i)
        '''

    except TypeError as e:
        logging.error ("Application cannot be fetched {} due to error {}".format(app_id, e))
        pass

    lgk_dest_partition = [{LastGoodKey.TAG_ID: app_id,
                           LastGoodKey.TAG_PARTITION: {},
                           LastGoodKey.TAG_PARTITION_META: {}}]
    meta_lgk_put = None
    lgk_to_be_process_source_partition = []

    # lock the source and target app id
    application_locking.lock(target_app_id, run_id, lock_period_hours*60*60)
    locked_app_id_list.append(target_app_id)
    # check if source app ids are locked --TO-DO change this to confirm PDT completion
    if "source" in app_config and type(app_config["source"]) is list:
        for source in app_config["source"]:
            lock = check_app_locks(source["app_id"], run_id, lock_period_hours*60*60)
            if lock == False:
                log("Source App_id %s still running" % (source))
                for locked_app_id in locked_app_id_list:
                    application_locking.unlock(locked_app_id, run_id)
                sys.exit("Source app_id locked")
            locked_app_id_list.append(source["app_id"])
            # add the source partition to lgk if it is not the main source_app_id
            if source["app_id"] <> source_app_id:
                lgk_to_be_process_source_partition.append({LastGoodKey.TAG_ID: source["app_id"],
                                     LastGoodKey.TAG_PARTITION: {},
                                     LastGoodKey.TAG_PARTITION_META: {}})

    latest_value = LastGoodKey.get_recent_entries(id=app_id, key=app_id, limit=1)
    log("LGK last entry: %s" % (str(latest_value)))

    # if source partition and target partition is not the same and need to be synch
    # we will use the hive dynamic parititon in spark to avoid untouch partition data overwrite

    # prepare SQL query variables
    if is_snapshot == 'y':
        if latest_value == []:
            last_update_date = '2018-02-07 00:00:00'
            formated_lud = datetime.strptime(last_update_date, '%Y-%m-%d %H:%M:%S')
            yesterday_mst = datetime.strptime(
                ((datetime.utcnow() - timedelta(hours=7)).date() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                '%Y-%m-%d %H:%M:%S')
            if formated_lud >= yesterday_mst:
                max_pull_date = yesterday_mst + timedelta(days=1)
                snapshot_date_param = yesterday_mst.strftime("%Y-%m-%d")
                pull_date_param = max_pull_date.strftime("%Y-%m-%d")
            else:
                max_pull_date = formated_lud + timedelta(days=1)
                snapshot_date_param = formated_lud.strftime("%Y-%m-%d")
                pull_date_param = max_pull_date.strftime("%Y-%m-%d")

        else:
            last_update_date = latest_value[0]["value"]["meta_info"]["last_update_date"]
            formated_lud = datetime.strptime(last_update_date, '%Y-%m-%d %H:%M:%S')
            yesterday_mst = datetime.strptime(
                ((datetime.utcnow() - timedelta(hours=7)).date() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                '%Y-%m-%d %H:%M:%S')
            if formated_lud >= yesterday_mst:
                max_pull_date = (yesterday_mst + timedelta(days=1))
                snapshot_date_param = yesterday_mst.strftime("%Y-%m-%d")
                pull_date_param = max_pull_date.strftime("%Y-%m-%d")
            else:
                max_pull_date = formated_lud + timedelta(days=1)
                snapshot_date_param = formated_lud.strftime("%Y-%m-%d")
                pull_date_param = max_pull_date.strftime("%Y-%m-%d")

        log("Snapshot Date: %s" % (str(last_update_date)))
        log("Pull Date: %s" % (str(max_pull_date)))
        sql_param_variables = [{"SNAPSHOT_DATE_PARAM": snapshot_date_param}, {"PULL_DATE_PARAM": pull_date_param}]
        final_s3_target_path = s3_target_path + target_partition_by_column_list[0] + '=' + snapshot_date_param
        meta_lgk_put = str(max_pull_date)
        year = formated_lud.strftime('%Y')
        month = formated_lud.strftime('%m')
        day = formated_lud.strftime('%d')
        hour = formated_lud.strftime('%H')
        # LGK put entry
        lgk_dest_partition = [{LastGoodKey.TAG_ID: app_id,
                               LastGoodKey.TAG_PARTITION: {LastGoodKey.TAG_PARTITION_YEAR: int(year),
                                                           LastGoodKey.TAG_PARTITION_MONTH: int(month),
                                                           LastGoodKey.TAG_PARTITION_DAY: int(day),
                                                           LastGoodKey.TAG_PARTITION_HOUR: int(hour)},
                               LastGoodKey.TAG_PARTITION_META: {}}]
        log("Dest partition in LGK put: %s" % (str(lgk_dest_partition)))
    else:
        final_s3_target_path = s3_target_path


    if is_hive_dynamic_partition == 'y':
        # source partition is year=yyyy/month=mm/day=dd
        # destination partition is mst_submit_date=yyyymmdd
        lgk_unprocessed_source_partitions = LastGoodKey.get_unprocessed_paritions(source_ids=[source_app_id], destination_ids=[target_app_id])
        # lgk_unprocessed_source_partitions = [{
        #     "id": "leads.edw.pdt.lead_transactional_fact",
        #     "partition": {
        #       "day": 9,
        #       "month": 1,
        #       "year": 2018
        #     },
        #     "partition_meta": {}
        # }]
        source_partitions = convert_partitions_to_array(lgk_unprocessed_source_partitions)
        to_be_process_source_partitions = []
        destination_partitions = []
        sql_param_variables_list = []
        where_partition_clause = ''
        j = 0
        if is_full_refresh == 'n':
            for source_partition in source_partitions:
                destination_partition=[]
                if j>=partition_batch_size:
                    break
                # destination partition is a date of yyyymmdd
                partition_date = ""
                #sql_param_variables = []
                i = 0
                for source_partition_item in source_partition:
                    if j == 0 and i == 0:
                        where_partition_clause += 'where%20('
                    elif i == 0:
                        where_partition_clause += ')%20or%20('
                    else:
                        where_partition_clause += '%20and%20'
                    partition_date += source_partition_item
                    where_partition_clause += '{}=%27{}%27'.format(source_partition_by_column_list[i], source_partition_item)
                    i += 1
                    # sql_param_variables.append({"<PARTITION{}>".format(i), source_partition_item})
                destination_partition.append(partition_date)
                destination_partitions.append(tuple(destination_partition))
                to_be_process_source_partitions.append(source_partition)
                # sql_param_variables_list.append(sql_param_variables)
                j += 1
            if where_partition_clause.startswith("where%20("):
                where_partition_clause += ')'
            sql_param_variables_list.append([{"<where_partition_clause>": where_partition_clause}])
            lgk_dest_partition = convert_list_to_lgk_partitions(target_app_id, target_partition_by_column_list, destination_partitions)
            lgk_to_be_process_source_partition.extend(convert_list_to_lgk_partitions(source_app_id, source_partition_by_column_list, to_be_process_source_partitions))

            if is_full_refresh == "n" and where_partition_clause=="":
                logging.info("There is no partition specified and this is not a full refresh")
                for locked_app_id in locked_app_id_list:
                    application_locking.unlock(locked_app_id, run_id)
                return
        elif is_full_refresh == "y":
            sql_param_variables_list.append([{"<where_partition_clause>": where_partition_clause}])
        else:
            logging.info("Not sure about whether it is full refresh or not. Do not start the glue process")
            for locked_app_id in locked_app_id_list:
                    application_locking.unlock(locked_app_id, run_id)
            return
    else:
        lgk_to_be_process_source_partition.append({LastGoodKey.TAG_ID: source_app_id,
                                     LastGoodKey.TAG_PARTITION: {},
                                     LastGoodKey.TAG_PARTITION_META: {}})


    glueutil = GlueUtil()

    var_args = {"--JOB_NAME": job_name,
                "--s3_target_path": final_s3_target_path,
                "--source_sql_file_path": json.dumps(sql_path_list, separators=(',', ':'), indent=None),
                # "--inputvariables": "[{\"DATE_FROM_PARAM\":\"2018-02-08\"}]",
                # "--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-cust-prod/cdh/processed-data-xact/cdhdb_move_orig_sys_references_v\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"cdhdb_move_orig_sys_references_v\"}]",
                "--data_frame_sources": json.dumps(data_frame_sources, separators=(',', ':'), indent=None)}

    if block_size_MB:
        var_args["--block_size_MB"] = block_size_MB
    else:
        var_args["--block_size_MB"] = 'default'

    start_runtime = datetime.utcnow()

    if is_snapshot == 'y':
        sql_param_variables_list = list(sql_param_variables)

    if sql_param_variables_list is not None and len(sql_param_variables_list) > 0:
        # allow to run multiple glue jobs based on the sql param variable list
        # Currently only allow one as we allow running multiple partitions at one time
        for sql_param_variables in sql_param_variables_list:
            var_args["--sql_param_variables"] = json.dumps(sql_param_variables, separators=(',', ':'), indent=None)
            logging.info(var_args)
            process_glue_job(glueutil, var_args, script_path, glue_dpu_instance_count)
    else:
        sql_param_variables = []
        var_args["--sql_param_variables"] = json.dumps(sql_param_variables, separators=(',', ':'), indent=None)
        process_glue_job(glueutil, var_args, script_path, glue_dpu_instance_count)

    end_runtime = datetime.utcnow()

    #LGK Put
    result = LastGoodKey.put_entry(id=app_id,
                                   key=LastGoodKey.KEY,
                                   start_runtime=start_runtime,
                                   end_runtime=end_runtime,
                                   source_partitions=lgk_to_be_process_source_partition,
                                   destination_partitions=lgk_dest_partition,
                                   meta_info={"last_update_date": meta_lgk_put})

    logging.info("Cassidy LGK put result: {}".format(result))
    log("LGK put result: %s" % (result))

    for locked_app_id in locked_app_id_list:
        application_locking.unlock(locked_app_id, run_id)

    log("Process Complete!!!")


if __name__ == "__main__":
    main()