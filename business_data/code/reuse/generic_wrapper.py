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
        print("{}: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))
    else:
        print("***************************************************************************************")
        print("*****\t{}".format(msg))
        print("***************************************************************************************")


@log_time
def check_app_locks(app_id, runid):
    try:
        application_locking.lock(app_id=app_id, run_id=runid, seconds=3)
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


@log_time
def main():
    parser = ArgumentParser(description="Provide BD app_id")
    parser.add_argument('--args', help='BD app_id')
    input_arguments = parser.parse_args()
    var_args = input_arguments.args
    var_args = json.loads(var_args)
    app_id = var_args['--APP_ID']

    # app_id = 'cust.cdh.bd.customer_mlsset'
    log("Starting load for bd app_id %s" % (app_id))
    start_runtime = datetime.utcnow()
    run_instance = str(uuid.uuid1())
    # read app config
    try:
        app_config = json.loads(MoveConfig.get_application_config(application_id=app_id, key='configuration'))
        print app_config
        # initialize glue variables
        jobname = app_config["job_name"]
        script_path = app_config["script_path"].replace('$env', env)
        sql_path = app_config["sql_path"].replace('$env', env)
        s3_target_path = app_config["s3_target_path"].replace('$env', env)
        target_partition_by_column = app_config["target_partition_by_column"]
        is_snapshot = app_config["is_snapshot"]
        glue_dpu_instance_count = app_config["glue_dpu_instance_count"]
        data_frame_sources = app_config["source"]
        block_size_MB= app_config["block_size_MB"]
        '''
        for source in app_config["source"]:
            i = {"s3_source_path": source["s3_source_path"],
                 "source_file_type_parquet_or_text": source["source_file_type_parquet_or_text"],
                 "temp_table_name": source["temp_table_name"]}
            data_frame_sources.append(i)
        '''

    except TypeError as e:
        print ("Application cannot be fetched {} due to error {}".format(app_id, e))
        pass

    # check if source app ids are locked --TO-DO change this to confirm PDT completion
    if "source" in app_config and type(app_config["source"]) is list:
        for source in app_config["source"]:
            lock = check_app_locks(source["app_id"], run_instance)
            if lock == False:
                log("Source App_id %s still running" % (source))
                sys.exit("Source app_id locked")

    latest_value = LastGoodKey.get_recent_entries(id=app_id, key=app_id, limit=1)
    log("LGK last entry: %s" % (str(latest_value)))

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
        inputvariables = [{"SNAPSHOT_DATE_PARAM": snapshot_date_param}, {"PULL_DATE_PARAM": pull_date_param}]
        final_s3_target_path = s3_target_path + target_partition_by_column + '=' + snapshot_date_param
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
        inputvariables = []
        final_s3_target_path = s3_target_path
        meta_lgk_put = None
        lgk_dest_partition = None

    glueutil = GlueUtil()

    var_args = {"--JOB_NAME": jobname,
                "--s3_target_path": final_s3_target_path,
                "--source_sql_file_path": sql_path,
                # "--inputvariables": "[{\"DATE_FROM_PARAM\":\"2018-02-08\"}]",
                "--inputvariables": json.dumps(inputvariables).replace(' ', ''),
                # "--data_frame_sources": "[{\"s3_source_path\":\"s3://move-dataeng-cust-prod/cdh/processed-data-xact/cdhdb_move_orig_sys_references_v\",\"source_file_type_parquet_or_text\":\"parquet\",\"temp_table_name\":\"cdhdb_move_orig_sys_references_v\"}]",
                "--data_frame_sources": json.dumps(data_frame_sources).replace(' ', ''),
                "--target_partition_by_column": target_partition_by_column,
                "--block_size_MB":block_size_MB}
    print var_args

    #Glue job initiation
    glueutil.execute_job(glue_job_name=jobname,
                         glue_script_location=script_path,
                         glue_dpu_instance_count=glue_dpu_instance_count,
                         script_args=var_args)

    end_runtime = datetime.utcnow()

    #LGK Put
    result = LastGoodKey.put_entry(id=app_id,
                                   key=app_id,
                                   start_runtime=start_runtime,
                                   end_runtime=end_runtime,
                                   source_partitions=None,
                                   destination_partitions=lgk_dest_partition,
                                   meta_info={"last_update_date": meta_lgk_put})

    print result
    log("LGK put result: %s" % (result))
    log("Process Complete!!!")


if __name__ == "__main__":
    main()