#!/usr/bin/env python

# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#     http://www.apache.org/licenses/LICENSE-2.0
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""
Run Tarus test cases and generate the Junit XML report
"""
# pylint: disable=redefined-builtin

import os
import sys
import time
import logging
import socket
import argparse
import glob
import pathlib
import subprocess
import yaml
import requests
import psutil
from subprocess import PIPE, STDOUT
import csv
from tqdm import tqdm
import pandas as pd
import boto3
import configuration
from junitparser import TestCase, TestSuite, JUnitXml, Skipped, Error, Failure


logger = logging.getLogger(__name__)
code = 0


class Timer(object):
    def __init__(self, description):
        self.description = description

    def __enter__(self):
        self.start = int(time.time())
        return self

    def __exit__(self, type, value, traceback):
        logger.info(f"{self.description}: {self.diff()}s")

    def diff(self):
        return int(time.time()) - self.start


def run_process(cmd, wait=True):
    print("running command : {}".format(cmd))

    if wait:
        os.environ["PYTHONUNBUFFERED"] = "1"
        p = subprocess.Popen(cmd, stdout=PIPE, stderr=STDOUT,
                              shell=True)
        lines =[]
        while True:
            line = p.stdout.readline().decode('utf-8').rstrip()
            if not line: break
            lines.append(line)
            print(line)

        return p.returncode, '\n'.join(lines)


    else:
        p = subprocess.Popen(cmd, shell=True)
        return p.returncode, ''


def get_test_yamls(dir_path=None, pattern="*.yaml"):
    if not dir_path:
        path = pathlib.Path(__file__).parent.absolute()
        dir_path = str(path) + "/tests"

    path_pattern = "{}/{}".format(dir_path, pattern)
    return glob.glob(path_pattern)


def get_options(artifacts_dir, jmeter_path=None):
    options=[]
    if jmeter_path:
        options.append('-o modules.jmeter.path={}'.format(jmeter_path))
    options.append('-o settings.artifacts-dir={}'.format(artifacts_dir))
    options.append('-o modules.console.disable=true')
    options.append('-o settings.env.BASEDIR={}'.format(artifacts_dir))
    options_str = ' '.join(options)

    return options_str


def get_folder_names(dir1):
    return [di for di in os.listdir(dir1) if os.path.isdir(os.path.join(dir1, di))]

def get_latest(names, env_id):
    max_ts = 0
    latest_run = ''
    for run_name in names:
        run_name_list = run_name.split('_')
        if env_id == run_name_list[0]:
            if int(run_name_list[2]) > max_ts:
                max_ts = int(run_name_list[2])
                latest_run = run_name

    return latest_run

def get_latest_dir(dir1, env_id):
    names= get_folder_names(dir1)
    latest_run= get_latest(names, env_id)
    return os.path.join(dir1, latest_run)


def get_env_info():
    {"platform" : psutil.os.sys.platform,
     }


def upload_to_s3(local_directory, destination, bucket="regression-reports-123"):
    run_process("aws s3 cp {} s3://{}/{}  --recursive".format(local_directory, bucket, destination))

    # client = boto3.client('s3')
    #
    # # enumerate local files recursively
    # for root, dirs, files in os.walk(local_directory):
    #     for filename in files:
    #         # construct the full local path
    #         local_path = os.path.join(root, filename)
    #
    #         # construct the full s3 path
    #         relative_path = os.path.relpath(local_path, local_directory)
    #         s3_path = os.path.join(destination, relative_path)
    #         logger.info('Searching "%s" in "%s"' % (s3_path, bucket))
    #         try:
    #             client.head_object(Bucket=bucket, Key=s3_path)
    #             logger.info("Path found on S3! Skipping %s..." % s3_path)
    #         except:
    #             logger.info("Uploading %s..x." % s3_path)
    #             client.upload_file(local_path, bucket, s3_path)
    #

def download_s3_files(env_id, tgt_path, bucket_name="regression-reports-123"):
    import boto3

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    result = bucket.meta.client.list_objects(Bucket=bucket.name,
                                             Delimiter='/')

    run_names = []
    for o in result.get('CommonPrefixes'):
        run_names.append(o.get('Prefix')[:-1])

    latest_run = get_latest(run_names, env_id)
    if not latest_run:
        logger.info("No run found for env_id {}".format(env_id))
        return False

    if not os.path.exists(tgt_path):
        os.makedirs(tgt_path)

    tgt_path = "{}/{}".format(tgt_path, latest_run)
    run_process("aws s3 cp  s3://{}/{} {} --recursive".format(bucket.name, latest_run, tgt_path))

    # for s3_object in bucket.objects.filter(Delimiter='', Prefix=latest_run):
    #     filename = s3_object.key
    #     filename= "{}/{}".format(tgt_path, filename)
    #     parent_path = pathlib.Path(filename).parent
    #     if not os.path.exists(parent_path):
    #         os.makedirs(parent_path)
    #     bucket.download_file(s3_object.key, filename)

    return tgt_path


def compare_artifacts(dir1, dir2, out_dir, diff_percent=None):
    ##compare metrics test case wise

    if not diff_percent:
        diff_percent = float(configuration.get('suite', 'diff_percent', 30))

    dir1, dir2 = dir1.strip(), dir2.strip()

    if not os.path.exists(dir1):
        logger.info("The path {} does not exit".format(dir1))
        return False

    if not os.path.exists(dir2):
        logger.info("The path {} does not exit".format(dir2))
        return False

    over_all_pass = True
    sub_dirs_1 = list([x[0].rsplit('/',1)[1] for x in os.walk(dir1) if x[0] != dir1])
    sub_dirs_2 = list([x[0].rsplit('/',1)[1] for x in os.walk(dir2) if x[0] != dir2])

    aggregates = ["mean", "max", "min"]
    header = ["test_suite", "metric", "run1", "run2", "percentage_diff", "result"]
    rows = [header]
    print(sub_dirs_1)
    print(sub_dirs_2)
    for sub_dir1 in sub_dirs_1:
        if sub_dir1 in sub_dirs_2:

            metrics_file1 = glob.glob("{}/{}/SAlogs_*".format(dir1, sub_dir1))
            metrics_file2 = glob.glob("{}/{}/SAlogs_*".format(dir2, sub_dir1))

            if not (metrics_file1 and metrics_file2):

                metrics_file1 = glob.glob("{}/{}/local_*".format(dir1, sub_dir1))
                metrics_file2 = glob.glob("{}/{}/local_*".format(dir2, sub_dir1))

                if not (metrics_file1 and metrics_file2):
                    rows.append([sub_dir1, "log_file", "NA", "NA", "NA", "pass"])
                    continue

            metrics1 = pd.read_csv(metrics_file1[0])
            metrics2 = pd.read_csv(metrics_file2[0])

            for col in metrics1.columns[1:]:
                for agg_func in aggregates:
                    name = "{}_{}".format(agg_func, str(col))
                    try:
                        val1 = getattr(metrics1[str(col)], agg_func)()
                    except TypeError:
                        val1 = "NULL"
                        print(col)

                    if str(col) in metrics2:
                        try:
                            val2 = getattr(metrics2[str(col)], agg_func)()
                        except TypeError:
                            val2 = "NULL"
                    else:
                        val2 = "NA"

                    if val1 == val2 and val1 == "NULL":
                         diff = "NULL"
                         pass_fail = "pass"
                    elif val1 == "NULL":
                        diff = val2
                        pass_fail = "fail"
                    elif val2 == "NULL":
                        diff = val1
                        pass_fail = "fail"
                    elif val2 == "NA":
                        diff = val2
                        pass_fail = "pass"
                    else:
                        try:
                            if val2 != val1:
                                diff = (abs(val2 - val1) / ((val2 + val1)/2)) * 100
                                pass_fail = "pass" if diff < diff_percent else "fail"
                            else: #special case of 0
                                pass_fail = "pass"

                        except Exception as e:
                            logger.info("error while calculating the diff {}".format(str(e)))
                            pass_fail = "fail"

                    if over_all_pass:
                        over_all_pass = pass_fail == "pass"
                    rows.append([sub_dir1, name, val1, val2, diff, pass_fail])
        else:
            rows.append([sub_dir1, "log_file", "A", "NA", "NA", "pass"])

    out_path = "{}/comparison.csv".format(out_dir)
    logger.info("Writing comparison report to log file ".format(out_path))
    with open(out_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(rows)

    return over_all_pass


def run_test_suite(artifacts_dir, test_dir, pattern, jmeter_path,
                   monitoring_server, env_name, diff_percent):
    if os.path.exists(artifacts_dir):
        raise Exception("Artifacts dir '{}' already exists. Provide different one.".format(artifacts_dir))

    path = pathlib.Path(__file__).parent.absolute()

    global_config_file = "{}/tests/common/global_config.yaml".format(path)
    with open(global_config_file) as conf_file:
        global_config = yaml.safe_load(conf_file)
    server_props = global_config["modules"]["jmeter"]["properties"]
    server_ping_url = "{}://{}:{}/ping".format(server_props["protocol"], server_props["hostname"], server_props["port"])
    try:
        requests.get(server_ping_url)
    except requests.exceptions.ConnectionError:
        raise Exception("Server is not running. Pinged url {}. Exiting..".format(server_ping_url))

    if monitoring_server:
        start_monitoring_server = "python3 {}/metrics_monitoring_server.py --start".format(path)
        code, output = run_process(start_monitoring_server, wait=False)
        time.sleep(2)

        # TODO -  Add check if server started

    junit_xml = JUnitXml()
    pre_command = 'export PYTHONPATH={}:$PYTHONPATH; '.format(str(path))

    test_yamls = get_test_yamls(test_dir, pattern)
    out_report_rows = []
    for test_file in tqdm(test_yamls, desc="Test Suites"):
        out_report_row = []
        suite_name = os.path.basename(test_file).rsplit('.', 1)[0]
        with Timer("Test suite {} execution time".format(suite_name)) as t:
            suit_artifacts_dir = "{}/{}".format(artifacts_dir, suite_name)
            options_str = get_options(suit_artifacts_dir, jmeter_path)
            code, err = run_process("{} bzt {} {} {}".format(pre_command, options_str,
                                                             test_file, global_config_file))
            suite_time = t.diff()
            suite_start = t.start

        # Assumes default file name
        xunit_file = "{}/xunit.xml".format(suit_artifacts_dir)
        tests, failures, skipped, errors = 0, 0, 0, 0
        err_txt = ""
        out_report_row = [suite_name]
        ts = TestSuite(suite_name)
        if os.path.exists(xunit_file):
            xml = JUnitXml.fromfile(xunit_file)
            for i, suite in enumerate(xml): #tqqdm
                for case in suite:
                    name = "scenario_{}: {}".format(i, case.name)
                    result = case.result
                    if isinstance(result, Error):
                        errors += 1
                        err_txt = err
                    elif isinstance(result, Failure):
                        failures += 1
                        err_txt = err
                    elif isinstance(result, Skipped):
                        skipped += 1
                    else:
                        tests +=1

                    tc = TestCase(name)
                    tc.result = result
                    # TODO Fix html report with system_err
                    # tc.system_err = err_txt[:-4]
                    ts.add_testcase(tc)
                    out_report_row.extend([name, result._tag if result else "passed"])
                    out_report_rows.append(out_report_row)

        else:
            tc = TestCase(suite_name)
            if code:
                tc.result = Error("Suite run failed", "Error")
                # tc.system_err = err[:-4]
            else:
                tc.result = Skipped()
                # tc.system_out = err[:-4]
            ts.add_testcase(tc)

        ts.hostname = socket.gethostname()
        ts.timestamp = suite_start
        ts.time = suite_time
        ts.tests = tests
        ts.failures = failures
        ts.skipped = skipped
        ts.errors = errors
        ts.update_statistics()
        junit_xml.add_testsuite(ts)

    junit_xml.update_statistics()
    junit_xml_path = '{}/junit.xml'.format(artifacts_dir)
    junit_html_path = '{}/junit.html'.format(artifacts_dir)
    junit_xml.write(junit_xml_path)
    run_process("vjunit -f {} -o {}".format(junit_xml_path, junit_html_path))

    if monitoring_server:
        stop_monitoring_server = "python3 {}/metrics_monitoring_server.py --stop".format(path)
        run_process(stop_monitoring_server)

    with open('final_report.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)

        csv_writer.writerows(out_report_rows)

    path = pathlib.Path(artifacts_dir)

    commit_id = subprocess.check_output('git rev-parse --short HEAD'.split()).decode("utf-8")[:-1]
    new_name = "{}_{}_{}".format(env_name, commit_id, suite_start)

    # new_name_path = "{}/{}/".format(path.parent, new_name)
    # os.rename(artifacts_dir, new_name_path)

    # latest_dir = get_latest_dir("{}/comp_data".format(artifacts_dir) , env_name)
    # result = compare_artifacts(artifacts_dir, latest_dir, artifacts_dir, diff_percent=diff_percent)

    compare_dir = download_s3_files(env_name, "{}/comp_data".format(artifacts_dir))
    compare_result = True
    if compare_dir:
        compare_result = compare_artifacts(artifacts_dir, compare_dir, artifacts_dir, diff_percent=diff_percent)

    upload_to_s3(artifacts_dir, new_name)

    if junit_xml.errors or junit_xml.failures or junit_xml.skipped:
        sys.exit(3)

    if not compare_result:
        sys.exit(4)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, format="%(message)s", level=logging.INFO)
    parser = argparse.ArgumentParser(prog='run_perfomance_suite.py', description='Performance Test Suite Runner')
    parser.add_argument('-a', '--artifacts-dir', nargs=1, type=str, dest='artifacts', required=True,
                           help='A artifacts directory')

    parser.add_argument('-e', '--env-name', nargs=1, type=bool, dest='env_name', default=[socket.gethostname()],
                        help='environment on which MMS server is running')

    parser.add_argument('-d', '--test-dir', nargs=1, type=str, dest='test_dir', default=[None],
                           help='A test dir')

    parser.add_argument('-p', '--pattern', nargs=1, type=str, dest='pattern', default=["*.yaml"],
                           help='Test case file name pattern. example *.yaml')

    parser.add_argument('-j', '--jmeter-path', nargs=1, type=str, dest='jmeter_path', default=[None],
                        help='JMeter executable bin path')

    parser.add_argument('-m', '--monitoring-server', nargs=1, type=bool, dest='monitoring_server', default=[True],
                        help='Whether to start monitoring server')

    parser.add_argument('-c', '--diff-percent', nargs=1, type=float, dest='diff_percent', default=[None],
                        help='Acceptable percentage difference between metrics from previous runs')

    args = parser.parse_args()
    run_test_suite(args.artifacts[0], args.test_dir[0], args.pattern[0], args.jmeter_path[0],
                   args.monitoring_server[0], args.env_name[0], args.diff_percent[0])
