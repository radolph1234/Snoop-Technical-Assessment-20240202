'''
Entry point script used by local debugger and remote server
Ensures correct configuration is in place and runs a given job
'''

import argparse
import os
from importlib import import_module


def add_default_args(argparse_parser):
    '''used in local runs or if values not given
    '''

    default_env = os.environ.get("environment")
    default_process = os.environ.get("pipeline")

    argparse_parser.add_argument('environment', default=default_env,
                                 type=str, nargs='?')
    argparse_parser.add_argument('job', default=default_process,
                                 type=str, nargs='?')


def execute_job(arguments):
    '''executes the job which was given
    e.g. calls the .process() function from within a stages .py script
    '''
    try:
        job_module, job_method = arguments.job.rsplit('.', 1)
        mod = import_module(job_module)
        met = getattr(mod, job_method)
        met(arguments)
    except Exception as e:
        raise Exception(f'{str(e)}')


parser = argparse.ArgumentParser()
add_default_args(parser)
run_args = parser.parse_args()

execute_job(run_args)
