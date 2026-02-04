import datetime
import logging

from shared.scheduler import get_apis_to_run
from apis.newsapi import run_newsapi
from apis.newsdata import run_newsdata
from apis.newsdatahub import run_newsdatahub

def main(timer):
    now = datetime.datetime.utcnow()
    logging.info(f"Timer triggered at {now}")

    apis = get_apis_to_run(now.minute)

    if "newsapi" in apis:
        run_newsapi(now)

    if "newsdata" in apis:
        run_newsdata(now)

    if "newsdatahub" in apis:
        run_newsdatahub(now)
