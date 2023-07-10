import argparse
import dataclasses
import datetime
import gzip
import json
import logging
import pathlib
import re
import statistics
import string
import sys
from dataclasses import dataclass
from collections import namedtuple, defaultdict
from datetime import datetime
from pathlib import Path
from typing import NamedTuple
# !/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

BASE_CONFIG = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "SCRIPT_LOG_FILE": "Logging",
    "ERROR_PROC": 1,
    "SORTED_FIELD": "time_sum",  # count, count_perc, time_sum, time_perc, time_avg, time_max, time_med
}

logging.basicConfig(format="[%(asctime)s]%(msecs)d %(levelname)s %(message)s",
                    datefmt="%Y.%m.%d %H:%M:%S",
                    level=logging.INFO,
                    filename=BASE_CONFIG["SCRIPT_LOG_FILE"])


# LogInfo = namedtuple("LogInfo", ["path", "ext", "report_name", "report_dir"])

@dataclass
class LogInfo:
    path: pathlib.PosixPath = None
    ext: str = None
    report_name: str = None
    report_dir: pathlib.PosixPath = None


def get_last_log(config):
    """https://snyk.io/advisor/python/pathlib/functions/pathlib.Path.mkdir
    https: // tproger.ru / translations / regular - expression - python /
    https://habr.com/ru/articles/330034/"""
    logging.info("Get lastLog")
    log_pattern = re.compile(r"nginx-access-ui\.log-(?P<date>\d{8})(?P<ext>\.gz|log)?$")
    if not Path(config["REPORT_DIR"]).exists():
        logging.info("Create Report Dir")
        Path.mkdir(Path.cwd() / config["REPORT_DIR"])

    report_dir = pathlib.Path(str(config["REPORT_DIR"]))
    if not Path(Path.cwd() / config["LOG_DIR"]).exists():
        logging.info("No Dir With Logs")
        raise FileNotFoundError("No Dir With Logs")

    log_dir = pathlib.Path(str(config["LOG_DIR"]))
    for path in log_dir.iterdir():
        try:

            log_name = log_pattern.match(str(path).split('/')[1])
            log_date = log_name['date']
            extension = log_name['ext']
            # [(log_date, extension)] = re.findall(r"nginx-access-ui\.log-(\d{8})(\.gz|txt)?$", str(path).split('/')[1])
            # print(log_date,extension)
            report_date = datetime.strptime(log_date, '%Y%m%d').date().strftime('%Y.%m.%d')
            report_name = f'report-{report_date}.html'
            if Path(report_dir / report_name).exists():
                logging.info(f" log all ready parsed {path}")
            else:
                logging.info(f" Find Log For Parse {path}")
                return LogInfo(path=path,
                               ext=extension,
                               report_name=report_name,
                               report_dir=report_dir)
        except ValueError:
            logging.exception("ValueError")


def line_generator(log_info):
    """https://regex101.com/
    https://proglib.io/p/regulyarnye-vyrazheniya-v-python-za-5-minut-teoriya-i-praktika-dlya-novichkov-i-ne-tolko-2022-04-05
    """
    url_pattern = re.compile(r"(GET|POST)\s(?P<url>\S+)")
    time_pattern = re.compile(r"(?P<time>\d+\.\d+$)")

    opener = gzip.open if log_info.ext == ".gz" else open

    with opener(Path(log_info.path), 'rt', encoding='utf8') as log:
        logging.info(f" Start Parsing")
        for line in log:
            try:
                method, url = url_pattern.search(line).group().split()
                request_time = float(time_pattern.search(line).group())
                yield url, request_time
            except AttributeError:
                yield None, None


def stats_calculater(LogInfo, config):
    """count - сколько раз встречается URL, абсолютное значение
    count_perc - сколько раз встречается URL, в процентнах относительно общего
    числа запросов
    time_sum - суммарный $request_time для данного URL'а, абсолютное значение
    time_perc - суммарный $request_time для данного URL'а, в процентах
    относительно общего $request_time всех запросов
    time_avg - средний $request_time для данного URL'а
    time_max - максимальный $request_time для данного URL'а
    time_med - медиана $request_time для данного URL'а
    https://pythonim.ru/list/kak-nayti-srednee-znachenie-spiska-v-python
    https://docs.python.org/3/library/statistics.html
    https://docs-python.ru/standart-library/modul-collections-python/klass-defaultdict-modulja-collections/

    """

    total_time: float = 0  # total time of all requests
    correct_line: int = 0  # parsed line
    incorrect_line: int = 0  # url or request time not in line
    raw_data = defaultdict(list)  # urls with all request time
    report: dict = []  # dict for jason dump
    for url, request_time in line_generator(LogInfo):
        if None in (url, request_time):
            incorrect_line += 1
        else:
            correct_line += 1
            total_time += float(request_time)
            raw_data[url].append(request_time)
    pars_error = 100 * incorrect_line / correct_line
    if pars_error >= config["ERROR_PROC"]:
        return logging.info("pars fails=", pars_error)
    else:
        for url in raw_data:
            url_count = len(raw_data[url])  # count urls by request time
            request_total = sum(raw_data[url])  # total request time of 1 url
            report.append({
                'url': url,
                'count': url_count,
                'count_perc': round(100 * url_count / float(correct_line), 3),
                'time_sum': round(request_total, 3),
                'time_perc': round(100 * request_total / total_time, 3),
                'time_avg': round(statistics.mean(raw_data[url]), 3),
                'time_max': round(max(raw_data[url]), 3),
                "time_med": round(statistics.median(raw_data[url]), 3),
            })

    return report


def create_report(report, log_info, config):
    """https://docs-python.ru/standart-library/modul-string-python/otchety-ispolzovaniem-string-template/
    https://docs.python.org/3/library/string.html#string.Template.safe_substitute"""

    sorted_report = sorted(report, key=lambda x: x[config["SORTED_FIELD"]], reverse=True)[:config["REPORT_SIZE"]]

    with Path(log_info.report_dir / "report.html").open(encoding='utf8') as f:
        template = string.Template(f.read())

    report = template.safe_substitute(table_json=json.dumps(sorted_report))

    with Path(log_info.report_dir / log_info.report_name).open('w', encoding='utf8') as f:
        f.write(report)
    logging.info(f"report {log_info.report_name} for log {log_info.path} created")


def main(config):
    log_info = get_last_log(config)
    if log_info is None:
        logging.info("no logs for parsing")
        sys.exit()
    else:
        report = stats_calculater(log_info, config)
        create_report(report, log_info, config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='log_analyzer',
        description='What the program does',
        epilog='Text at the bottom of help')
    parser.add_argument('-c', '--config', dest="new_config", help="add config file ")
    arg = parser.parse_args()
    if arg.new_config:
        with open(arg.new_config) as cfg:
            config = json.load(cfg)

    else:
        config = BASE_CONFIG
    # print(arg.new_config)
    # print(config)
    try:
        main(config)
    except KeyboardInterrupt:
        logging.exception("KeyboardInterrupt")
    except Exception as err:
        logging.exception(err)
