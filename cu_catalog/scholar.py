# Crawling Google Scholar profiles of instructors
# Because Google bans too frequent requests, this script needs not to be effient
# and actually needs to

import json
import time

import pandas as pd
from scholarly import scholarly
import models.cudata as cudata
import random
from columbia_crawler import util
from cu_catalog import config
from cu_catalog.models.util import words_match2

# Configuration options
MIN_WAIT, MAX_WAIT = 7, 60
MAX_PROCESS = 300
UPDATE_MIN_DAYS = 30
UPDATE_MAX_DAYS = 90

# logger setup
logger = config.get_logger('scholar', 'scholar.log')

# load data
instructors = cudata.load_instructors()
instructors_internal_db = util.InstructorsInternalDb(['gscholar_last_search', 'gscholar_last_update'])

# compute overall progress
def _check_date(x):
    return util.InstructorsInternalDb.recent_threshold(x, UPDATE_MIN_DAYS, UPDATE_MAX_DAYS)
df = instructors_internal_db.df_internal
# TODO need to join instructors here and sheck if 'gscholar' is set or not: need to update or search
df = df[df['gscholar_last_search'].apply(_check_date)]
to_process = len(df)
total = len(instructors_internal_db.df_internal)
logger.info("Overall progress: %s / %s", total - to_process, total)
will_process = min(to_process, MAX_PROCESS)
logger.info("Today will process: %s / %s", will_process, to_process)

# print estimate
estimate_min = will_process * (MIN_WAIT + (MAX_WAIT - MIN_WAIT)/2 + 5) / 60
logger.info("Processing %s will take about %.1f mins or %.1f hours.",
            will_process, estimate_min, estimate_min / 60)

def _save():
    """ Saves progress """
    df_json = pd.DataFrame(instructors.values())
    cudata.store_instructors(df_json)
    instructors_internal_db.store()
    unsure_file.flush()


names = list(instructors.keys())
random.seed(0)
random.shuffle(names)
pictures = {}
num_processed = 0
num_found = 0
num_possible = 0
unsure_file = open('unsure.json', 'a')  # list of possible matches
for instructor_name in names:
    if not instructors_internal_db.check_its_time(instructor_name, 'gscholar_last_search',
                                                  UPDATE_MIN_DAYS, UPDATE_MAX_DAYS):
        continue
    if instructors[instructor_name]['gscholar'] \
            and instructors_internal_db.check_its_time(instructor_name, 'gscholar_last_update',
                                                       UPDATE_MIN_DAYS, UPDATE_MAX_DAYS):
        # skipping cause not expired 'gscholar_last_update' and gscholar already exists
        continue

    # search or update
    if instructors[instructor_name]['gscholar']:
        logger.info("Updating instructor: %s", instructor_name)
        result = scholarly.search_author_id(instructors[instructor_name]['gscholar']['scholar_id'])
        found = True
    else:
        logger.info("Searching instructor: %s", instructor_name)
        results = scholarly.search_author(instructor_name)
        # print("Our db:", instructors[instructor_name])
        found = False
        for result in results:
            # print("Google Scholar result: ", result)
            if ('columbia' in result['affiliation'].lower() and 'british' not in result['affiliation'].lower()) \
                    or 'columbia' in result['email_domain'].lower() \
                    or 'barnard' in result['email_domain'].lower():
                found = True
                break
            elif words_match2(instructor_name, result['name']):
                logger.info("Possible match. Link: https://scholar.google.com/citations?user=%s", result['scholar_id'])
                unsure_file.write(json.dumps({'instructor': instructor_name, 'result': result}) + "\n")
                num_possible += 1

    if found:
        num_found += 1
        scholarly.fill(result, sections=['indices', 'counts'])
        logger.info("Found in GScholar: %s", result)
        gscholar = {
            'scholar_id': result.get('scholar_id'),
            'interests': result.get('interests'),
            'citedby': result.get('citedby'),
            'citedby5y': result.get('citedby5y'),
            'hindex': result.get('hindex'),
            'hindex5y': result.get('hindex5y'),
            'i10index': result.get('i10index'),
            'i10index5y': result.get('i10index5y'),
            'cites_per_year': result.get('cites_per_year'),
        }
        # todo: save picture
        # pictures[instructor_name] = result['url_picture']
        # exclude this one: https://scholar.google.com/citations?view_op=medium_photo&user=DTl7ej4AAAAJ
        instructors[instructor_name]['gscholar'] = gscholar
        instructors_internal_db.update_instructor(instructor_name, 'gscholar_last_update')
    else:
        logger.info("Instructor not found in Google Scholar: %s", instructor_name)
    instructors_internal_db.update_instructor(instructor_name, 'gscholar_last_search')

    # print progress
    num_processed += 1
    logger.info("\n")
    logger.info("Processed %s / %s", num_processed, MAX_PROCESS)
    if num_processed % 10 == 0:
        _save()
        logger.info("Updated instructors: %s / %s", num_found, MAX_PROCESS)
        logger.info("Unsure search results: %s", num_possible)
        logger.info("*** Saved ***")
    if num_processed == MAX_PROCESS:
        break

    # pause to avoid being banned
    w = random.randint(MIN_WAIT, MAX_WAIT)
    logger.info('Pausing for %s sec', w)
    time.sleep(w)

_save()
logger.info("Updated instructors: %s / %s", num_found, MAX_PROCESS)
logger.info("Unsure search results (unsure.json): %s", num_possible)
