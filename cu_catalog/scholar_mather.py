# This script is just workbench for manually reviewing unsure google scholar matches

import json

import pandas as pd

import models.cudata as cudata
from columbia_crawler import util
from cu_catalog import config
from cu_catalog.models.util import str_similarity, extract_word_stems2dict

start_line = 516

instructors_internal_db = util.InstructorsInternalDb(['gscholar_last_search', 'gscholar_last_update'])
instructors = cudata.load_instructors()
classes = pd.concat([
    cudata.load_term('2016-Spring'),
    cudata.load_term('2016-Fall'),
    cudata.load_term('2017-Spring'),
    cudata.load_term('2017-Fall'),
    cudata.load_term('2018-Spring'),
    cudata.load_term('2018-Fall'),
    cudata.load_term('2019-Spring'),
    cudata.load_term('2019-Fall'),
    cudata.load_term('2020-Fall'),
    cudata.load_term('2020-Spring'),
    cudata.load_term('2021-Fall'),
    cudata.load_term('2021-Spring')])


def get_course_title_by_code(course_code: str):
    df = classes[classes['course_code'] == course_code]
    if len(df) == 0:
        return course_code
    return df.iloc[0]['course_title']


def _save():
    df_json = pd.DataFrame(instructors.values())
    cudata.store_instructors(df_json)
    instructors_internal_db.store()
    statf.flush()


skip_name = None
num_line = 0
f = open(config.DATA_GSCHOLAR_UNSURE_FILENAME, 'r')
statf = open('unsure-stat.json', 'a')
while True:
    line = f.readline()
    if not line:
        break
    num_line += 1
    if num_line < start_line:
        continue

    item = json.loads(line)
    instructor_name = item['instructor']
    result = item['result']
    catalog_instr = instructors[instructor_name]

    if catalog_instr['gscholar'] is not None:
        # we already found gscholar profile, don't overwrite
        continue

    terms = set([t for t, c in catalog_instr['classes']])
    clss = set([c for t, c in catalog_instr['classes']])
    clss = list(map(get_course_title_by_code, clss))
    name_similarity = str_similarity(instructor_name, result['name'])

    # compute overlap of words in cu and cg descriptions
    cu_words = extract_word_stems2dict(" ".join(catalog_instr['departments']) + " ".join(clss), remove_stopwords=True)
    cg_words = extract_word_stems2dict(" ".join(result['interests']), remove_stopwords=True)
    overlap = set(w for w in cu_words.keys() if w in cg_words.keys())

    features = {'name': instructor_name,
                'overlap': list(overlap),
                'name_similarity': name_similarity,
                'result': result}

    # skip if name marked na
    if skip_name == instructor_name:
        features['label'] = False
        statf.write(json.dumps(features) + "\n")
        continue

    if not (len(overlap) >= 2 and name_similarity > 0.95):
        # TODO comment this out for processing all entries
        continue

    print("Instructor:          ", instructor_name)
    print("Department @CU:      ", ", ".join(catalog_instr['departments']))
    print("Classes @CU:         ", "; ".join(clss))
    print("Terms @CU:           ", ", ".join(terms))
    print('---')
    print("Name @GC:            ", result['name'])
    print("Interests @GC:       ", ", ".join(result['interests']))
    print("Affiliation @GC:     ", result['affiliation'])
    print("GC Link:              https://scholar.google.com/citations?user=" + result['scholar_id'])
    print('Name similarity:', name_similarity)
    print('Interests overlap:', overlap)

    print("  =>  Match? y/n/na")
    r = input()
    if r == 'y':
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
        catalog_instr['gscholar'] = gscholar
        instructors_internal_db.update_instructor(instructor_name, 'gscholar_last_update')
        _save()
        features['label'] = True
    elif r == 'na':
        skip_name = instructor_name
        features['label'] = False
    else:
        features['label'] = False

    statf.write(json.dumps(features) + "\n")

    print("processed", num_line + 1)

f.close()
print("done.")
