'''
KDD Cup Data Extractor
----------------------
In this script we read data from text file and import in to mongodb.

'''

import sys, getopt
from pymongo import Connection as MongoConnection

from main.db.structure import *
from main.db.mongo import *

def extract_number(file_name, force=False):
    ''' Read KDD dataset and import to mongo with new structure '''
    # New structure in Mongo DB
    # students: {
    #    <student_id>: {
    #        'trans': <list_of_transactions>,
    #        'sims': <similarity_to_other_students>
    #    }
    # }
    #

    file_name = file_name or KDD_DATA_TEST
    no_line = 0
    no_student = 0
    no_prob = 0
    no_step = 0

    print 'Opening data file %s ...' % file_name

    # Step 1: Read file
    with open (file_name) as fread:
        for line in fread:
            if (no_line % READ_STEP_PRINT) == 0:
                print '- Reading line %s ...' % no_line

            # Extract data
            if no_line > 0:
                trans = extract_line(line)
                student_id = trans['student_id']
                prob_id = trans['prob_name']
                prob_step = prob_id + '_' + trans['step_name']

                # Check whether student exists
                if student_id not in STUDENTS:
                    no_student += 1
                    STUDENTS[student_id] = {
                        'student_id': student_id,
                        'trans': [trans]
                    }
                else:
                    STUDENTS[student_id]['trans'].append(trans)

                # Check whether problem exists
                if prob_id not in PROBLEMS:
                    no_prob += 1
                    PROBLEMS[prob_id] = 1

                # Check whether step exists
                if prob_step not in PROBLEM_STEPS:
                    no_step += 1
                    PROBLEM_STEPS[prob_step] = 1

            no_line += 1

    # Step 2: Import into database
    print "Dropping old data ..."
    MONGO_DATABASE.drop_collection('students')

    collection_student = MONGO_DATABASE['students']
    num_student = collection_student.count()
    print "Checking collection `students`. Rows: %s" % num_student
    if not num_student or force:
        print "Importing %s students transactions ..." % no_student
        for student_id in STUDENTS:
            collection_student.insert(STUDENTS[student_id])
        print "Imported %s students" % len(STUDENTS)

    print "Imported !"


    sparsity = 100.0 - (100.0 * no_line / (no_student * no_prob))
    sparsity_step = 100.0 - (100.0 * no_line / (no_student * no_step))

    print '* Statistic *'
    print '\t - No of line: %s' % no_line
    print '\t - No of user: %s' % no_student
    print '\t - No of problems: %s' % no_prob
    print '\t - Problem Sparsity: %s %%' % sparsity
    print '\t - Step Sparsity: %s %%' % sparsity_step

def extract_line(astr):
    parts = astr.split(SEPARATED_CHAR)
    n_parts = len(parts)

    trans = {}
    if n_parts != NUM_ATTRS:
        raise Exception('Data input incorrect. Expect %s cols. Got %s.' % (NUM_ATTRS, n_parts))
    else:
        for idx, part_name in enumerate(DATA_STRUCTURE_ATTR_MAPS):
            trans[part_name] = parts[idx]
    return trans

def load_students():
    collection_student = MONGO_DATABASE['students']
    students = collection_student.find({})
    return students

def calculate_similarity():
    print '-- Calculate similarity --'
    print 'Loading data from database ...'
    students = load_students()
    print 'Maping students ...'
    no_student = 0
    for student in students:
        STUDENTS[student['student_id']] = student
        no_student += 1

    print 'Loaded %s students' % no_student
    print 'Calculating similarity ...'
    for id1, st1 in STUDENTS.iteritems():
        sims = {}
        print '- Student %s ' % id1
        for id2, st2 in STUDENTS.iteritems():
            if id1 != id2:
                sim = _sim_students(st1, st2)
                if sim:
                    print '\t - Similarity to student %s: %s' % (id2, sim)
                    sims[id2] = sim
        STUDENTS[id1]['sims'] = sims
    print 'Storing similarity ...'
    MONGO_DATABASE.drop_collection('students')
    collection_student = MONGO_DATABASE['students']
    for sid in STUDENTS:
        collection_student.insert(STUDENTS[sid])

    print 'Done'

    # Delete old db

def _sim_students(s1, s2):
    num_same = 0.0
    num_solve_same = 0.0
    trans1 = s1['trans']
    trans2 = s2['trans']

    for t1 in trans1:
        for t2 in trans2:
            if t1['prob_name'] == t2['prob_name']:
                num_same += 1.0
                if t1['trans_corrects'] > 0 and t1['trans_corrects'] > 0:
                    num_solve_same += 1.0

    if num_same:
        sim = num_solve_same / num_same
    else:
        sim = 0.0
    return sim


def _nomalize_student_trans(s):
    return s



