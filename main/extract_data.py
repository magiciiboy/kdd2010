'''
KDD Cup Data Extractor
----------------------
In this script we read data from text file and import in to mongodb.

'''

import sys, getopt
from pymongo import Connection as MongoConnection

from main.db.structure import *
from main.db.structure import TransactionVector
from main.db.mongo import *
from main.utils import SimilarityUtil

_db_loaded      = False
_num_students   = 0
_num_probs      = 0

def extract_number(file_name, force=True):
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
                prob_name = trans['prob_name']
                if student_id not in STUDENTS:
                    no_student += 1
                    STUDENTS[student_id] = {
                        'student_id': student_id,
                        'trans': {
                            prob_name: trans
                        }
                    }
                else:
                    STUDENTS[student_id]['trans'][prob_name] = trans

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
    MONGO_DATABASE.drop_collection('problems')

    collection_student = MONGO_DATABASE['students']
    num_student = collection_student.count()
    print "Checking collection `students`. Rows: %s" % num_student
    if not num_student or force:
        print "Importing %s students transactions ..." % no_student
        for student_id in STUDENTS:
            collection_student.insert(STUDENTS[student_id])
        print "Imported %s students" % len(STUDENTS)

    collection_problem = MONGO_DATABASE['problems']
    num_problem = collection_problem.count()
    print "Checking collection `problems`. Rows: %s" % num_problem
    if not num_problem or force:
        for prob_id in PROBLEMS:
            collection_problem.insert({'prob_name': prob_id})
        print "Imported %s problems" % len(PROBLEMS)

    print "Imported !"

    no_prob = len(PROBLEMS)
    no_student = len(STUDENTS)

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
    idx = 0
    for id1, st1 in STUDENTS.iteritems():
        sims = {}
        # print '- Student %s ' % id1
        sys.stdout.write("\rProcessing: {0:.0f}%".format((idx+1) * 100/no_student))
        sys.stdout.flush()
        for id2, st2 in STUDENTS.iteritems():
            if id1 != id2:
                sim = _sim_students(st1, st2)
                if sim:
                    # print '\t - Similarity to student %s: %s' % (id2, sim)
                    sims[id2] = sim
        STUDENTS[id1]['sims'] = sims
        idx += 1

    print '\nStoring similarity ...'
    MONGO_DATABASE.drop_collection('students')
    collection_student = MONGO_DATABASE['students']
    for sid in STUDENTS:
        collection_student.insert(STUDENTS[sid])

    print 'Done'

def prob_solve_single(student_id, prob_name):
    '''
    Calculate probability of a student when solving a problem 
    :param student_id: Student ID
    :param prob_name: Student
    '''
    print '-- Calculate probability --'
    _load_dataset_deep()

    s = _had_solved(student_id, prob_name)
    if s == -1:
        sim_students = STUDENTS[student_id]['sims']
        dot_val = 0.0
        sum_sim = 0.0
        sum_std = 0.0
        for std, sim_std in sim_students.iteritems():
            std_has_solve = _had_solved(std, prob_name)
            if std_has_solve != -1:
                print 'Sim: ', std, sim_std, std_has_solve
                dot_val += sim_std * std_has_solve
                sum_sim += sim_std
                sum_std += 1.0
            else:
                dot_val += 0.0
                sum_sim += 0.0
                sum_std += 0.0
        if sum_sim:
            prob = dot_val / sum_std
        else:
            prob = 0.0
    else:
        prob = s

    return prob

def _load_dataset_deep():
    ''' Load dataset from mongo and map in memory '''
    global _db_loaded
    if _db_loaded:
        return 0
    else:
        print 'Loading data from database ...'
        students = load_students()
        print 'Maping students ...'
        no_student = 0
        for student in students:
            STUDENTS[student['student_id']] = student
            no_student += 1

        _db_loaded = True
        return 1

def _had_solved(student_id, prob_name):
    '''
    Check whether a student solved a problem
    :param student_id: Student ID
    :param prob_name: Student
    '''
    if STUDENTS and student_id in STUDENTS and STUDENTS[student_id]['trans'] \
        and prob_name in STUDENTS[student_id]['trans']:
        if STUDENTS[student_id]['trans'][prob_name]['trans_corrects']:
            return 1.0
        else:
            return 0.0
    else:
        return -1


def _init_matrix():
    _load_dataset_deep()
    print 'New dataset matrix'

def _sim_students(s1, s2):
    num_sim = 0.0
    sum_sim = 0.0
    trans1 = s1['trans']
    trans2 = s2['trans']

    for t1, tr1 in trans1.iteritems():
        for t2, tr2 in trans2.iteritems():
            if t1 == t2:
                num_sim += 1.0
                # Compute similarity
                v1 = TransactionVector.from_trans(tr1)
                v2 = TransactionVector.from_trans(tr2)
                sim = SimilarityUtil.cos(v1, v2)
                sum_sim += sim

    aggr_sim = 0.0
    if num_sim:
        aggr_sim = sum_sim / num_sim

    return aggr_sim


def _nomalize_trans(s):
    return s



