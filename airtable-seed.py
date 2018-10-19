import os

from airtable import Airtable

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from xml.etree import ElementTree


vote_key = os.environ['VOTE_SMART_API_KEY']
air_key = os.environ['AIR_TABLE_API_KEY']
woman_up = os.environ['AIR_TABLE_WOMANUP']
year = '2018'
previous_year = '2017'

# VOTE SMART END POINTS

base_vote_url = 'http://api.votesmart.org'
offices_url = base_vote_url + '/Office.getOfficesByType?key=' + vote_key
states_url = base_vote_url + '/State.getStateIDs?key=' + vote_key
districts_url = base_vote_url + '/District.getByOfficeState?key=' + vote_key
elections_state_year_url = base_vote_url + '/Election.getElectionByYearState?key=' + vote_key
candidates_election_url = base_vote_url + '/Candidates.getByElection?key=' + vote_key
candidate_bio_url = base_vote_url + '/CandidateBio.getBio?key=' + vote_key
categories_url = base_vote_url + '/Rating.getCategories?key=' + vote_key
sig_url = base_vote_url + '/Rating.getSig?key=' + vote_key
ratings_url = base_vote_url + '/Rating.getSigRatings?key=' + vote_key
candidate_ratings_url = base_vote_url + '/Rating.getCandidateRating?key=' + vote_key
candidate_address_url = base_vote_url + '/Address.getOfficeWebAddress?key=' + vote_key

# WOMANUP TABLES

states_table = Airtable(woman_up, 'states', air_key)
offices_table = Airtable(woman_up, 'offices', air_key)
office_types_table = Airtable(woman_up, 'office_types', air_key)
districts_table = Airtable(woman_up, 'districts', air_key)
elections_table = Airtable(woman_up, 'elections', air_key)
candidates_table = Airtable(woman_up, 'candidates', air_key)
addresses_table = Airtable(woman_up, 'addresses', air_key)
category_table = Airtable(woman_up, 'categories', air_key)
sigs_table = Airtable(woman_up, 'sigs', air_key)
ratings_table = Airtable(woman_up, 'ratings', air_key)
scores_table = Airtable(woman_up, 'scores', air_key)
rating_categories = Airtable(woman_up, 'rating_categories', air_key)

# GENERIC SESSION REQUEST

def get_request(url, params=''):
    s = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    r = s.get(url, params=params)
    return r

# RETRIEVAL METHODS FOR ITERATORS

def get_state_ids():
    states = []
    state_records = states_table.get_all()
    for state in state_records:
        state_id = state['fields']['stateId']
        states.append(state_id)
    return states

def get_sig_ids():
    sigs = []
    sig_records = sigs_table.get_all()
    for sig in sig_records:
        sig_id = sig['fields']['sigId']
        sigs.append(sig_id)
    return sigs

def get_rating_ids():
    ratings = []
    ratings_records = ratings_table.get_all()
    for rating in ratings_records:
        rating_id = rating['fields']['ratingId']
        ratings.append(rating_id)
    return ratings

def get_candidate_ids():
    candidates = []
    candidates_records = candidates_table.get_all()
    for candidate in candidates_records:
        candidate_id = candidate['fields']['candidateId']
        candidates.append(candidate_id)
    return candidates

def get_office_ids():
    offices = []
    offices_records = offices_table.get_all()
    for office in offices_records:
        office_id = office['fields']['officeId']
        offices.append(office_id)
    return offices

# ITERATORS

states = get_state_ids()
sigs = get_sig_ids()
ratings = get_rating_ids()
offices = get_office_ids()

# AIRTABLE RETRIEVAL METHODS

# METHODS TO COLLECT IDs OF RECORDS
def get_sig_id(sig_id):
    return sigs_table.match('sigId', sig_id)['id']

def get_state_id(state_id):
    return states_table.match('stateId', state_id)['id']

def get_office_id(office_id):
    return offices_table.match('officeId', office_id)['id']

def get_district_id(district_id):
    return districts_table.match('districtId', district_id)['id']

def get_rating_id(rating_id):
    return ratings_table.match('ratingId', rating_id)['id']

def get_category_id(category_id):
    return category_table.match('categoryId', category_id)['id']

def get_candidate_id(candidate_id):
    return candidates_table.match('candidateId', candidate_id)['id']

def get_election_id(election_id):
    return elections_table.match('electionId', election_id)['id']

def get_office_id(office_id):
    return offices_table.match('officeId', office_id)['id']

def get_office_type_id(office_type_id):
    return office_types_table.match('officeTypeId', office_type_id)['id']

# METHODS FOR FIRST SEED (mostly static, only run when setting up)

def office_seed():
    for office_type_id in ['P', 'C', 'G', 'S', 'K', 'L', 'J', 'M', 'N', 'H' ]:
        params = { 'officeTypeId' : office_type_id }
        r = get_request(offices_url, params)
        root = ElementTree.fromstring(r.content)
        for office in root.iter('office'):
            office_id = office.find('officeId').text
            office_type_id = office.find('officeTypeId').text
            office_level_id = office.find('officeLevelId').text
            office_branch_id = office.find('officeBranchId').text
            office_name = office.find('name').text

            offices_table.insert({
                'officeId' : office_id,
                'officeTypeId' : office_type_id,
                'officeLevelId' : office_level_id,
                'officeBranchId' : office_branch_id,
                'officeName' : office_name,
            })


def state_seed():
    r = requests.get(states_url)
    root = ElementTree.fromstring(r.content)
    for state in root.iter('state'):
        state_id = state.find('stateId').text
        name = state.find('name').text
        states_table.insert({'stateId': state_id, 'name': name})

def category_seed():
    r = requests.get(categories_url)
    root = ElementTree.fromstring(r.content)
    for category in root.iter('category'):
        category_id = category.find('categoryId').text
        category_name = category.find('name').text
        category_table.insert(
            {'categoryId': category_id,
             'name': category_name,
            })

def district_seed():
    for state in states:
        state_id_record = get_state_id(state)
        for office in offices:
            office_id_record = get_office_id(office)
            params = { 'officeId' : office, 'stateId' : state }
            r = get_request(districts_url, params)
            root = ElementTree.fromstring(r.content)
            for district in root.iter('district'):
                district_id = district.find('districtId').text
                district_name = district.find('name').text
                print('inserting record for ' + district_name + ' in ' + state )
                districts_table.insert({
                    'districtId' : district_id,
                    'districtName' : district_name,
                    'officeId' : [ office_id_record ],
                    'stateId' : [ state_id_record ],
                })

# METHODS FOR REGULAR UPDATING

def sig_seed(sig_id):
    params = {'sigId': sig_id}
    r = get_request(sig_url, params)
    root = ElementTree.fromstring(r.content)
    sig_id = root.find('sigId').text
    state_id = root.find('stateId').text
    name = root.find('name').text
    description = root.find('description').text
    
    print('inserting sig record for ' + sig_id)
    sigs_table.insert({
        'sigId': sig_id,
        'stateId': [ get_state_id(state_id) ],
        'name': name,
        'description': description,
    })
    # update local store so don't repeat seed
    sigs.append(sig_id)

def rating_seed(sig_id):
    params = {'sigId': sig_id}
    r = get_request(ratings_url, params)
    root = ElementTree.fromstring(r.content)
    for rating in root.iter('rating'):
        rating_id = rating.find('ratingId').text
        time = rating.find('timespan').text
        name = rating.find('ratingName').text
        text = rating.find('ratingText').text
        if year in time or previous_year in time:
            
            print('inserting rating record for ' + str(rating_id))
            ratings_table.insert({
                'ratingId': rating_id,
                'timespan': time,
                'ratingName': name,
                'ratingText': text,
                'sigId': [ get_sig_id(sig_id) ],
        })
        # update local store so don't repeat seed
        ratings.append(rating_id)

def candidate_ratings_seed():
    candidate_ids = get_candidate_ids()
    for candidate_id in candidate_ids:
        candidate_id_record = get_candidate_id(candidate_id)
        print('getting ratings for candidateId ' + candidate_id)
        params = {'candidateId': candidate_id}
        r = get_request(candidate_ratings_url, params)
        root = ElementTree.fromstring(r.content)
        parent_ratings = root.findall('rating')
        if parent_ratings: 
            for rating in parent_ratings:
                # return xml has nested 'rating' objects.
                # so use findall which only finds objects that are direct children of root.
                    
                # prepare to seed ratings for the candidate
                score = rating.find('rating').text
                name = rating.find('ratingName').text
                text = rating.find('ratingText').text
                sig_id = rating.find('sigId').text
                rating_id = rating.find('ratingId').text
                time = rating.find('timespan').text

                # seed info for recent ratings / sigs
                if previous_year in time or year in time:
                    # if it's a new sig, seed the information
                    if sig_id not in sigs:
                        sig_seed(sig_id)

                    # if it's a new rating, seed the information
                    if rating_id not in ratings:
                        rating_seed(sig_id)
                        
                        # also seed the categories associated with the new rating
                        # note, there are several duplicate categories for a single rating in the vote smart data
                        # will have to clean up in our database ... 
                        categories = rating.find('categories')
                        for category in categories.iter('category'):
                            category_id = category.find('categoryId').text
                            rating_categories.insert({
                                'ratingId' : [ get_rating_id(rating_id) ],
                                'categoryId': [ get_category_id(category_id) ],
                            })

                    # write the candidate's rating info
                    print('inserting ratings for candidateId ' + candidate_id)
                    score_data_obj = {
                        'ratingId': [ get_rating_id(rating_id) ],
                        'sigId': [ get_sig_id(sig_id) ],
                        'candidateId': [ candidate_id_record ],
                        'score': score,
                        'name': name,
                        'text': text,
                    }

                    print('inserting candidate rating score for rating ' + str(rating_id) + ' and candidate: ' + str(candidate_id))
                    scores_table.insert(score_data_obj)

def election_seed():
    for state in states:
        state_record = get_state_id(state)
        print('getting elections for state ' + state)
        params = { 'stateId': state, 'year': year }
        r = get_request(elections_state_year_url, params)
        root = ElementTree.fromstring(r.content)
        for election in root.iter('election'):
            election_id = election.find('electionId').text
            election_name = election.find('name').text
            office_type_id = election.find('officeTypeId').text
            election_data_obj = {
                'electionId': election_id,
                'name': election_name,
                'stateId': [ state_record ],
                'officeTypeId': [ get_office_type_id(office_type_id) ],
                }

            print('inserting election record for election: ' + str(election_id))
            elections_table.insert(election_data_obj)

def candidate_seed():
    # TODO: refactor into iterator
    elections = elections_table.get_all()
    # iterate through all current elections
    for election in elections:
        election_id = election['fields']['electionId']
        election_id_record = election['id']
        print('getting candidates for electionId ' + election_id)
        params = { 'electionId': election_id }
        r = get_request(candidates_election_url, params)
        root = ElementTree.fromstring(r.content)
        for candidate in root.iter('candidate'):

            # capture candidate election information
            candidate_id = candidate.find('candidateId').text
            election_stage = candidate.find('electionStage').text
            election_state_id = candidate.find('electionStateId').text
            election_office_id = candidate.find('electionOfficeId').text
            election_date = candidate.find('electionDate').text
            election_parties = candidate.find('electionParties').text
            election_status = candidate.find('electionStatus').text
            election_district_id = candidate.find('electionDistrictId').text
            election_state_id = candidate.find('electionStateId').text
            office_id = candidate.find('officeId').text # 'State House'
            office_district_id = candidate.find('officeDistrictId').text # '20496'
            office_state_id = candidate.find('officeStateId').text
            office_status = candidate.find('officeStatus').text # 'active'
            office_parties = candidate.find('officeParties').text
        
            # get candidate's detailed bio
            print('getting candidate detailed bio for candidateId ' + candidate_id)
            params = { 'candidateId': candidate_id }
            r = get_request(candidate_bio_url, params)
            root = ElementTree.fromstring(r.content)
            
            # capture candidate bio data
            candidate = root.find('candidate')
            is_female = candidate.find('gender').text == 'Female'
            photo = candidate.find('photo').text
            first_name = candidate.find('firstName').text
            last_name = candidate.find('lastName').text

            # if office data, capture additional office data from bio & write it
            office = root.find('office')
            in_office = 'true' if office else ''
            title = office.find('title').text if in_office else '' # 'Senator'
            first_elect = office.find('firstElect').text if in_office else ''
            last_elect = office.find('lastElect').text if in_office else ''
            next_elect = office.find('nextElect').text if in_office else '' # 2018
            term_start = office.find('termStart').text if in_office else '' # 11/10/1992
            term_end = office.find('termEnd').text if in_office else ''

            # TODO: also grab lastElect

            candidate_record_obj = {
                    'electionId' : [ election_id_record ],
                    'candidateId' : candidate_id,
                    'photo': photo,
                    'firstName' : first_name,
                    'lastName' : last_name,
                    'electionParties' : election_parties,
                    'electionStatus' : election_status,
                    'electionStage' : election_stage,
                    'electionDate': election_date,
                    'inOffice' : in_office,
                    'title' : title,
                    'officeParties' : office_parties,
                    'firstElect' : first_elect,
                    'lastElect' : last_elect,
                    'nextElect' : next_elect,
                    'termStart' : term_start,
                    'termEnd' : term_end,
                    'officeStatus' : office_status,
                    }

            # inject linked records conditionally (cannot be None)
            if election_district_id:
                candidate_record_obj['electionDistrictId'] = [ get_district_id(election_district_id) ]
            if election_state_id:
                candidate_record_obj['electionStateId'] =  [ get_state_id(election_state_id) ]
            if election_office_id:
                candidate_record_obj['electionOfficeId'] = [ get_office_id(election_office_id) ]
            if office_id:
                candidate_record_obj['officeId'] = [ get_office_id(office_id) ]
            if office_district_id:
                candidate_record_obj['officeDistrictId'] = [ get_district_id(office_district_id) ]
            if office_state_id:
                candidate_record_obj['officeStateId'] = [ get_state_id(office_state_id) ]
            
            # if female, write to database
            if is_female:
                print('inserting candidate record candidate: ' + str(candidate_id))
                candidates_table.insert(candidate_record_obj)


def candidate_address_seed():
    candidate_ids = get_candidate_ids()
    for candidate_id in candidate_ids:
        candidate_id_record = get_candidate_id(candidate_id)
        params = { 'candidateId' : candidate_id }
        r = get_request(candidate_address_url, params)
        root = ElementTree.fromstring(r.content)
        for address in root.iter('address'):
            address_type_id = address.find('webAddressTypeId').text
            address_type = address.find('webAddressType').text
            address = address.find('webAddress').text
            
            address_data_obj = {
                'candidateId' : [ candidate_id_record ],
                'webAddressTypeId' : address_type_id,
                'webAddressType' : address_type,
                'webAddress' : address,
            }
   
            print('inserting candidate address record for candidate: ' + str(candidate_id))
            addresses_table.insert(address_data_obj)

# CLEAN UP METHODS

def rating_categories_cleanup():
    ''' because votesmart provides multiple category / rating associations, run this to clean up rating_categories table '''

    all_rating_categories = rating_categories.get_all()
    print(all_rating_categories)
    uniques = dict()
    for record in all_rating_categories:
        airtable_id = record['id']
        record_id = record['fields']['id']
        rating_id = record['fields']['ratingId'][0]
        category_id = record['fields']['categoryId'][0]
        # determine uniqueness of record
        if rating_id not in uniques:
            uniques[rating_id] = [ category_id ]
        elif rating_id in uniques and category_id not in uniques[rating_id]:
            uniques[rating_id].append(category_id)
        else:
            # record not unique, delete from table
            print('deleting record from rating_categories table')
            rating_categories.delete(airtable_id)

def regular_update_prep():
    tables_to_delete = [elections_table, candidates_table, addresses_table, sigs_table, ratings_table, rating_categories, scores_table]
    for table in tables_to_delete: 
    record_ids = []
        records = table.get_all()
        for record in records:
            record_id = record['id']
            record_ids.append(election_id)
        table.batch_delete(record_ids)

# HOW TO SEED A NEW WOMAN UP DATABASE: 
# 1. change year and previous_year variables accordingly.
# 2. uncomment the below functions, as well as the functions below (for regular updates).
# 3. run 'python seed.py' in terminal.

# office_seed()
# print('OFFICE SEED DONE')
# state_seed()
# print('STATE SEED DONE')
# district_seed()
# print('DISTRICT SEED DONE')
# category_seed()
# print('CATEGORY SEED DONE')

# HOW TO REGULARLY UPDATE A WOMAN UP DATABASE:
# suggest running this script at least once per week to capture new elections and candidate information
# 1. uncomment the below functions.
# 2. run 'python seed.py' in terminal.
# regular_update_prep()
# print('TABLES CLEARED')
# election_seed()
# print('ELECTION SEED DONE')
# candidate_seed()
# print('CANDIDATE SEED DONE')
# candidate_address_seed()
# print('CANDIDATE ADDRESS SEED DONE')
# candidate_ratings_seed()
# print('CANDIDATE RATINGS SEED DONE')
# rating_categories_cleanup()
# print('CATEGORIES CLEANUP DONE')
# print('WOMAN UP SEEDING COMPLETE')


'''

TODO:
- clearing tables and reseeding for now, but when db is live we will want to update a record if it exists and insert if it doesn't
- start working with data in Angular from airtable and determine if you need any db structre refinements
- create table with summary stats (total # women, # running, # lost, # withdrawn, etc. ) and methods to write info as last step of seed.py

'''






