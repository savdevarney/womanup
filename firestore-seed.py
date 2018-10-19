import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import os

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from xml.etree import ElementTree

import google.cloud


vote_key = os.environ['VOTE_SMART_API_KEY']
fire_store_id = os.environ['FIRESTORE_ID']
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

# FIREBASE APP AND DB
cred = credentials.Certificate('./serviceAccountKey.json')
firebase_admin.initialize_app(cred, {
  'projectId': fire_store_id,
})
db = firestore.client()

# GENERIC SESSION REQUEST

def get_request(url, params=''):
    s = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    r = s.get(url, params=params)
    return r

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
            print('inserting office: {0}'.format(office_id))
            db.collection('offices').document(office_id).set({
                'officeTypeId' : office_type_id,
                'officeLevelId' : office_level_id,
                'officeBranchId' : office_branch_id,
                'officeName' : office_name,
            })

def office_type_seed():
    office_type_rows = [
        ['P', 'F', 'E', 'Presidential and Cabinet'],
        ['C', 'F', 'L', 'Congressional'],
        ['G', 'S', 'E', 'Governor and Cabinet'],
        ['S', 'S', 'E', 'Statewide'],
        ['K', 'S', 'J', 'State Judicial'],
        ['L', 'S', 'L', 'State Legislature'],
        ['J', 'F', 'J', 'Supreme Court'],
        ['M', 'L', 'E', 'Local Executive'],
        ['N', 'L', 'L', 'Local Legislative'],
        ['H', 'L', 'J', 'Local Judicial'],
    ]
    for row in office_type_rows:
        print('inserting office type: {0}'.format(row[0]))
        db.collection('office_types').document(row[0]).set({
            'officeLevelId': row[1],
            'officeBranchId': row[2],
            'officeName': row[3],
        })

def state_seed():
    r = requests.get(states_url)
    root = ElementTree.fromstring(r.content)
    for state in root.iter('state'):
        state_id = state.find('stateId').text
        name = state.find('name').text
        print('inserting state: {0}'.format(state_id))
        db.collection('states').document(state_id).set({'name': name})

def category_seed():
    r = requests.get(categories_url)
    root = ElementTree.fromstring(r.content)
    for category in root.iter('category'):
        category_id = category.find('categoryId').text
        category_name = category.find('name').text
        db.collection('categories').document(category_id).set(
            {
             'name': category_name,
            })

def district_seed():
    states = [snapshot.reference for snapshot in db.collection(u'states').get()]
    offices = [snapshot.reference for snapshot in db.collection(u'offices').get()]
    # use caches for associated data to save on writes when seeding
    state_district_cache = {}
    office_district_cache = {}
    for state in states:
        state_id = state.id
        for office in offices:
            office_id = office.id
            print('trying to find districts for officeId {0} in {1}'.format(office_id, state_id))
            params = { 'officeId' : office_id, 'stateId' : state_id }
            r = get_request(districts_url, params)
            root = ElementTree.fromstring(r.content)
            for district in root.iter('district'):
                print('found districts to insert')
                district_id = district.find('districtId').text
                district_name = district.find('name').text
                print('inserting record for {0} in state {1}'.format(district_id, state_id))
                db.collection('districts').document(district_id).set({
                    'districtName' : district_name,
                    'offices' :  
                        { office_id: True },
                    'states' : 
                        { state_id: True } ,
                })
                # populate the caches
                if state_id not in state_district_cache:
                    state_district_cache[state_id] = {}
                if district_id not in state_district_cache[state_id]:
                    state_district_cache[state_id][district_id] = True
                if office_id not in office_district_cache:
                    office_district_cache[office_id] = {}
                if district_id not in office_district_cache[office_id]:
                    office_district_cache[office_id][district_id] = True
    # write related data from caches
    for state, districts in state_district_cache.items():
        print('iterating through cache: state {0} and districts {1}'.format(state, districts))
        db.collection('states').document(state).set({
            'districts': districts
        })
    for office, districts in office_district_cache.items():
        print('iterating through cache: office {0} and districts {1}'.format(office, districts))
        db.collection('offices').document(office).set({
            'districts': districts
        })

def election_seed():
    states = [snapshot.reference for snapshot in db.collection('states').get()]
    for state in states:
        state_id = state.id
        print('getting elections for {0}'.format(state_id))
        params = { 'stateId': state_id, 'year': year }
        r = get_request(elections_state_year_url, params)
        root = ElementTree.fromstring(r.content)
        for election in root.iter('election'):
            election_id = election.find('electionId').text
            election_name = election.find('name').text
            office_type_id = election.find('officeTypeId').text
            print('inserting election record for election: {0}'.format(str(election_id)))
            db.collection('elections').document(election_id).set({
                'name': election_name,
                'states': {
                    state_id: True
                },
                'officeTypes': {
                    office_type_id: True
                }
            })
            db.collection('states').document(state_id).update({
                'elections.{0}'.format(election_id): True
            })
            db.collection('office_types').document(office_type_id).update({
                'elections.{0}'.format(election_id): True
            })

def candidate_seed():
    candidate_summary = {
        'total': 0,
        'female': {
            'total': 0,
            'status': {}
        },
        'male': {
            'total': 0,
            'status': {}
        }
    }
    elections = [snapshot.reference for snapshot in db.collection('elections').get()]
    # iterate through all 2018 current elections
    for election in elections:
        election_id = election.id
        print('getting candidates for electionId {0}'.format(election_id))
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

            # if office data, capture additional office data from bio
            office = root.find('office')
            in_office = 'true' if office else ''
            title = office.find('title').text if in_office else '' # 'Senator'
            first_elect = office.find('firstElect').text if in_office else ''
            last_elect = office.find('lastElect').text if in_office else ''
            next_elect = office.find('nextElect').text if in_office else '' # 2018
            term_start = office.find('termStart').text if in_office else '' # 11/10/1992
            term_end = office.find('termEnd').text if in_office else ''

            # prepare to write candidate info
            candidate_record_obj = {
                    'elections' : {
                        election_id: True,
                    },
                    'candidateId' : candidate_id,
                    'photo': photo,
                    'firstName' : first_name,
                    'lastName' : last_name,
                    'runningParties' : election_parties,
                    'runningStatus' : election_status,
                    'runningStage' : election_stage,
                    'runningDate': election_date,
                    'inOffice' : in_office,
                    'title' : title,
                    'electedParties' : office_parties,
                    'firstElect' : first_elect,
                    'lastElect' : last_elect,
                    'nextElect' : next_elect,
                    'termStart' : term_start,
                    'termEnd' : term_end,
                    'electedOfficeStatus' : office_status,
                    'isFemale' : is_female,
                    }
    
            # increment summary counter
            candidate_summary['total'] += 1
            if is_female:
                candidate_summary['female']['total'] += 1
                if election_status in candidate_summary['female']['status']:
                    candidate_summary['female']['status'][election_status] += 1
                else:
                    candidate_summary['female']['status'][election_status] = 1
            else:
                candidate_summary['male']['total'] += 1
                if election_status in candidate_summary['male']['status']:
                    candidate_summary['male']['status'][election_status] += 1
                else:
                    candidate_summary['male']['status'][election_status] = 1

            if is_female:
                print('inserting candidate record for candidate: {0}'.format(candidate_id))
                db.collection('candidates').document(candidate_id).set(candidate_record_obj)

                print('updated candidate summary:')
                print(candidate_summary)

                # inject linked records conditionally
                if election_district_id:
                    candidate_record_obj['runningDistricts'] = {
                        election_district_id: True,
                    }
                    db.collection('districts').document(election_district_id).update({
                        'runningCandidates.{0}'.format(candidate_id): True,
                    })
                if election_state_id:
                    candidate_record_obj['runningStates'] =  { 
                        election_state_id: True, 
                    }
                    db.collection('states').document(election_state_id).update({
                        'runningCandidates.{0}'.format(candidate_id): True,
                    })
                if election_office_id:
                    candidate_record_obj['runningOffices'] = {
                        election_office_id: True,
                    }
                    db.collection('offices').document(election_office_id).update({
                        'runningCandidates.{0}'.format(candidate_id): True,
                    })
                if office_district_id:
                    candidate_record_obj['electedDistricts'] = {
                        office_district_id: True,
                    }
                    db.collection('districts').document(office_district_id).update({
                        'electedCandidates.{0}'.format(candidate_id): True,
                    })
                if office_state_id:
                    candidate_record_obj['electedStates'] =  { 
                        office_state_id: True, 
                    }
                    db.collection('states').document(office_state_id).update({
                        'electedCandidates.{0}'.format(candidate_id): True,
                    })
                if office_id:
                    candidate_record_obj['electedOffices'] = {
                        office_id: True,
                    }
                    db.collection('offices').document(office_id).update({
                        'electedCandidates.{0}'.format(candidate_id): True,
                    })

                # update the candidate record with any linked data
                db.collection('candidates').document(candidate_id).update(candidate_record_obj)

                # get the web addresses info for candidate to store in addresses
                params = { 'candidateId' : candidate_id }
                r = get_request(candidate_address_url, params)
                root = ElementTree.fromstring(r.content)
                addresses = {'addresses': []}
                for address in root.iter('address'):
                    address_type_id = address.find('webAddressTypeId').text
                    address_type = address.find('webAddressType').text
                    address = address.find('webAddress').text
                    
                    addresses['addresses'].append({
                        'webAddressTypeId' : address_type_id,
                        'webAddressType' : address_type,
                        'webAddress' : address,
                    })
        
                    print('inserting candidate address record for candidate: {0}'.format(candidate_id))
                    db.collection('candidates').document(candidate_id).update(addresses)

    # After the entire function runs, store the summary
    print('Candidate seed finished! Writing summary')
    db.collection('stats').document('candidates').set(candidate_summary)        

def sig_seed(sig_id):
    params = {'sigId': sig_id}
    r = get_request(sig_url, params)
    root = ElementTree.fromstring(r.content)
    sig_id = root.find('sigId').text
    state_id = root.find('stateId').text
    name = root.find('name').text
    description = root.find('description').text
    url = root.find('url').text
    
    print('inserting sig record for ' + sig_id)
    db.collection('sigs').document(sig_id).set({
        'states': {
            state_id: True
        },
        'name': name,
        'description': description,
        'url': url,
    })
    db.collection('states').document(state_id).update({
        'sigs.{0}'.format(sig_id): True
    })

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
            
            print('inserting rating record for {0}'.format(rating_id))
            db.collection('ratings').document(rating_id).set({
                'timespan': time,
                'ratingName': name,
                'ratingText': text,
                'sigs': {
                    sig_id: True,
                }
            })
            db.collection('sigs').document(sig_id).update({
                'ratings.{0}'.format(rating_id): True
            })
        
def candidate_ratings_seed():
    candidates = [snapshot.reference for snapshot in db.collection('candidates').get()]
    # local sigs and ratings caches so don't continue to write data for unique sigs / ratings
    sigs = {}
    ratings = {}
    for candidate in candidates:
        candidate_id = candidate.id
        print('getting ratings for candidate: {0}'.format(candidate_id))
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
                # update local stores so don't repeat seeding
                if previous_year in time or year in time:
                    # if it's a new sig, seed the information
                    if sig_id not in sigs:
                        sigs[sig_id] = True
                        sig_seed(sig_id)

                    # if it's a new rating, seed the information
                    if rating_id not in ratings:
                        ratings[rating_id] = True
                        rating_seed(sig_id)
                        
                        # also seed the categories associated with the new rating
                        # note, there are several duplicate categories for a single rating in the vote smart data
                        # will have to clean up in our database ... 
                        categories = rating.find('categories')
                        for category in categories.iter('category'):
                            category_id = category.find('categoryId').text
                            try:
                                db.collection('categories').document(category_id).update({
                                    'sigs.{0}'.format(sig_id): True,
                                    'ratings.{0}'.format(rating_id): True,
                                })
                            except google.cloud.exceptions.NotFound:
                                db.collection('categories').document(category_id).set({
                                    'notInVoteSmart': True,
                                    'sigs.{0}'.format(sig_id): True,
                                    'ratings.{0}'.format(rating_id): True,
                                })
                            db.collection('sigs').document(sig_id).update({
                                'categories.{0}'.format(category_id): True,
                            })
                            db.collection('ratings').document(rating_id).update({
                                'categories.{0}'.format(category_id): True,
                            })
                        
                    print('inserting candidate rating score for rating: {0} and candidate: {1}'.format(rating_id, candidate_id))
                    # write the candidate's rating info to scores table
                    score_id = candidate_id + rating_id
                    score_ref = db.collection('scores').document(score_id).set({
                        'ratings': {
                            rating_id: True
                        },
                        'sigs': {
                            sig_id: True
                        },
                        'candidates': {
                            candidate_id: True
                        },
                        'score': score,
                        'name': name,
                        'text': text,
                    })

                    db.collection('ratings').document(rating_id).update({
                        'scores.{0}'.format(score_id): True
                    })
                    db.collection('sigs').document(sig_id).update({
                        'scores.{0}'.format(score_id): True
                    })
                    db.collection('candidates').document(candidate_id).update({
                        'scores.{0}'.format(score_id): True
                    })

# office_type_seed()
# state_seed()
# office_seed()
# district_seed()

# election_seed()
# candidate_seed()

# category_seed()
# candidate_ratings_seed()

print('WOMANUP DB SEEDING COMPLETE!')