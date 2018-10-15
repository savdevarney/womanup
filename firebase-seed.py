import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

import os

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from xml.etree import ElementTree


vote_key = os.environ['VOTE_SMART_API_KEY']
fire_base_url = os.environ['FIREBASE_URL']
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
cred = credentials.Certificate("./serviceAccountKey.json")
app = firebase_admin.initialize_app(cred, {
    'databaseURL' : fire_base_url
})
db_root = db.reference()

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

            db_root.child('offices').push({
                'officeId' : office_id,
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
        db_root.child('office_types').push({
            'officeTypeId': row[0],
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
        db_root.child('states').push({'stateId': state_id, 'name': name})

def category_seed():
    r = requests.get(categories_url)
    root = ElementTree.fromstring(r.content)
    for category in root.iter('category'):
        category_id = category.find('categoryId').text
        category_name = category.find('name').text
        db_root.child('categories').push(
            {'categoryId': category_id,
             'name': category_name,
            })

def district_seed():
    states_snapshot = db.reference('/states').get()
    offices_snapshot = db.reference('/offices').get()
    for state_record, state in states_snapshot.items():
        for office_record, office in offices_snapshot.items():
            print('trying to find districts for officeId {0} in {1}'.format(office['officeId'], state['stateId']))
            params = { 'officeId' : office['officeId'], 'stateId' : state['stateId'] }
            r = get_request(districts_url, params)
            root = ElementTree.fromstring(r.content)
            for district in root.iter('district'):
                district_id = district.find('districtId').text
                district_name = district.find('name').text
                print('inserting record for {0} in state {1}'.format(district_name, state['stateId']))
                district_record = db_root.child('districts').push({
                    'districtId' : district_id,
                    'districtName' : district_name,
                    'offices' :  
                        { office_record: True },
                    'states' : 
                        { state_record: True } ,
                })
                district_record = district_record.key
                db_root.child('states/' + state_record + '/districts').update({
                    district_record: True
                })
                db_root.child('offices/' + office_record + '/districts').update({
                    district_record: True
                })

def election_seed():
    states_snapshot = db.reference('/states').get()
    for state_record, state in states_snapshot.items():
        print('getting elections for {0}'.format(state['stateId']))
        params = { 'stateId': state['stateId'], 'year': year }
        r = get_request(elections_state_year_url, params)
        root = ElementTree.fromstring(r.content)
        for election in root.iter('election'):
            election_id = election.find('electionId').text
            election_name = election.find('name').text
            office_type_id = election.find('officeTypeId').text
            office_type_query = db.reference('office_types').order_by_child('officeTypeId').equal_to(office_type_id)
            office_type_snapshot = office_type_query.get()
            office_type_record = list(office_type_snapshot.items())[0][0]
            print('inserting election record for election: {0}'.format(str(election_id)))
            election_record = db_root.child('elections').push({
                'electionId': election_id,
                'name': election_name,
                'states': {
                    state_record: True
                },
                'officeTypes': {
                    office_type_record: True
                }
            })
            election_record = election_record.key
            db_root.child('states/' + state_record + '/elections').update({
                election_record: True
            })
            db_root.child('office_types/' + office_type_record + '/elections').update({
                election_record: True
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
    elections_snapshot = db.reference('/elections').get()
    # iterate through all 2018 current elections
    for election_record, election in elections_snapshot.items():
        election_id = election['electionId']
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
                        election_record: True,
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
                    candidate_summary['female'][election_status] = 1
            else:
                candidate_summary['male']['total'] += 1
                if election_status in candidate_summary['male']['status']:
                    candidate_summary['male']['status'][election_status] += 1
                else:
                    candidate_summary['male']['status'][election_status] = 1

            print('inserting candidate record for candidate: ' + str(candidate_id))
            candidate_record_ref = db_root.child('candidates').push(candidate_record_obj)
            candidate_record = candidate_record_ref.key

            print('updated candidate summary:')
            print(candidate_summary)

            # inject linked records conditionally
            if election_district_id:
                district_query = db.reference('districts').order_by_child('districtId').equal_to(election_district_id)
                district_snapshot = district_query.get()
                district_record = list(district_snapshot.items())[0][0]
                candidate_record_obj['runningDistricts'] = {
                    district_record: True,
                }
                db_root.child('districts/' + district_record + '/runningCandidates').update({
                    candidate_record: True,
                 })
            if election_state_id:
                state_query = db.reference('states').order_by_child('stateId').equal_to(election_state_id)
                state_snapshot = state_query.get()
                state_record = list(state_snapshot.items())[0][0]
                candidate_record_obj['runningStates'] =  { 
                    state_record: True, 
                }
                db_root.child('states/' + state_record + '/runningCandidates').update({
                    candidate_record: True,
                })
            if election_office_id:
                office_query = db.reference('offices').order_by_child('officeId').equal_to(election_office_id)
                office_snapshot = office_query.get()
                office_record = list(office_snapshot.items())[0][0]
                candidate_record_obj['runningOffices'] = {
                    office_record: True,
                }
                db_root.child('offices/' + office_record + '/runningCandidates').update({
                    candidate_record: True,
                })
            if office_district_id:
                district_query = db.reference('districts').order_by_child('districtId').equal_to(office_district_id)
                district_snapshot = district_query.get()
                district_record = list(district_snapshot.items())[0][0]
                candidate_record_obj['electedDistricts'] = {
                    district_record: True,
                }
                db_root.child('districts/' + district_record + '/electedCandidates').update({
                    candidate_record: True,
                 })
            if office_state_id:
                state_query = db.reference('states').order_by_child('stateId').equal_to(office_state_id)
                state_snapshot = state_query.get()
                state_record = list(state_snapshot.items())[0][0]
                candidate_record_obj['electedStates'] =  { 
                    state_record: True, 
                }
                db_root.child('states/' + state_record + '/electedCandidates').update({
                    candidate_record: True,
                })
            if office_id:
                office_query = db.reference('offices').order_by_child('officeId').equal_to(office_id)
                office_snapshot = office_query.get()
                office_record = list(office_snapshot.items())[0][0]
                candidate_record_obj['electedOffices'] = {
                    office_record: True,
                }
                db_root.child('offices/' + office_record + '/electedCandidates').update({
                    candidate_record: True,
                })

            # Update the candidate record with any linked data
            candidate_record_ref.update(candidate_record_obj)

    # After the entire function runs, store the summary
    print('Candidate seed finished! Writing summary')
    db_root.child('summary').push(candidate_summary)        


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


# office_seed()
# office_type_seed()
# state_seed()
# category_seed()
# district_seed()

# election_seed()
# candidate_seed()