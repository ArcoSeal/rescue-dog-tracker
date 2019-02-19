import os
from urllib.parse import urljoin
import datetime
import logging

import requests
from bs4 import BeautifulSoup

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BATTERSEA_API_URL = 'https://www.battersea.org.uk/api/animals/dogs'

DT_BASEURL = 'https://www.dogstrust.org.uk/'
DT_CENTRES = {'Basildon': '/rehoming/dogs/filters/ess~~~~~n~',
                 'Harefield': '/rehoming/dogs/filters/har~~~~~n~'}

P4H_BASEURL = 'https://www.pets4homes.co.uk/responsive_search_pets.php?page={page}&type_id=3&advert_type={advert_type}&location={search_location}&distance={search_distance_miles}&maxprice={search_maxprice}&results=20&sort=creatednew'

class Dog:
    def __init__(self, dogid, owner, location, name, breed, status, url):
        self.dogid = dogid
        self.owner = owner
        self.location = location
        self.name = name
        self.breed = breed if breed else 'Unknown'
        self.status = status
        self.url = url

    def __str__(self):
        return str(vars(self))

    @classmethod
    def from_bdhjson(cls, battersea_data):
        dogid = 'bdh' + str(battersea_data['nid'])
        owner = 'BDH'
        location = battersea_data['field_animal_centre'].title()
        name = battersea_data['title']
        breed = battersea_data['field_animal_breed'].title()

        if battersea_data['field_animal_rehomed'].lower() == 'rehomed':
            status = 'Rehomed'
        elif battersea_data['field_animal_reserved'].lower() == 'reserved':
            status = 'Reserved'
        elif battersea_data['field_animal_rehomed'].lower() == '' and battersea_data['field_animal_reserved'].lower() == '':
            status = 'Available'
        else:
            status = 'Unknown'

        url = urljoin('https://www.battersea.org.uk', battersea_data['path'])

        return cls(dogid, owner, location, name, breed, status, url)

def _test_init_table(table):
    import json

    with open('dogdict_old.json', 'r') as infile: dogdict_old = json.load(infile)
    dogs_old = [Dog.from_bdhjson(ii) for ii in dogdict_old.values()]

    response = table.scan(ProjectionExpression='dogid')
    dogs_table = response['Items']
    for dogid in dogs_table:
        try:
            response = table.delete_item(Key=dogid)
        except:
            raise

    for dog in dogs_old:
        try:
            response = table.put_item(Item=dog.__dict__)
        except:
            raise

    print('Test initialised table with {} dogs'.format(len(dogs_old)))

def getdogs_bdh():
    try:
        resp = requests.get(BATTERSEA_API_URL)
        dogs = [Dog.from_bdhjson(ii) for ii in resp.json()['animals'].values()]
    except:
        logger.error('Failed to retrieve BDH dogs')
        return []
    else:
        logger.info('Got {} dogs from Battersea Dogs Home'.format(len(dogs)))
        return dogs

def getdogs_dt():
    def parse_dt_dog_html(dog_html):
        name = dog_html.find('h3').text.strip()
        breed = dog_html.find_all('span')[0].text.strip()
        if breed.lower() == 'a crossbreed': breed = 'Crossbreed'
        status = 'Reserved' if dog_html.find('div', class_='label label--reserved') else 'Available'
        url = urljoin(DT_BASEURL, dog_html['href'])
        dogid = 'dt' + url.split('/')[-2]

        return dogid, name, breed, status, url

    try:
        dogs = []
        for centre_name in DT_CENTRES:
            centre_url = urljoin(DT_BASEURL, DT_CENTRES[centre_name])
            resp = requests.get(centre_url)
            soup = BeautifulSoup(resp.text, 'html.parser')

            pageurls = soup.find('ul', attrs={'id': 'BodyContent_DogList1_ulPagination'}).find_all('a')
            pageurls = [urljoin(DT_BASEURL, ii['href']) for ii in pageurls]

            for ii in pageurls:
                resp = requests.get(ii)
                soup = BeautifulSoup(resp.text, 'html.parser')

                for dog_html in soup.find_all('a', class_='grid__element', id=lambda x: x and x.startswith('BodyContent_DogList')):
                    try:
                        dogid, name, breed, status, url = parse_dt_dog_html(dog_html)
                    except Exception as e:
                        logger.warning('Error parsing DT dog: {} (Raw HTML: {})'.format(e, dog_html))
                    else:
                        owner = 'DT'
                        location = centre_name
                        dogs.append(Dog(dogid, owner, location, name, breed, status, url))

    except:
        logger.error('Failed to retrieve DT dogs')
        return []

    else:
        logger.info('Got {} dogs from Dogs Trust'.format(len(dogs)))
        return dogs

def getdogs_p4h(search_location, search_distance_miles, search_maxprice, title_filters=[], owner_filters=[]):
    def parse_p4h_dog_html(dog_html):
        categories = dog_html.find('div', class_='categories hidden-xs hidden-sm').find_all('a')
        
        url = dog_html.find('h2', class_='headline').a.get('href')
        dogid = 'p4h' + url.split('/')[-1].split('-')[0]
        owner = 'P4H/' + dog_html.find('div', class_='username').a.text
        location = categories[3].text
        name = dog_html.find('h2', class_='headline').text
        breed = categories[2].text

        return dogid, owner, location, name, breed, url

    try:
        dogs = []
        
        for advert_type in (1,2): # 1 -> For Sale, 2 -> For Adoption
            pagenums = [1]
            current_page = 0

            while current_page < max(pagenums):
                current_page += 1

                resp = requests.get(P4H_BASEURL.format(advert_type=advert_type,
                                                        page=current_page,
                                                        search_location=search_location,
                                                        search_distance_miles=search_distance_miles,
                                                        search_maxprice=search_maxprice))
                soup = BeautifulSoup(resp.text, 'html.parser')

                if current_page == 1: # if this is our first page load, get the other page numbers that we'll need to load afterwards
                    extrapages = soup.find('div', class_='paginate pull-right ').find_all('a', class_='paginate')
                    extrapages = [int(ii.text) for ii in extrapages]
                    pagenums.extend(extrapages)
                
                for dog_html in soup.find_all('div', class_='col-xs-12 profilelisting'):
                    try:
                        dogid, owner, location, name, breed, url = parse_p4h_dog_html(dog_html)
                    except Exception as e:
                        logger.warning('Error parsing P4H dog: {} (Raw HTML: {})'.format(e, dog_html))
                    else:
                        status = 'Available'
                        if not any([ii.lower() in name.lower() for ii in title_filters]) and not any([ii.lower() in owner.lower() for ii in owner_filters]):
                            dogs.append(Dog(dogid, owner, location, name, breed, status, url))

    except:
        logger.error('Failed to retrieve P4H dogs')
        return []

    else:
        logger.info('Got {} dogs from Pets4Homes'.format(len(dogs)))
        return dogs     

def removedogs(table, dogids):
    for dogid in dogids:
        try:
            response = table.delete_item(Key={'dogid': dogid})
        except ClientError as e:
            logger.warning('Failed to delete: {}'.format(dogid))
            logger.warning(e.response['Error']['Message'])
        else:
            logger.info('Deleted: {}'.format(dogid))

def adddogs(table, dogs):
    for dog in dogs:
        try:
            response = table.put_item(Item=dog.__dict__)
        except ClientError as e:
            logger.warning('Failed to add: {}/{} ({})'.format(dog.dogid, dog.name, dog.url))
            logger.warning(e.response['Error']['Message'])
        else:
            logger.info('Added: {} ({})'.format(dog.dogid, dog.name))

def updatedogs(table, dogs):
    for dog in dogs:
        try:
            response = table.update_item(
                Key={'dogid': dog.dogid},
                UpdateExpression='SET #st = :s',
                ExpressionAttributeNames={'#st': 'status'},
                ExpressionAttributeValues={':s': dog.status})
        except ClientError as e:
            logger.warning('Failed to update: {}/{} ({})'.format(dog.dogid, dog.name, dog.url))
            logger.warning(e.response['Error']['Message'])
        else:
            logger.info('Updated: {} ({})'.format(dog.dogid, dog.name))

def generate_sns_message(dogs_curr, dogs_prev, dogids_added, dogids_removed, dogids_statuschange):
    newdogs = 'New Dogs:\n{}\n'.format('\n'.join(['{name} ({breed} @ {owner}/{location}): {url}'.format(
                    name=dogs_curr[dogid].name,
                    breed=dogs_curr[dogid].breed,
                    owner=dogs_curr[dogid].owner,
                    location=dogs_curr[dogid].location,
                    url=dogs_curr[dogid].url) for dogid in dogids_added]))

    statuschanges = 'Status changes:\n{}\n'.format('\n'.join(['{name} ({oldstatus} -> {newstatus}): {url}'.format(
                    name=dogs_curr[dogid].name,
                    oldstatus=dogs_prev[dogid]['status'],
                    newstatus=dogs_curr[dogid].status,
                    url=dogs_curr[dogid].url) for dogid in dogids_statuschange]))

    removeddogs = 'Removed dogs:\n{}\n'.format('\n'.join(['{name}'.format(
                    name=dogs_prev[dogid]['name']) for dogid in dogids_removed]))

    sns_message = 'Rescue Dog Update @ {}\n'.format(datetime.datetime.now()) \
                    + (newdogs if dogids_added else '') \
                    + (statuschanges if dogids_statuschange else '') \
                    + (removeddogs if dogids_removed else '') \

    return sns_message

def main(dbname, snstopicarn, p4h_search_location, p4h_search_distance_miles, p4h_search_maxprice, p4h_title_filters=[], p4h_owner_filters=[]):
    dogs_bdh = getdogs_bdh()
    dogs_dt = getdogs_dt()
    dogs_p4h = getdogs_p4h(p4h_search_location, p4h_search_distance_miles, p4h_search_maxprice, p4h_title_filters, p4h_owner_filters)

    dogs_curr = dogs_bdh + dogs_dt + dogs_p4h
    dogs_curr = {dog.dogid: dog for dog in dogs_curr}

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dbname)
    
    response = table.scan(
        ProjectionExpression='dogid, #nm, #st',
        ExpressionAttributeNames={'#nm': 'name', '#st': 'status'})
    dogs_prev = {ii['dogid'] : ii for ii in response['Items']}

    dogids_added, dogids_removed = dogs_curr.keys() - dogs_prev.keys(), dogs_prev.keys() - dogs_curr.keys()
    dogids_common = dogs_curr.keys() & dogs_prev.keys()

    dogids_statuschange = [dogid for dogid in dogs_curr if (dogid in dogids_common) and (dogs_curr[dogid].status != dogs_prev[dogid]['status'])]
    dogids_statuschange_available = [dogid for dogid in dogids_statuschange if dogs_curr[dogid].status == 'Available']

    removedogs(table, dogids_removed)
    adddogs(table, [dogs_curr[ii] for ii in dogids_added])
    updatedogs(table, [dogs_curr[ii] for ii in dogids_statuschange])

    if __name__ == '__main__':
        print('Dogs added: {}'.format(', '.join([dogs_curr[dogid].name for dogid in dogids_added])))
        print('Dogs removed: {}'.format(', '.join([dogs_prev[dogid]['name'] for dogid in dogids_removed])))
        print('\nStatus changes\n--------------')
        print('\n'.join(['{}: {} -> {}'.format(dogs_curr[dogid].name, dogs_prev[dogid]['status'], dogs_curr[dogid].status) for dogid in dogids_statuschange]))

    else:
        if sum([len(ii) for ii in (dogids_added, dogids_statuschange_available)]) > 0:
            sns = boto3.client('sns')
            response = sns.publish(
                TopicArn=snstopicarn,
                Subject='Rescue Dog Update ({})'.format(datetime.datetime.now()),
                Message=generate_sns_message(dogs_curr, dogs_prev, dogids_added, dogids_removed=[], dogids_statuschange=dogids_statuschange_available))
            logger.info('Notification sent to topic: {}'.format(snstopicarn))

def lambda_handler(event, context):
    dbname, snstopicarn = os.environ['dbname'], os.environ['snstopicarn']
    
    p4h_search_location, p4h_search_distance_miles, p4h_search_maxprice = os.environ['p4h_search_location'], os.environ['p4h_search_distance_miles'], os.environ['p4h_search_maxprice']
    p4h_title_filters, p4h_owner_filters = [ii.strip() for ii in os.environ['p4h_title_filters'].split(',')], [ii.strip() for ii in os.environ['p4h_owner_filters'].split(',')]
    
    main(dbname, snstopicarn, p4h_search_location, p4h_search_distance_miles, p4h_search_maxprice, p4h_title_filters, p4h_owner_filters)

if __name__ == '__main__':
    # for running manually
    main(dbname='rescue-dog-tracker',
            snstopicarn='', # fill me!
            p4h_search_location='london',
            p4h_search_distance_miles=30,
            p4h_search_maxprice=300)