import os, random, json, re, csv, itertools, datetime
from faker import Faker
from PIL import Image, ImageDraw, ImageFont, ImageOps
from google.cloud import storage, bigquery
from dataclasses import dataclass, field, astuple, asdict, InitVar
from typing import Any
import img2pdf

#Faker instance
fake = Faker(['en-US', 'en_US', 'en_US', 'en-US'])

#Initialize the GCP clients for BQ & Storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ".credentials/vision_gcloud.json"

bq_client = bigquery.Client()
storage_client = storage.Client()
# storage_bucket = 'looker-dat-vision'
storage_bucket = 'gov_portal'

#Valid CA Zipcodes and populations
ca_zipcodes = json.load(open('misc/ca.json'))
#Valid outside CA zips
_tmp_file = open('misc/uszips.csv')
_tmp = csv.reader(_tmp_file)
us_ex_ca_zipcodes = []
next(_tmp)
for row in _tmp:
    us_ex_ca_zipcodes.append(row)

class CustomJSONEncoder(json.JSONEncoder):
    # overload method default
    def default(self, obj):
        # Match all the types you want to handle in your converter
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        if len(str(obj)) == 5 and re.fullmatch(r'/d{5}',str(obj)):
            return str(obj)
        if obj in ('F','M'):
            return obj
        # Call the default method for other types
        return json.JSONEncoder.default(self, obj)

applications = list()
documents = list()
account_events = list()
cases = list()
case_events = list()
payments = list()

govportal_employees = {
    'ron@govportal.io':2,
    'sal@govportal.io':1,
    'anil@govportal.io':1,
    'desmond@govportal.io':1,
    'salma@govportal.io':2,
    'tess@govportal.io':2,
    'max@govportal.io':1,
    'susan@govportal.io':1
}


# @dataclass
class Address(object):
    def __init__(self, mode=''):
        if mode == 'outside_ca':
            state_zip = random.choice(us_ex_ca_zipcodes)
            self.address = fake.street_address()
            self.city = state_zip[1]
            self.state = state_zip[2]
            self.zip = state_zip[0]
            self.full = f'{self.address} \n{self.city}, {self.state} {self.zip}'
        else:
            self.address = fake.street_address()
            tmp = random.choices(ca_zipcodes,weights=list(i["pop"] for i in ca_zipcodes))
            self.city = tmp[0]['city']
            self.state = 'CA'
            self.zip = tmp[0]['zip']
            self.full = f'{self.address} \n{self.city}, {self.state} {self.zip}'
    def __hash__(self):
        return hash(self.full)


dupes = {
         'phone': {fake.phone_number():[]}
        ,'email': {fake.email():[]}
        ,'mail_address': {Address():[]}
        ,'home_address': {Address():[]}
        ,'ipv4': {fake.ipv4():[]}
}
def set_dupe(type,val,person_id):
    if val not in dupes[type].keys():
        dupes[type][val] = [person_id]
    else:
        dupes[type][val].append(person_id)

def get_dupe(type,person_id):
    selection = random.choice(list(dupes[type].keys()))
    dupes[type][selection].append(person_id)
    return selection

@dataclass
class Application(object):
    application_id:int = field(default_factory=itertools.count().__next__)
    person_id:int = field(init=False)
    created_datetime:datetime = field(default_factory=lambda:fake.date_time_between_dates(datetime.datetime(2019,1,1), datetime.datetime.now()))
    previous_employer:str = field(default_factory=lambda:fake.company())
    previous_income:int = field(default_factory=lambda:random.randint(500,8500))
    language:str = field(init=False)
    ip_address:str = field(default_factory=lambda:fake.ipv4())
    person:InitVar[Any] = None

    def __post_init__(self, person=None):
        self.person_id = person.person_id
        self.language = person.language

@dataclass
class Document(object):
    document_id:int = field(default_factory=itertools.count().__next__)
    person_id:int = field(init=False)
    type:str = field(init=False)
    location:str = field(init=False)
    _type:InitVar[str] = ''
    person:InitVar[Any] = None

    def __post_init__(self, _type='', person=None):
        self.type = _type
        self.person_id = person.person_id
        if self.type == 'drivers_license':
            self.location = generate_drivers_license(person)
        elif self.type == 'w2':
            self.location = generate_w2(person)
        elif self.type == '1099':
            self.location = generate_1099(person)

@dataclass
class Payment:
    payment_id:int = field(default_factory=itertools.count().__next__)	
    person_id:int = field(init=False)
    date:date = field(init=False)
    payment_number:int = field(init=False)
    amount:float = field(init=False)
    status:str = field(init=False)
    amount_total:float = 0
    person:InitVar[Any] = None
    pn:InitVar[int] = 0
    amt:InitVar[int] = 0

    def __post_init__(self, person=None, pn=0, amt=0):
        self.person_id = person.person_id
        self.date = person.application.created_datetime + datetime.timedelta(days=(pn*30))
        self.payment_number = pn
        self.amount = amt
        self.status = 'active'

@dataclass
class Person:
    person_id:int = field(default_factory=itertools.count().__next__)
    gender:str = field(init=False)
    last_name:str = field(init=False)
    first_name:str = field(init=False)
    name:str = field(init=False)
    ssn:str = field(default_factory=lambda:fake.ssn())
    date_of_birth:datetime = field(default_factory=lambda:fake.date_of_birth(minimum_age=18, maximum_age=70))
    phone_number:str = field(default_factory=lambda:fake.phone_number())
    email_address:str = field(default_factory=lambda:fake.email())
    home_address:str = field(init=False)
    home_city:str = field(init=False)
    home_state:str = field(init=False)
    home_zip:str = field(init=False)
    mail_address:str = field(init=False)
    mail_city:str = field(init=False)
    mail_state:str = field(init=False)
    mail_zip:str = field(init=False)
    language:str = ['english','english','english','espanol','other'][random.randint(0,4)]

    def __post_init__(self):
        self.gender = ['M','F'][random.randint(0,1)]
        if self.gender == 'M':
            self.first_name = fake.first_name_male()
        else:
            self.first_name = fake.first_name_female()
        self.last_name = fake.last_name()
        self.name = self.first_name + ' ' + self.last_name
        #generate application
        self.application = Application(person=self)
        applications.append(asdict(self.application))


        email_probability = random.randint(1,10)
        if email_probability == 1:
            self.email_address = get_dupe('email',self.person_id)
            cases.append(asdict(Case(person=self,error_code='duplicate email')))
        elif email_probability == 2:
            self.email_address = fake.email()
            set_dupe('email',self.email_address,self.person_id)
        else:
            self.email_address = fake.email()

        address_probability = random.randint(1,10)
        if address_probability == 1:
            self._home_address = get_dupe('home_address',self.person_id)
            if random.randint(1,10) < 9:
                cases.append(asdict(Case(person=self,error_code='duplicate home address')))
        elif address_probability == 2:
            self._home_address = Address()
            set_dupe('home_address',self._home_address,self.person_id)
        else:
            self._home_address = Address()
        self.home_address = self._home_address.address
        self.home_city = self._home_address.city
        self.home_state = self._home_address.state
        self.home_zip = self._home_address.zip
        
        if random.randint(1,100) <= 20:
            self._mail_address = Address(mode='outside_ca')
            cases.append(asdict(Case(person=self,error_code='out of state mail address')))
        else:
            if random.randint(1,100) <= 5:
                self._mail_address = self._home_address
                set_dupe('mail_address',self._home_address,self.person_id)
            else:
                self._mail_address = self._home_address

        self.mail_address = self._mail_address.address
        self.mail_city = self._mail_address.city
        self.mail_state = self._mail_address.state
        self.mail_zip = self._mail_address.zip

        if random.randint(1,100) <= 3:
            self._dl_address = Address()
        else:
            self._dl_address = self._home_address

        #generate documents
        self.drivers_license = Document(person=self, _type='drivers_license')
        documents.append(asdict(self.drivers_license))
        self._1099 = Document(person=self, _type='1099')
        documents.append(asdict(self._1099))
        self.w2 = Document(person=self, _type='w2')
        documents.append(asdict(self.w2))
        #temporary mock
        # self.drivers_license = None
        # documents.append({'document_id':1,'person_id':self.person_id,'location':'foo.jpg','type':'drivers_license'})
        # self._1099 = None
        # documents.append({'document_id':1,'person_id':self.person_id,'location':'foo.jpg','type':'1099'})
        # self.w2 = None
        # documents.append({'document_id':1,'person_id':self.person_id,'location':'foo.jpg','type':'w2'})


        #generate a random number of events
        for i in range(random.randint(1,35)):
            account_events.append(asdict(AccountEvent(person=self)))
        #calculate # of payments
        month_diff = int((datetime.datetime.now() - self.application.created_datetime).days/30)
        #benefit amount
        benefit_amount = int(self.application.previous_income * (random.randint(80,120)/100))
        for i in range(month_diff):
            payments.append(asdict(Payment(person=self,pn=i, amt=benefit_amount)))

@dataclass
class AccountEvent(object):
    event_id:int = field(default_factory=itertools.count().__next__)
    person_id:int = field(init=False)
    datetime:datetime = field(init=False)
    type:str = field(default_factory=lambda: random.choices(['login','submit_application','name_update','address_update','email_update'],weights=[10,1,1,1,1],k=1)[0])
    context:str = field(init=False)
    person:InitVar[Any] = None

    def __post_init__(self, person=None):
        self.person_id = person.person_id
        if random.randint(0,10) < 3:
            self.ip_address = fake.ipv4()
        else:
            self.ip_address = person.application.ip_address
        if self.type == 'login':
            self.datetime = fake.date_time_between_dates(person.application.created_datetime, datetime.datetime.now())
            self.context = f'''{{
                "user_agent":"{fake.user_agent()}",
                "time":"{self.datetime.isoformat()}",
                "ip_address":"{self.ip_address}",
                "person_id":"{person.person_id}",
                "type":"login"
            }}'''
        elif self.type == 'submit_application':
            self.datetime = person.application.created_datetime
            self.context = f'''{{
                "user_agent":"{fake.user_agent()}",
                "time":"{self.datetime.isoformat()}",
                "ip_address":"{person.application.ip_address}",
                "person_id":"{person.person_id}",
                "type":"login"
            }}'''
        elif self.type == 'name_update':
            self.datetime = fake.date_time_between_dates(person.application.created_datetime, datetime.datetime.now())
            self.context = f'''{{
                "user_agent":"{fake.user_agent()}",
                "time":"{self.datetime.isoformat()}",
                "ip_address":"{self.ip_address}",
                "person_id":"{person.person_id}",
                "type":"name_update",
                "old_value":"{fake.name()}",
                "new_value":"{person.name}"
            }}'''
        elif self.type == 'address_update':
            self.datetime = fake.date_time_between_dates(person.application.created_datetime, datetime.datetime.now())
            self.context = f'''{{
                "user_agent":"{fake.user_agent()}",
                "time":"{self.datetime.isoformat()}",
                "ip_address":"{self.ip_address}",
                "person_id":"{person.person_id}",
                "type":"address_update",
                "old_value":"{fake.address()}",
                "new_value":"{person._home_address.full}"
            }}'''
        elif self.type == 'email_update':
            self.datetime = fake.date_time_between_dates(person.application.created_datetime, datetime.datetime.now())
            self.context = f'''{{
                "user_agent":"{fake.user_agent()}",
                "time":"{self.datetime.isoformat()}",
                "ip_address":"{self.ip_address}",
                "person_id":"{person.person_id}",
                "type":"address_update",
                "old_value":"{fake.email()}",
                "new_value":"{person.email_address}"
            }}'''

@dataclass
class Case(object):
    case_id:int = field(default_factory=itertools.count().__next__)
    application_id:int = field(init=False)
    person_id:int = field(init=False)	
    opened_datetime:datetime = field(init=False)
    closed_datetime:datetime = field(init=False)
    flag_reason_code:str = field(init=False)
    human_reason_code:str = field(init=False)
    status:str = field(init=False)
    judgement:str = field(init=False)
    fips_score:float = field(init=False)
    opened_by:str = field(init=False)
    person:InitVar[Any] = None
    error_code:InitVar[str] = None

    def __post_init__(self, person=None, error_code=''):
        day_diff = int((datetime.datetime.now() - person.application.created_datetime).days)
        self.opened_datetime = person.application.created_datetime + datetime.timedelta(days=random.randint(1,day_diff))
        self.status = 'open'
        self.application_id = person.application.application_id
        self.person_id = person.person_id
        self.closed_datetime = None
        self.judgement = ''
        self.fips_score = 0
        case_events.append(asdict(CaseEvent(case=self, _notes='Opened',dt=self.opened_datetime)))
        #20% of the cases were discovered by govportal employees, 80% AI
        if random.randint(1,10) <= 2:
            self.opened_by = random.choices(list(govportal_employees.keys()),weights=list(govportal_employees.values()),k=1)[0]
            self.flag_reason_code = ''
            self.human_reason_code = error_code
        else:
            self.opened_by = 'AI'
            self.flag_reason_code = error_code
            self.human_reason_code = ''

        #30% of cases are already closed
        if random.randint(1,10) <= 3:
            self.status = 'closed'
            time_since_open = int((datetime.datetime.now() - self.opened_datetime).days)
            if time_since_open <=1:
                time_since_open = 2
            self.closed_datetime = self.opened_datetime + datetime.timedelta(days=random.randint(1,time_since_open))
            if random.randint(1,10) <= 3:
                self.judgement = 'not_fraud'
                case_events.append(asdict(CaseEvent(case=self, _notes='Adjudicated not fradulent',dt=self.closed_datetime)))
            else:
                self.judgement = 'fraud'
                case_events.append(asdict(CaseEvent(case=self, _notes='Adjudicated fradulent', dt=self.closed_datetime)))

@dataclass
class CaseEvent:
    case_event_id:int = field(default_factory=itertools.count().__next__)
    case_id:int = field(init=False)
    type:str = field(init=False)
    notes:str = field(init=False)	
    datetime:datetime = field(init=False)
    opened_by:str = field(init=False)
    case:InitVar[Any] = None
    _notes:InitVar[Any] = ''
    dt:InitVar[Any] = ''
    # _datetime:InitVar[datetime] = None

    # def __post_init__(self, case=None, _notes='', _datetime=None):
    def __post_init__(self, case=None, _notes='', dt=''):
        self.case_id = case.case_id
        self.type = ''
        self.notes = ''
        time_since_open = int((datetime.datetime.now() - case.opened_datetime).days)
        #protects against empty range
        if time_since_open <= 1:
            time_since_open = 3

        self.datetime = case.opened_datetime + datetime.timedelta(days=random.randint(1,time_since_open))
        self.opened_by = random.choices(list(govportal_employees.keys()),weights=list(govportal_employees.values()),k=1)[0]
        if dt:
            self.datetime = dt
        if _notes:
            self.notes = _notes

def upload_to_storage(source_file_name,public=False):
    """Uploads a file to the bucket."""
    bucket = storage_client.bucket(storage_bucket)
    destination_blob_name = source_file_name.split('/')[-1]
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    if public:
        blob.make_public()
    return blob.public_url

def generate_drivers_license(person):
    image = Image.open('base_images/license.jpg')
    # folder = 'pixelated'
    folder = 'dl_photos'
    photo_array = {
        "M": [f'{folder}/bryce_hetzel.jpg',f'{folder}/peter_whitehead.jpg',f'{folder}/bryan_weber.jpg',
              f'{folder}/russell_garner.jpg',f'{folder}/alex_burch.jpg',f'{folder}/justin_pao.jpg',
              f'{folder}/skyler_shasky.jpg',f'{folder}/greg_sanders.jpg',f'{folder}/prasad_pagade.jpg',
              f'{folder}/alex_christiansen.jpg',f'{folder}/anmol_singh.jpg',f'{folder}/anthony_billet.jpg',
              f'{folder}/aaron_wilkowitz.jpg',f'{folder}/elliot_glasenk.jpg',f'{folder}/alick_zhang.jpg'],
        "F": [f'{folder}/leigha_jarret.jpg',f'{folder}/deirdre_reilly.jpg',f'{folder}/ayala_mansky.jpg']
    }
    img_src = photo_array[person.gender][random.randint(0,len(photo_array[person.gender])-1)]

    pic = Image.open(img_src)

    mywidth = 279
    myheight = 368
    hpercent = (myheight/float(pic.size[1]))
    wsize = int((float(pic.size[0])*float(hpercent)))
    pic = pic.resize((wsize,myheight), Image.ANTIALIAS)
    border_redux = int((wsize - mywidth)/2)
    border = (border_redux,0,border_redux,0)
    cropped_pic = ImageOps.crop(pic, border)
    image.paste(cropped_pic,(145,138))
    cropped_pic = cropped_pic.resize((106,144), Image.ANTIALIAS).convert('L')
    image.paste(cropped_pic,(696,377))

    coordinates =  {
         'First Name': (485,295)
        ,'Last Name':  (485, 255)
        ,'Sex': (600,501)
        ,'Height': (600,526)
        ,'Hair': (747,501)
        ,'Weight': (745,526)
        ,'Eye Color': (923,501)
        ,'Issued': (905,567)
        ,'Address': (447,330)
        ,'Signature': (165,520)
        ,'Picture': {
            'upper left corner': (145,158)
            ,'lower left corner': (145,526)
            ,'upper right corner': (424,158)
            ,'lower right corner': (424,526)
        }
    }
    #font Setup
    mainFont = ImageFont.truetype('fonts/PublicSans-Regular.ttf', 22)
    mainSmall = ImageFont.truetype('fonts/PublicSans-Regular.ttf', 20)
    mainBold = ImageFont.truetype('fonts/PublicSans-Bold.ttf', 30)
    mainBoldSmall = ImageFont.truetype('fonts/PublicSans-Bold.ttf', 22)
    sigFonts = [
         ImageFont.truetype('fonts/CedarvilleCursive-Regular.ttf', 40)
        ,ImageFont.truetype('fonts/Creattion Demo.otf', 60)
        ,ImageFont.truetype('fonts/Allura-Regular.ttf', 40)
        # ,ImageFont.truetype('fonts/Autumn in November.ttf', 40)
        ]
    sigFont = random.randint(0,len(sigFonts)-1)
    sigFont = sigFonts[sigFont]

    d1 = ImageDraw.Draw(image)
    d1.text(coordinates['First Name'], person.first_name.upper(), font=mainBold, fill =(0, 0, 0))
    d1.text(coordinates['Last Name'], person.last_name.upper(), font=mainBold, fill =(0, 0, 0))
    d1.text(coordinates['Address'], person._dl_address.full.upper(), font=mainFont, fill =(0, 0, 0))
    d1.text(coordinates['Sex'], person.gender, font=mainFont, fill =(0, 0, 0))
    d1.text(coordinates['Height'], "5'10\"", font=mainFont, fill =(0, 0, 0))
    d1.text(coordinates['Hair'], "Brn".upper(), font=mainFont, fill =(0, 0, 0))
    d1.text(coordinates['Weight'], "160", font=mainFont, fill =(0, 0, 0))
    d1.text(coordinates['Eye Color'], "Grn".upper(), font=mainFont, fill =(0, 0, 0))
    d1.text(coordinates['Issued'], "8/15/2012", font=mainSmall, fill =(0, 0, 0))
    d1.text(coordinates['Signature'], f'{person.first_name} {person.last_name}', font=sigFont, fill=(0, 0, 0))
    #Hair Text
    d1.text((689,500), "HAIR", font=mainBoldSmall, fill =(22, 75, 144))

    image.show()
    out = f'output/dl_{person.person_id}_{person.first_name}_{person.last_name}.jpg'
    watermark = Image.open('base_images/FOR DEMO ONLY.png')
    image.paste(watermark,(20,20),watermark)
    # image.save(out)
    return upload_to_storage(out,public=True)

def generate_w2(person):
    #get base image
    image = Image.open('base_images/w2.jpg')
    #establish font
    font = ImageFont.truetype('fonts/PublicSans-Regular.ttf', 60)
    #Insert Text
    d1 = ImageDraw.Draw(image)
    #company name 
    d1.text((233,526), person.application.previous_employer, font=font, fill =(0, 0, 0))
    #company address 
    d1.text((233, 611), fake.address(), font=font, fill =(0, 0, 0))
    #company TIN 
    d1.text((233,358), "10-" + fake.ean(length=8), font=font, fill =(0, 0, 0))
    #Person TIN/SSN 
    d1.text((842,222), person.ssn, font=font, fill =(0, 0, 0))
    #Amount 
    d1.text((1854,369), str(person.application.previous_income), font=font, fill =(0, 0, 0))
    #Amount Withheld (1618,1035)
    d1.text((2501,369), str(int(person.application.previous_income*0.3)), font=font, fill =(0, 0, 0))
    #Recipient Name (1618,1035)
    d1.text((250,1050), person.name, font=font, fill =(0, 0, 0))
    #Recipient Driver's License Address (1618,1035)
    d1.text((250,1135), person._dl_address.full, font=font, fill =(0, 0, 0))

    #save & return
    # image.show()
    out = f'output/w2_{person.person_id}_{person.first_name}_{person.last_name}.jpg'
    watermark = Image.open('base_images/FOR DEMO ONLY.png')
    image.paste(watermark,(20,20),watermark)
    image.save(out)
    pdf_path = out.split('.')[0]+'.pdf'
    pdf_bytes = img2pdf.convert(out)
    file = open(pdf_path, "wb")
    file.write(pdf_bytes)
    return upload_to_storage(pdf_path)

def generate_1099(person):
    #get base image
    image = Image.open('base_images/1099.jpg')
    #establish font
    font = ImageFont.truetype('fonts/PublicSans-Regular.ttf', 60)
    #Insert Text
    d1 = ImageDraw.Draw(image)
    #company name (103,252)
    d1.text((103,252), person.application.previous_employer, font=font, fill =(0, 0, 0))
    #company address (103, 282)
    d1.text((103, 310), fake.address(), font=font, fill =(0, 0, 0))
    #company TIN (103,777)
    d1.text((103,777), "10-" + fake.ean(length=8), font=font, fill =(0, 0, 0))
    #Person TIN/SSN (103,777)
    d1.text((842,777), person.ssn, font=font, fill =(0, 0, 0))
    #Amount (1618,477)
    d1.text((1618,465), str(person.application.previous_income), font=font, fill =(0, 0, 0))
    #Amount Withheld (1618,1035)
    d1.text((1618,1020), str(int(person.application.previous_income*0.3)), font=font, fill =(0, 0, 0))
    #Recipient Name (1618,1035)
    d1.text((103,1019), person.name, font=font, fill =(0, 0, 0))
    #Recipient Driver's License Address (1618,1035)
    d1.text((103,1077), person._dl_address.full, font=font, fill =(0, 0, 0))

    #save & return
    # image.show()
    out = f'output/1099_{person.person_id}_{person.first_name}_{person.last_name}.jpg'
    watermark = Image.open('base_images/FOR DEMO ONLY.png')
    image.paste(watermark,(20,20),watermark)
    image.save(out)
    pdf_path = out.split('.')[0]+'.pdf'
    pdf_bytes = img2pdf.convert(out)
    file = open(pdf_path, "wb")
    file.write(pdf_bytes)
    return upload_to_storage(pdf_path)




job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE",autodetect=True)

persons = []
for _ in range(1):
    persons.append(asdict(Person()))

# person_data = json.dumps(persons, cls=CustomJSONEncoder)
# res = bq_client.load_table_from_json(json.loads(person_data),'vision.person',job_config=job_config)
# print(person_data)
# print('person:',res.result(),res.errors)

# documents_data = json.dumps(documents, cls=CustomJSONEncoder)
# res = bq_client.load_table_from_json(json.loads(documents_data),'vision.documents',job_config=job_config)
# print('documents: ',res.result(),res.errors)

# application_data = json.dumps(applications, cls=CustomJSONEncoder)
# res = bq_client.load_table_from_json(json.loads(application_data),'vision.applications',job_config=job_config)
# print('applications: ',res.result(),res.errors)
# print(application_data)

# account_events_data = json.dumps(account_events, cls=CustomJSONEncoder)
# res = bq_client.load_table_from_json(json.loads(account_events_data),'vision.account_events',job_config=job_config)
# print('account_events: ',res.result(),res.errors)

# payments_data = json.dumps(payments, cls=CustomJSONEncoder)
# res = bq_client.load_table_from_json(json.loads(payments_data),'vision.payments',job_config=job_config)
# print('payments: ',res.result(),res.errors)

# case_data = json.dumps(cases, cls=CustomJSONEncoder)
# res = bq_client.load_table_from_json(json.loads(case_data),'vision.case',job_config=job_config)
# print('cases: ',res.result(),res.errors)

# case_events_data = json.dumps(case_events, cls=CustomJSONEncoder)
# res = bq_client.load_table_from_json(json.loads(case_events_data),'vision.case_events',job_config=job_config)
# print('case events: ',res.result(),res.errors)

# Anamoly requirements:
# fuzzy_email -> russgarner@gmail   russgarner+123@gmail, elliot123@yahoo  -> elliot456@yahoo.com
# recent_domain -> using a sketchy new domain (looks at dns registration recency) 
# mail_address_change -> if they change their mailing address
# name_change -> if they changed their name more than once
# email_change -> if they changed their email more than once
# duplicate_mail_adddress -> some other enrollee has the same address
