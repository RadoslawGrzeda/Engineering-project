from faker import Faker
from random import randint
class CrmGenerator():
    def __init__(self):
        self.fake=Faker('pl_PL')

    def _fake_basic_parameters(self):
        # random=randint(0,100)
        person = {
            "first_name": self.fake.first_name(),
            'second_name': self.fake.first_name() if randint(0,100) > 90 else None,
            'last_name': self.fake.last_name(),
            'date_of_birth': self.fake.date_of_birth(minimum_age=15, maximum_age=90),
            'sex': 'female' if self.fake.first_name().lower().endswith('a') else 'male',
            'passport_number': self.fake.passport_number() if randint(0,100) > 90 else None,
        }
        return person

    def _fake_contact_parameters(self):
        person = {
            'email_address': self.fake.email(),
            'primary_email_address' : True if randint(0,100) <80 else False,
            'telephone_number': self.fake.phone_number(),
            'primary_telephone_number' : True if randint(0,100) <80 else False,
            # 'fax_number' : True if randint(0,100) <80 else False,
            # 'address' : self.fake.address(),
            'address_country': 'Polska',
            'address_city': self.fake.city(),
            'address_street': self.fake.street_address(),
            'address_zip_code': self.fake.zipcode(),
        }
        return person

# random=randint(0,100)
# print(random)
crm=CrmGenerator()
person=crm._fake_basic_parameters()
contact=crm._fake_contact_parameters()
print(contact)
# print(person)
