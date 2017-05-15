import faker
import random
import babel


class RandomText(object):

    def __init__(self):
    self.factories = dict((locale, faker.Faker(locale=locale)) for locale in faker.config.AVAILABLE_LOCALES)
    self.names = dict((locale, self.factories[locale].name)
                 for locale in faker.utils.loading.find_available_locales(['faker.providers.person']))
    self.companies = dict((locale, self.factories[locale].company)
                     for locale in faker.utils.loading.find_available_locales(['faker.providers.company']))
    self.emails = dict((locale, self.factories[locale].email)
                     for locale in faker.utils.loading.find_available_locales(['faker.providers.internet']))
    self.logins = dict((locale, self.factories[locale].user_name)
                     for locale in faker.utils.loading.find_available_locales(['faker.providers.internet']))
    self.dates = {}
    for locale in faker.config.AVAILABLE_LOCALES:
        factory = self.factories[locale]
        try:
            l = babel.Locale.parse(locale)
            self.dates[locale] = lambda : self.random_date_or_time(factory, l)
        except:
            pass

    def random_date_or_time(self, faker, locale=None):
        locale = locale or faker.locale()
        func = random.choice([babel.dates.format_date, babel.dates.format_datetime, babel.dates.format_time])
        fmt = random.choice(["full", "long", "medium", "short"])
        return func(faker.date_time(), format=fmt, locale=locale)

    def tiny_random(self):
        if random.randint(0,1):
            return str(random.randint(10,99))
        return random.choice(self.ogins.values())()

    def short_random(self):
        provider = random.choice([self.names, self.companies, self.emails, self.dates])
        return random.choice(provider.values())()

    def medium_random(self):
        provider_1 = random.choice([self.names, self.companies])
        provider_2 = random.choice([self.emails, self.logins])
        locale, provider_1 = random.choice(provider_1.items())
        provider_2 = provider_2.get(locale, provider_2['en_US'])
        text = [provider_1(), provider_2()]
        random.shuffle(text)
        return ' '.join(text)

    def long_random(self, min_length=2, max_lenth=10):
        locale = random.choice(self.names.keys() + self.companies.keys())
        providers = list(p[locale] for p in (self.names, self.companies) if locale in p)
        result = []
        for i in range(random.randint(min_length, max_lenth)):
            result.append(random.choice(providers)())
        return ' '.join(result)