import time

from influxdb import InfluxDBClient
import csv
import requests
import os
from github import Github
import json
import copy
import re
from progress.bar import Bar

DOWNLOAD_LINK = ""
INFLUX_CONFIG = {
    "IP": "influx_server.xyz",
    "USER": "influx_user",
    "PASSWORD": "influx_password",
    "Database": 'covid_stats',
    "PORT": 8086,
    "SSL": True,
    "SSL_VERFIY": True
}
GITHUB_CONFIG = {"auth_token": "github auth token"}


class HeaderMismatch(Exception):
    pass


def safe_float_cast(fl: str, default: float = 0):
    try:
        return float(fl)
    except:
        return float(default)


def safe_int_cast(fl: str, default: int = 0):
    try:
        return int(fl)
    except:
        return int(default)


def check_settings(config_root=""):
    configs = [('influx_config', INFLUX_CONFIG), ('github_config', GITHUB_CONFIG)]
    try:
        ensure_directory(config_root + 'configs')
        for f, config in configs:
            with open(config_root + 'configs/default_' + f + '.json', 'w') as default:
                json.dump(config, default, indent="  ")
    except Exception as e:
        print('could not create default config files with error :', e, "\n", e.args)
    try:
        for file, config in configs:
            config: dict
            try:
                with open(config_root + 'configs/' + file + '.json') as config_json:
                    cfg_dict: dict = json.load(config_json)
                    for key in cfg_dict.keys():
                        if config.get(key, "") != cfg_dict[key]:
                            config[key] = cfg_dict[key]
                            print("applied config {} from file {} from location {}".format(str(key), str(file),
                                                                                           str(config_root)))
            except FileNotFoundError:
                print('config file > {}.json from location {} < not available using defaults instead '.format(file, str(
                    config_root)))
    except FileNotFoundError:
        print("no configs found in directory ", config_root, configs)


def fix_china(string):
    if string == 'Mainland China':
        return 'China'
    if string == 'Taiwan':
        return 'Taiwan*'
    else:
        return string


def set_time_to_noon(date_str):
    day = re.split("[T ]", date_str)[0]
    return day + ' 12:00:00'


def fix_american_dates(date_str: str):
    if '/20' in date_str:
        spl = re.split("[/ ]", date_str)
        return "{}-{}-{}T{}".format('2020', spl[0], spl[1], spl[3])
    else:
        return date_str


class DataPoint:
    def __init__(self, input_list, headers, province_data={}):
        """

        :param input_list:
        :param headers:
        :param province_data: dic
        """
        self.in_list = input_list
        self.headers = headers

        self.fill_from_list(province_data)

    def fill_from_list(self, province_data):

        if (self.headers == ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_',
                             'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key']):
            self.fill_from_header4()
        elif (self.headers == ['Province/State', 'Country/Region', 'Last Update', 'Confirmed', 'Deaths', 'Recovered',
                               'Latitude', 'Longitude']):
            self.fill_from_header3()
        elif self.headers == ['Province/State', 'Country/Region', 'Last Update', 'Confirmed', 'Deaths', 'Recovered']:
            self.fill_from_header2(province_data)
        elif self.headers == ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_',
                              'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'Incidence_Rate',
                              'Case-Fatality_Ratio']:

            self.fill_from_header5()
        elif self.headers == ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_',
            'Confirmed',
            'Deaths', 'Recovered', 'Active', 'Combined_Key', 'Incident_Rate',
            'Case_Fatality_Ratio']:
            self.fill_from_header6()
        else:
            print('USING DEBUG METHOD', self.headers)
            self.fill_from_header5()

    def fill_from_header2(self, province_data):

        requiered_headers = ['Province/State', 'Country/Region', 'Last Update', 'Confirmed', 'Deaths', 'Recovered']
        for i in range(len(requiered_headers)):
            try:
                if requiered_headers[i] != self.headers[i]:
                    raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
            except IndexError as ind:
                print(ind, ind.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
            except ValueError as val:
                print(val, val.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")

        self.measurement = 'cases'
        self.time = set_time_to_noon(fix_american_dates(self.in_list[2]))

        self.tags = {
            'FIPS': '',
            'Admin2': "",
            'Province_State': fix_china(self.in_list[0]),
            'Country_Region': fix_china(self.in_list[1]),
            # Todo cast to datetime object => new function to transform all string formats..
            'Combined_Key': '{},{}'.format(fix_china(self.in_list[0]), fix_china(self.in_list[1]))
        }
        region_country_index = self.tags["Province_State"] + self.tags['Country_Region']
        self.fields = {
            'Confirmed': safe_int_cast(self.in_list[3]),
            'Deaths': safe_int_cast(self.in_list[4]),
            'Recovered': safe_int_cast(self.in_list[5]),
            'Active': safe_int_cast(self.in_list[3]) - safe_int_cast(self.in_list[5]) - safe_int_cast(self.in_list[4]),
            'Last_Update': fix_american_dates(self.in_list[2]),
            'Lat': safe_float_cast(province_data.get(region_country_index, {}).get("Lat", '')),
            'Long_': safe_float_cast(province_data.get(region_country_index, {}).get("Long_", '')),
        }

    def fill_from_header3(self):

        requiered_headers = ['Province/State', 'Country/Region', 'Last Update', 'Confirmed', 'Deaths', 'Recovered',
                             'Latitude', 'Longitude']

        for i in range(len(requiered_headers)):
            try:
                if requiered_headers[i] != self.headers[i]:
                    raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
            except IndexError as ind:
                print(ind, ind.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
            except ValueError as val:
                print(val, val.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")

        self.measurement = 'cases'
        self.time = set_time_to_noon(fix_american_dates(self.in_list[2]))
        self.tags = {
            'FIPS': '',
            'Admin2': "",
            'Province_State': fix_china(self.in_list[0]),
            'Country_Region': fix_china(self.in_list[1]),

            # Todo cast to datetime object => new function to transform all string formats..

            'Combined_Key': '{},{}'.format(fix_china(self.in_list[0]), fix_china(self.in_list[1]))
        }
        self.fields = {
            'Confirmed': int(self.in_list[3]),
            'Deaths': int(self.in_list[4]),
            'Recovered': int(self.in_list[5]),
            'Active': int(self.in_list[3]) - int(self.in_list[5]) - int(self.in_list[4]),
            'Last_Update': fix_american_dates(self.in_list[2]),
            'Lat': safe_float_cast(self.in_list[6]),
            'Long_': safe_float_cast(self.in_list[7]),
        }

    def fill_from_header4(self):
        requiered_headers = ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_',
                             'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key']
        for i in range(len(requiered_headers)):
            try:
                if requiered_headers[i] != self.headers[i]:
                    raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
            except IndexError as ind:
                print(ind, ind.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
            except ValueError as val:
                print(val, val.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")

        self.measurement = 'cases'
        self.time = set_time_to_noon(self.in_list[4])
        self.tags = {
            'FIPS': self.in_list[0],
            'Admin2': self.in_list[1],
            'Province_State': self.in_list[2],
            'Country_Region': self.in_list[3],

            # Todo cast to datetime object => new function to transform all string formats..

            'Combined_Key': self.in_list[11]
        }
        self.fields = {
            'Confirmed': int(self.in_list[7]),
            'Deaths': int(self.in_list[8]),
            'Recovered': int(self.in_list[9]),
            'Active': int(self.in_list[10]),
            'Last_Update': fix_american_dates(self.in_list[4]),
            'Lat': safe_float_cast(self.in_list[5]),
            'Long_': safe_float_cast(self.in_list[6]),
        }

    def fill_from_header5(self):
        requiered_headers = ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_',
                             'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'Incidence_Rate',
                             'Case-Fatality_Ratio']
        for i in range(len(requiered_headers)):
            try:
                if requiered_headers[i] != self.headers[i]:
                    raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
            except IndexError as ind:
                print(ind, ind.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
            except ValueError as val:
                print(val, val.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header4")
        try:
            self.measurement = 'cases'
            self.time = set_time_to_noon(self.in_list[4])
            self.tags = {
                'FIPS': self.in_list[0],
                'Admin2': self.in_list[1],
                'Province_State': self.in_list[2],
                'Country_Region': self.in_list[3],



                'Combined_Key': self.in_list[11]
            }
            self.fields = {
                'Last_Update': fix_american_dates(self.in_list[4]),
                'Lat': safe_float_cast(self.in_list[5]),
                'Long_': safe_float_cast(self.in_list[6]),
                'Incident_Rate': safe_float_cast(self.in_list[12]),
                'Case-Fatality_Ratio': safe_float_cast(self.in_list[13])
            }

            to_cast_to_int = [('Deaths', self.in_list[8]),
                              ('Recovered', self.in_list[9]),
                              ('Active', self.in_list[10]),
                              ('Confirmed', self.in_list[7])]
            for p in to_cast_to_int:
                try:
                    self.fields[p[0]] = int(p[1])
                except Exception as _e:
                    print("{} can not Format {} from line {}".format(_e, p, self.in_list))


        except ValueError as _v:
            print(_v, self.in_list)
    def fill_from_header6(self):
        requiered_headers = ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_',
                             'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'Incidence_Rate',
                             'Case_Fatality_Ratio']
        for i in range(len(requiered_headers)):
            try:
                if requiered_headers[i] != self.headers[i]:
                    raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header6")
            except IndexError as ind:
                print(ind, ind.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header6")
            except ValueError as val:
                print(val, val.args)
                raise HeaderMismatch("Header Mismatch fix headers or DataPoint.fill_from_header6")
        try:
            self.measurement = 'cases'
            self.time = set_time_to_noon(self.in_list[4])
            self.tags = {
                'FIPS': self.in_list[0],
                'Admin2': self.in_list[1],
                'Province_State': self.in_list[2],
                'Country_Region': self.in_list[3],

                # Todo cast to datetime object => new function to transform all string formats..

                'Combined_Key': self.in_list[11]
            }
            self.fields = {
                'Last_Update': fix_american_dates(self.in_list[4]),
                'Lat': safe_float_cast(self.in_list[5]),
                'Long_': safe_float_cast(self.in_list[6]),
                'Incident_Rate': safe_float_cast(self.in_list[12]),
                'Case-Fatality_Ratio': safe_float_cast(self.in_list[13])
            }

            to_cast_to_int = [('Deaths', self.in_list[8]),
                              ('Recovered', self.in_list[9]),
                              ('Active', self.in_list[10]),
                              ('Confirmed', self.in_list[7])]
            for p in to_cast_to_int:
                try:
                    self.fields[p[0]] = int(p[1])
                except Exception as _e:
                    print("{} can not Format {} from line {}".format(_e, p, self.in_list))


        except ValueError as _v:
            print(_v, self.in_list)


class TimeSeriesPoint:
    def __init__(self, meta_list, value_list):
        """

        :param meta_list:
        :param headers:
        :param value_list(Confirmed,Deaths,Recovered,Date): list[int,int,int,str]
        """
        self.value_list = value_list
        self.meta_list = meta_list
        self.fill_from_line()

    def fill_from_line(self):
        self.measurement = 'timeseries'
        # Province/State,Country/Region,Lat,Long,
        self.time = self.value_list[3]
        self.tags = {'Province_State': self.meta_list[0],
                     'Country_Region': self.meta_list[1],
                     }

        self.fields = {
            'Lat': safe_float_cast(self.meta_list[2]),
            'Long': safe_float_cast(self.meta_list[3]),
            'Confirmed': safe_int_cast(self.value_list[0]),
            'Deaths': safe_int_cast(self.value_list[1]),
            'Recovered': safe_int_cast(self.value_list[2])
        }


def get_timeseries_points():
    def get_list_from_csv_object(csv_object):
        l = []
        for r in csv_object:
            l.append(r)
        return l

    def get_line_from_indentifier(case_list, indentifier):
        # Province / State, Country / Region

        for _i, line in enumerate(case_list):
            if line[:2] == list(indentifier):
                return _i
        else:
            return None

    git = Github(GITHUB_CONFIG["auth_token"])
    repository = git.get_repo('CSSEGISandData/COVID-19')
    contents = repository.get_contents("csse_covid_19_data/csse_covid_19_time_series")
    deaths = ''
    confirmed = ''
    recovered = ''
    timepoints = []
    print(deaths)
    for ct in contents:  # limit to new format Todo Formatting for all existing options
        split_name = ct.name.split('.')
        time.sleep(.1)
        if split_name[-1] == 'csv':
            if 'global' in ct.name:
                if 'deaths' in ct.name:
                    req = requests.get(ct.download_url)
                    deaths = req.text.strip()
                elif 'confirmed' in ct.name:
                    confirmed = requests.get(ct.download_url).text.strip()
                elif 'recovered' in ct.name:
                    recovered = requests.get(ct.download_url).text.strip()
                else:
                    print('not using {}'.format(ct.name))

    deaths = get_list_from_csv_object(csv.reader(deaths.split("\n")))
    confirmed = get_list_from_csv_object(csv.reader(confirmed.split("\n")))
    recovered = get_list_from_csv_object(csv.reader(recovered.split("\n")))
    print(len(deaths), len(confirmed), len(recovered))
    date_start = 4
    for i, region_line in enumerate(confirmed[1:]):
        recovered_line = get_line_from_indentifier(recovered, region_line[:2])
        for j, d in enumerate(confirmed[0][date_start:]):
            point_date = fix_american_dates(d + 'T 12:00:00')
            k = j + date_start
            l: int = 1 + i  # header_length=1
            conf = confirmed[l][k]
            try:
                death = deaths[l][k]
            except:
                death = None
            try:
                rec = recovered[recovered_line][k]
            except:
                rec = None
            timepoints.append(
                TimeSeriesPoint(region_line[0:4], (conf, death, rec, point_date)))
    return timepoints


def push_data_to_influx(datalist):
    """

    :param datalist:: list[DataPoint]
    :return:
    """

    def chunks(l, n):
        n = max(1, n)
        return (l[i:i + n] for i in range(0, len(l), n))

    influx_client = InfluxDBClient(host=INFLUX_CONFIG["IP"], username=INFLUX_CONFIG["USER"],
                                   password=INFLUX_CONFIG["PASSWORD"], database=INFLUX_CONFIG["Database"],
                                   port=INFLUX_CONFIG["PORT"], ssl=INFLUX_CONFIG["SSL"],
                                   verify_ssl=INFLUX_CONFIG["SSL_VERFIY"])
    dictlist = []
    for d in datalist:
        dictlist.append(d.__dict__)
    chunk_size = 500
    number_of_chunks = int(len(dictlist) / chunk_size)
    dictlist_chunks = chunks(dictlist, chunk_size)

    print('uploading data in {} chuks\n\n'.format(number_of_chunks))

    progress_bar = Bar("uploading chunks", max=number_of_chunks)
    for i, c in enumerate(dictlist_chunks):
        progress_bar.next()
        result = influx_client.write_points(c)
    progress_bar.finish()
    influx_client.close()
    return result


def ensure_directory(dirpath):
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)


def get_data(github_link):
    def remove_leading_space(string):
        if len(string) == 0:
            return string
        if string[0] != " ":
            return string
        else:
            return string[1:]

    git = Github(GITHUB_CONFIG["auth_token"])
    repository = git.get_repo('CSSEGISandData/COVID-19')
    contents = repository.get_contents(github_link)
    provinces = get_provinces()
    headers = ['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_',
               'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key']
    return_list: list[DataPoint] = []
    for ct in contents:  # limit to new format Todo Formatting for all existing options
        split_name = ct.name.split('.')
        time.sleep(.1)
        if split_name[-1] == 'csv':
            if split_name[0] == '03-13-2020':
                print('Skipping : ', ct)
                continue  # FALSE DATA IN RAW DATA FIX WHEN AVAILABLE

            req = requests.get(ct.download_url)
            csv_text = req.text
            if csv_text[:1] == '\ufeff':
                print('File from {} is unicode'.format(split_name[0]))
                csv_text = csv_text[1:]
            csv_data = csv.reader(csv_text.split("\n"))

            for i, row in enumerate(csv_data):
                if i == 0:
                    headers = row
                    print(split_name[0], headers)
                    continue
                try:
                    raw_dp = DataPoint(row, headers, province_data=provinces)
                    region_keys = raw_dp.tags.get('Combined_Key', '').split(',')
                    dp_s = []

                    for k in region_keys:
                        temp_dp = copy.deepcopy(raw_dp)
                        temp_dp.tags['Combined_Key'] = remove_leading_space(k)
                        dp_s.append(temp_dp)
                    return_list.extend(dp_s)

                except IndexError as _e:
                    if len(row) == 0:
                        pass
                    else:
                        raise _e
                except ValueError as val:
                    print(val, val.args, row, headers)
                    raise val

                except HeaderMismatch:
                    print('Header not supported for file {} :'.format(split_name[0]), HeaderMismatch.args)
                    break

    return return_list


def get_provinces():
    province_location = "csse_covid_19_data"
    git = Github(GITHUB_CONFIG["auth_token"])
    repository = git.get_repo('CSSEGISandData/COVID-19')
    contents = repository.get_contents(province_location)
    # headers = [UID,iso2,iso3,code3,FIPS,Admin2,Province_State,Country_Region,Lat,Long_,Combined_Key]
    province_data = {}
    for ct in contents:
        split_name = ct.name.split('.')
        time.sleep(.1)
        if split_name[-1] == 'csv':
            req = requests.get(ct.download_url)
            csv_text = req.text
            if csv_text[:1] == '\ufeff':
                print('File from {} is unicode'.format(split_name[0]))
                csv_text = csv_text[1:]
            csv_data = csv.reader(csv_text.strip().split("\n"))
            for i, row in enumerate(csv_data):
                if i == 0:
                    headers = row
                    try:
                        province_index = headers.index('Province_State')
                        country_index = headers.index('Country_Region')
                    except ValueError as v:
                        print('Format in UID_ISO_FIPS_LookUp_Table.csv changed, adapat get provinces method')
                        raise v
                    continue
                index = row[province_index] + row[country_index]
                province_data[row[province_index] + row[country_index]] = {}
                for j, k in enumerate(headers):  # Currently picks the alphabetically last county of every state
                    province_data[index][k] = row[j]  # US only error...
    return province_data


if __name__ == '__main__':
    default_config_dir = ''
    check_settings(default_config_dir)
    timepoints = get_timeseries_points()
    print(push_data_to_influx(timepoints))

    data = get_data("csse_covid_19_data/csse_covid_19_daily_reports")

    print(push_data_to_influx(data))
