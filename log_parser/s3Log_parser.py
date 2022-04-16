import boto3
from dateutil.parser import parse
import re
from os.path import exists

s3 = boto3.resource('s3')
bucket = s3.Bucket('stellar.health.test.t.choudhury')
filename = 'patients.log'
new_filename = 'patients.log.corrected'


def download_log_file():
    for obj in bucket.objects.all():
        key = obj.key
        s3.Object(bucket.name, key).download_file(filename)


def upload_new_log_file():
    if exists(new_filename):
        s3.Object(bucket, new_filename).upload_file(
            Filename="patients.log.corrected")


def read_file():
    file = open(filename, 'r', encoding='utf8')
    lines = file.readlines()

    for line in lines:
        if 'DOB' in line:
            unformatted_dob = line.strip().split(' ')[6]
            dob = unformatted_dob.split('\'')[1]
            if is_valid_date(dob):
                if len(dob.split('/')[2]) < 4:
                    dob = append_century_to_year(dob)

                new_dob = modify_date(dob)

                if len(dob.split('/')[2]) > 3:
                    result = modify_line(line, new_dob)
                    append_to_new_log_file(result)
            else:
                append_to_new_log_file(line)
        else:
            append_to_new_log_file(line)

    upload_new_log_file()

    file.close()


def append_to_new_log_file(line):
    fout = open(new_filename, "a", encoding='utf-8')
    fout.write(line)
    fout.close()


def is_valid_date(date_text, fuzzy=False):
    try:
        parse(date_text, fuzzy=fuzzy)
        if len(date_text.split('/')) < 2:
            return False
        return True
    except ValueError:
        return False


def append_century_to_year(date_text):
    year = int(date_text.split('/')[2])

    if year > 22:
        new_date = date_text.replace('44', '1944')
    else:
        new_date = date_text.replace('44', '2044')

    return new_date


def modify_date(date_text):
    month = date_text.split('/')[0]
    day = date_text.split('/')[1]
    new_date = date_text.replace(month, "X", 1).replace(day, "X", 1)

    return new_date


def modify_line(line, new_date):
    regex = r'(\d{1,2})/(\d{1,2})/(\d{4})'
    result = re.sub(regex, new_date, line, 1)
    return result