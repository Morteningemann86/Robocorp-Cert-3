from robocorp import workitems
from robocorp.tasks import task
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables

from resources.variables import *

# https://robocorp.com/docs/courses/work-data-management-python/15-loop-the-work-items

http = HTTP()
json = JSON()
table = Tables()



@task
def produce_traffic_data():
    """
    Inhuman Insurance Inc. Artificial Intelligence System automation.
    Produces traffic work items.
    """
    http.download(url=JSON_TRAFFIC_DATA, target_file=PATH_TRAFFIC_DATA, overwrite=True)
    traffic_data = load_traffic_data_as_table()
    table.write_table_to_csv(table=traffic_data, path=PATH_CSV_TEST)
    filtered_traffic_data = filter_and_sort_traffic_data(traffic_data)
    filtered_traffic_data = get_latest_data_by_country(filtered_traffic_data)
    payloads = create_work_item_payloads(filtered_traffic_data)
    save_work_item_payloads(payloads)

    print("produce")


@task
def consume_traffic_data():
    """
    Inhuman Insurance Inc. Artificial Intelligence System automation.
    Consumes traffic data work items.
    """
    print("consume")

def load_traffic_data_as_table():
    json_data = json.load_json_from_file(PATH_TRAFFIC_DATA)
    return table.create_table(json_data['value'])

def filter_and_sort_traffic_data(traffic_data):
    table.filter_table_by_column(traffic_data, RATE_KEY, '<', MAX_RATE)
    table.filter_table_by_column(traffic_data, GENDER_KEY, '==', BOTH_GENDERS)
    table.sort_table_by_column(traffic_data, YEAR_KEY, False)
    return traffic_data

def get_latest_data_by_country(traffic_data):
    data = table.group_table_by_column(traffic_data, COUNTRY_KEY)
    latest_data_ny_country = []
    for group in data:
        first_row = table.pop_table_row(group)
        latest_data_ny_country.append(first_row)
    return latest_data_ny_country

def create_work_item_payloads(filtered_traffic_data):
    payloads = []
    for row in filtered_traffic_data:
        payload = dict(
            country=row[COUNTRY_KEY],
            year=row[YEAR_KEY],
            rate=row[RATE_KEY],
        )
        payloads.append(payload)
    return payloads

def save_work_item_payloads(payloads):
    for payload in payloads:
        variables = dict(traffic_data=payload)
        workitems.outputs.create(variables)