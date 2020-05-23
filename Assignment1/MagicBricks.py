# Internship - Assignment1
# Scrape data from 99acres.com for properties in bangalore south available for rent
# and store it in NoSQL database mongodb for future use.

# import statements
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pandas as pd
import re
from urllib.request import Request, urlopen


# function to return property name from property_name_tag
def process_property_name(property_name_tag):
    property_name = []
    for tag in property_name_tag:
        property_name.append(tag.text)
    return property_name


# function to return size of the property from size_tag
def process_size_with_unit(size_tag):
    size_with_unit = []
    for tag in size_tag:
        pattern_size = 'srp_tuple_primary_area">(.*?)<span'
        pattern_size_unit = '-->(.*?)<!--'
        size = re.search(pattern_size, str(tag)).group(1)
        size_unit = re.search(pattern_size_unit, str(tag)).group(1)
        size_with_unit.append(size + " " + size_unit)
    return size_with_unit


# function to return bedroom type (1 BHK / 2 BHK) of property from bedroom_tag
def process_bedroom(bedroom_tag):
    bedroom = []
    for tag in bedroom_tag:
        pattern_bedroom = 'srp_tuple_bedroom">(.*?)<'
        bedroom_text = re.search(pattern_bedroom, str(tag)).group(1)
        bedroom.append(bedroom_text)
    return bedroom


# function to return date of posting of property from date_of_posting_tag
def process_date_of_posting(date_of_posting_tag):
    date_of_posting = []
    for tag in date_of_posting_tag:
        patter_date = '<span>(.*?) by'
        date_text = re.search(patter_date, str(tag)).group(1)
        date_of_posting.append(date_text)
    return date_of_posting


# function to return price with unit of property from price_tag
def process_price(price_tag):
    price_with_unit = []
    for tag in price_tag:
        pattern_price = 'srp_tuple_price">(.*?) <span'
        pattern_price_unit = '{srp_tuple_price_unit}">(.*?)</span'
        price = re.search(pattern_price, str(tag)).group(1)
        price_unit = re.search(pattern_price_unit, str(tag)).group(1)
        price_with_unit.append(price + " " + price_unit)
    return price_with_unit


# function to return price per unit area from price_per_unit_area_tag
def process_price_per_unit_area(price_per_unit_area_tag):
    price_per_unit_area = []
    for tag in price_per_unit_area_tag:
        price_per_unit_area.append(tag.text)
    return price_per_unit_area


# function to return description of property from description_tag
def process_description(description_tag):
    description = []
    for tag in description_tag:
        description.append(tag.text)
    return description


# split each data source field into seperate components for easier maintenance
def acres_99():
    # fetch the details about each property available for rent in bangalore south
    url = 'https://www.99acres.com/search/property/buy/residential-all/bangalore-south?search_type=QS&refSection=GNB' \
          '&search_location=NRI&lstAcn=NR_R&lstAcnId=-1&src=CLUSTER&preference=S&selected_tab=1&city=217&res_com=R' \
          '&property_type=R&isvoicesearch=N&keyword_suggest=Bangalore%20South%3B&fullSelectedSuggestions=Bangalore' \
          '%20South&strEntityMap' \
          '=W3sidHlwZSI6ImNpdHkifSx7IjEiOlsiQmFuZ2Fsb3JlIFNvdXRoIiwiQ0lUWV8yMTcsIFBSRUZFUkVOQ0VfUywgUkVTQ09NX1IiXX1d' \
          '&texttypedtillsuggestion=bangalore&refine_results=Y&Refine_Localities=Refine%20Localities&action=%2Fdo' \
          '%2Fquicksearch%2Fsearch&suggestion=CITY_217%2C%20PREFERENCE_S%2C%20RESCOM_R&searchform=1&price_min=null' \
          '&price_max=null '
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    content = urlopen(req).read()
    soup = BeautifulSoup(content, 'html.parser')

    # finding the required data field in the html parser
    price_tag = soup.find_all('td', id='srp_tuple_price')
    price_per_unit_area_tag = soup.find_all('div', id='srp_tuple_price_per_unit_area')
    size_tag = soup.find_all('td', id='srp_tuple_primary_area')
    bedroom_tag = soup.find_all('td', id='srp_tuple_bedroom')
    date_of_posting_tag = soup.find_all('div', class_='caption_strong_small')
    description_tag = soup.find_all('div', id='srp_tuple_description')
    property_name_tag = soup.find_all('a', class_='body_med srpTuple__propertyName')

    # calling process_property_name() to get list of property names
    property_name = process_property_name(property_name_tag)

    # calling process_size_with_unit() to get list of size with unit
    size_with_unit = process_size_with_unit(size_tag)

    # calling process_bedroom() to get list of bedroom(1 BHK / 2 BHK)
    bedroom = process_bedroom(bedroom_tag)

    # calling process_date_of_posting() to get list of date of posting
    date_of_posting = process_date_of_posting(date_of_posting_tag)

    # calling process_price() to get list of price with unit
    price_with_unit = process_price(price_tag)

    # calling process_price_per_unit_area() to get list of price per unit area
    price_per_unit_area = process_price_per_unit_area(price_per_unit_area_tag)

    # calling process_description() to get list of description
    description = process_description(description_tag)

    # Creating Pandas Series out of the processed list
    price_series = pd.Series(price_with_unit)
    price_per_unit_area_series = pd.Series(price_per_unit_area)
    size_series = pd.Series(size_with_unit)
    bedroom_series = pd.Series(bedroom)
    date_of_posting_series = pd.Series(date_of_posting)
    description_series = pd.Series(description)
    property_name = pd.Series(property_name)

    # merging all the DataFrames using pd.merge()
    df_1 = pd.merge(pd.DataFrame(data=property_name), pd.DataFrame(data=size_series), left_index=True, right_index=True,
                    how='inner')
    df_2 = pd.merge(df_1, pd.DataFrame(data=bedroom_series), left_index=True, right_index=True, how='inner')
    df_3 = pd.merge(df_2, pd.DataFrame(data=date_of_posting_series), left_index=True, right_index=True, how='inner')
    df_4 = pd.merge(df_3, pd.DataFrame(data=price_series), left_index=True, right_index=True, how='inner')
    df_5 = pd.merge(df_4, pd.DataFrame(data=price_per_unit_area_series), left_index=True, right_index=True, how='inner')
    df = pd.merge(df_5, pd.DataFrame(data=description_series), left_index=True, right_index=True, how='inner')

    # assigning column names and creating an index along axis = 1
    df.columns = ['property', 'size', 'bedroom', 'date of posting', 'price', 'price per unit area', 'description']
    df = df.sort_index()
    df = df.reset_index(level=0)
    df = df.rename({'index': '_id'}, axis=1)

    # insert into mongodb database
    database = client['99_acres_database']
    collection = database['99_acres_collection']
    collection.insert_many(df.to_dict('records'))

    # print progress
    print("99acres.com data added to MongoDB sucessfully!")


if __name__ == "__main__":
    # connect to MongoDB
    client = MongoClient(
        "mongodb+srv://saigirish:saigirish@cluster0-vxrjv.mongodb.net/test?retryWrites=true&w=majority")
    db = client.test

    # fetch data from 99acres.com and dump it into mongodb
    acres_99()
