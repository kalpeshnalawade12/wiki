"""
Author: Daniel Seidner
Purpose: Backup a W3 Wiki
Note: This code is in a proof-of-concept state and shouldn't be used yet. Several tasks remain
to get a usable wiki from W3 into Confluence, including creating a navigation tree, re-writing
the HTML to link to the attached images/files, etc...

Instructions:
    1) Only works on a Mac because of the pycookiecheat library
    2) Make sure you're in Chrome and login to W3 Communities
    3) Find the wiki id by looking at the URL in your browser, it should be at the end of the URL for the base Wiki page
    4) Put the wiki id in the w3_wiki_id var
    5) Adjust the w3_number_of_pages to the total number of pages you need to download from the wiki.
       This is helpful if you just want to test a few pages instead of the whole thing.
    6) Set the sync_to_confluence to True/False if you want to enable/disable syncing the content from W3 to Confluence
    7) Set your username, password, and the key of your Confluence Space. The space key should be just a few letters.
       The endpoint is just the URL of the Confluence instance you're sending data to.

Requirements:
    pip install requests
    pip install pycookiecheat
    pip install xmltodict
    pip install atlassian-python-api
    pip install beautifulsoup4
    Confluence must have a home page (as all pages are created under home so they can be navigated to easily):
    https://confluence.atlassian.com/doc/set-up-a-space-home-page-829076213.html

"""

import requests
from pycookiecheat import chrome_cookies
import xmltodict
import json
from datetime import datetime
import pprint
import math
import os
from atlassian import Confluence
from bs4 import BeautifulSoup

# max number of pages to bring back from index
w3_number_of_pages = 30
# the wiki id
w3_wiki_id = 'Wc486ea8350c9_45b3_8ca1_f123f0bd59b8'

# Do you want to sync the data from W3 to Confluence?
# Set to False if you just want local backup
sync_to_confluence = True
conf_username = "yourusername@acoustic.co"
conf_password = "yourstrongpassword"
conf_endpoint = "https://conftest.acoustic.co/"
conf_space_name = 'CEST'

# turn on more logging
debug_script = True


# make it easy to debug in one line
def debug_w3(log, pretty=False):
    if debug_script:
        if not pretty:
            print(log)
        else:
            pprint.pprint(log)


class XmlWorker:
    def __init__(self, xmldata):
        self.xmldata = xmldata

    def getDict(self):
        dict_of_feed = json.dumps(xmltodict.parse(self.xmldata))
        return json.loads(dict_of_feed)

    def getWikiSecondId(self, xmldict):
        second_id = xmldict['feed']['id']
        just_the_id = second_id.replace("urn:lsid:ibm.com:td:", "")
        return just_the_id


class WikiWorker:
    def __init__(self, wikiid):
        self.wikiid = wikiid

    def getIndexUrl(self, w3_number_of_pages):
        w3_index = 'https://w3-connections.ibm.com/wikis/form/api/wiki/{}/feed?ps={}&includeTags=true&sK=modified&sO=dsc' \
            .format(self.wikiid, w3_number_of_pages)
        return w3_index

    def getAttachmentUrl(self, secondid, pageid):
        w3_attachment = 'https://w3-connections.ibm.com/wikis/basic/api/wiki/{}/page/{}/feed?category=attachment' \
            .format(secondid, pageid)
        return w3_attachment

    def getNavUrlID(self, the_cookies, the_headers):
        na = requests.get('https://w3-connections.ibm.com/wikis/form/api/wiki/{}/homepage/entry?inline=true'.format(self.wikiid), cookies=the_cookies, headers=the_headers)
        xml_nav = XmlWorker(na.text)
        xml_nav_dict = xml_nav.getDict()
        nav_id = xml_nav_dict['entry']['id']
        just_the_nav_id = nav_id.replace("urn:lsid:ibm.com:td:", "")
        return just_the_nav_id

    def convertSize(self, size_bytes):
        size_bytes = int(size_bytes)
        if size_bytes == 0:
            return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])


# build out wiki details
wiki_meta = WikiWorker(w3_wiki_id)
w3_index_url = wiki_meta.getIndexUrl(w3_number_of_pages)

# get cookies from your own Chrome, make sure you are currently logged in to W3!
cookies = chrome_cookies(w3_index_url)
# need to present a user-agent for W3 to accept your visit
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}
# make request to the W3 index with the cookies and header created earlier
print("Making request for W3 Wiki Index")
try:
    r = requests.get(w3_index_url, cookies=cookies, headers=headers)
except Exception as e:
    print("Unable to get the wiki, make sure you're on AnyConnect\nError: {}".format(e))
    raise SystemExit

# start parsing the W3 index and get additional ids needed
xml_wiki_index = XmlWorker(r.text)
print("Trying to parse XML for Wiki Index")
items = None
try:
    items = xml_wiki_index.getDict()
except Exception as e:
    print("Unable to parse XML, make sure you're logged in to W3 in Chrome\nError: {}".format(e))
    raise SystemExit
wiki_second_id = xml_wiki_index.getWikiSecondId(items)
print("Found secondary W3 ID: " + wiki_second_id)

# determine how many pages are from the index
try:
    number_of_expected_pages = items['feed']['opensearch:totalResults']
    wiki_title = items['feed']['title']['#text']
    print("Expecting {} total pages in Wiki named '{}', getting {} pages".format(number_of_expected_pages, wiki_title, w3_number_of_pages))
except Exception as e:
    print(e)
    raise SystemExit

# setup empty dictionary that we'll append the pages metadata to
pages_to_download = []

# build out the dictionary with the info of the page
def create_page_append(url, title, link, author, created, modifier, modified, media_link, page_id, attachments):
    return {'download_url': url
        , 'title': title
        , 'link': link
        , 'author': author
        , 'created': created
        , 'modifier': modifier
        , 'modified': modified
        , 'media_url': media_link
        , 'page_id': page_id
        , 'attachments': attachments}


for it in items['feed']['entry']:
    x_title = it['title']['#text']
    x_link = it['link'][1]['@href'] # second href has the best url
    x_author = it['author']['name']
    x_created = datetime.strptime(it['td:created'], "%Y-%m-%dT%H:%M:%S.%fZ")
    x_modifier = it['td:modifier']['name']
    x_modified = datetime.strptime(it['td:modified'], "%Y-%m-%dT%H:%M:%S.%fZ")
    x_page_id = it['td:uuid']
    x_download_link = None
    x_media_link = None
    x_attachment_link = wiki_meta.getAttachmentUrl(wiki_second_id, x_page_id)

    # find the enclosure URL so we can download the HTML
    for l in it['link']:
        if l['@rel'] == 'enclosure':
            x_download_link = l['@href']

    for l in it['link']:
        if l['@rel'] == 'edit-media':
            x_media_link = l['@href']

    # generate a dictionary of page items
    pages_to_download.append(
        create_page_append(
            x_download_link, x_title, x_link, x_author, x_created, x_modifier, x_modified, x_media_link, x_page_id, x_attachment_link
        )
    )

print("#" * 20)
print("Looking for attachments on pages")
print("#" * 20)

# dictionary to hold attachment metadata using page id as key
attachments_to_download = {}

# start searching for attachments on the pages collected
for x in pages_to_download:
    # download attachment metadata
    attach_data = requests.get(x['attachments'], cookies=cookies, headers=headers)
    attach_xml = XmlWorker(attach_data.text)
    # make the metadata into a dictionary
    attach_meta = attach_xml.getDict()
    # setup empty list to hold dictionaries of the attachment metadata
    attachment_info = []
    num_of_attachments = attach_meta['feed']['opensearch:totalResults']
    print("{} -- Num of attachments: {}".format(x['title'], num_of_attachments))

    # Ughh, the W3 XML changes if there is exactly one attachment
    # Duplicating some code here, come back and cleanup later :(
    if num_of_attachments == '1':
        at = attach_meta['feed']['entry']
        x_attach_size = None
        for attach in at['link']:
            if attach['@rel'] == 'enclosure':
                x_attach_size = attach['@length']

        attachment_info.append({'content': at['content']['@src']
                                   , 'type': at['content']['@type']
                                   , 'title': at['title']['#text']
                                   , 'size_bytes': x_attach_size
                                   , 'size_human': wiki_meta.convertSize(x_attach_size)})

        attachments_to_download[x['page_id']] = attachment_info

    # assumes there is more than one attachment
    elif num_of_attachments not in ['0','1']:
        for at in attach_meta['feed']['entry']:
            x_attach_size = None
            for attach in at['link']:
                if attach['@rel'] == 'enclosure':
                    x_attach_size = attach['@length']

            attachment_info.append({'content': at['content']['@src']
                                       , 'type': at['content']['@type']
                                       , 'title': at['title']['#text']
                                       , 'size_bytes': x_attach_size
                                       , 'size_human': wiki_meta.convertSize(x_attach_size)})

        attachments_to_download[x['page_id']] = attachment_info

if sync_to_confluence:
    confluence = Confluence(
        url=conf_endpoint,
        username=conf_username,
        password=conf_password)

    print("Trying to login to Confluence")
    space_details = confluence.get_space(conf_space_name)
    if 'HTTP Status 401' in space_details:
        print("ERROR: Login incorrect, you are not authorized. Verify your credentials!")
        raise SystemExit

    try:
        homepage_id = space_details['homepage']['id']
        print("Confluence Homepage ID is {}".format(homepage_id))
    except Exception as e:
        print("ERROR: Couldn't find homepage for space...")
        if 'message' in space_details:
            pprint.pprint(space_details['message'])
        raise SystemExit


def create_conf_page(title, body):
    print("Trying to create confluence page")
    try:
        page_create = confluence.create_page(
            space=conf_space_name,
            title=title,
            body=body,
            parent_id=str(homepage_id)
        )
        print("####PAGE CREATE####")
        if 'id' in page_create:
            new_page_id = page_create['id']
            print("Created page id " + new_page_id)
            return new_page_id
        else:
            print("ERROR: Couldn't create page...")
            pprint.pprint(page_create)
            if 'page with this title already exists' in page_create['message']:
                print("Page must have a unique name that isn't already in Confluence")
            else:
                debug_w3(body, pretty=True)
            return 0
    except Exception as e:
        print("ERROR: Could not create page...")
        print(str(e))
        raise SystemExit


def create_conf_attachment(page_id, file_name, file):
    print("Trying to create confluence attachment {}".format(file_name))
    try:
        status = confluence.attach_file(file, page_id=str(page_id), space=conf_space_name, name=file_name)
        print("####ATTACHMENT CREATE####")
        if 'id' in status['results'][0]:
            new_attachment_id = status['results'][0]['id']
            print("Created attachment id " + new_attachment_id)
        else:
            print("ERROR: Couldn't attach file...")
            pprint.pprint(status)
    except Exception as e:
        print("ERROR: Could not attach file...")
        print(str(e))
        raise SystemExit


# setup local copy directory under wikibackup folder and enter it
script_directory = os.getcwd()
local_wiki_directory = os.path.join(script_directory, 'wikibackup', wiki_title)
os.makedirs(local_wiki_directory, exist_ok=True)
os.chdir(local_wiki_directory)

print("#" * 20)
print("Putting wiki files in {}".format(local_wiki_directory))
print("#" * 20)

# create folders for each page and download pages/attachments into the folder
for page in pages_to_download:
    conf_page_id = None
    os.chdir(local_wiki_directory)
    os.makedirs(page['title'], exist_ok=True)
    pd = requests.get(page['download_url'], cookies=cookies, headers=headers)
    print("Downloading HTML for {}".format(page['title']))
    with open(os.path.join(os.getcwd(), page['title'], 'index.html'), 'wb') as f:
        f.write(pd.content)

    if sync_to_confluence:
        soup = BeautifulSoup(pd.content, 'html.parser')
        # need to remove some items that cause fits for Confluence xhtml parser
        for meta in soup.find_all('meta'):
            meta.decompose()
        for v in soup.find_all('v:rect'):
            v.decompose()
        conf_page_id = create_conf_page(page['title'], soup.prettify())

    # only try to get attachments if they exist for a page id
    if page['page_id'] in attachments_to_download.keys():
        # print("Attachments exist for {}".format(page['title']))
        for fi in attachments_to_download[page['page_id']]:
            ad = requests.get(fi['content'], cookies=cookies, headers=headers)
            print('    Downloading {} || size - {}'.format(fi['title'], fi['size_human']))
            with open(os.path.join(os.getcwd(), page['title'], fi['title']), 'wb') as f:
                f.write(ad.content)

            if sync_to_confluence and int(conf_page_id) > 0:
                create_conf_attachment(conf_page_id, fi['title'], os.path.join(os.getcwd(), page['title'], fi['title']))
    print("#" * 20)