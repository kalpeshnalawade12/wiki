"""
Author: Daniel Seidner
Purpose: Backup a W3 Wiki to Confluence
Note: Review this blog post before executing:
    https://confluence.acoustic.co/display/~daniel.seidner@acoustic.co/2019/12/30/Migrating+from+W3+Wikis+to+Confluence+Spaces

API References:
    1) Connections API: https://www-10.lotus.com/ldd/lcwiki.nsf/xpAPIViewer.xsp?lookupName=IBM+Connections+6.0+API+Documentation#action=openDocument&res_title=Working_with_wiki_pages_ic60&content=apicontent
    2) Confluence API: https://docs.atlassian.com/ConfluenceServer/rest/7.2.0/

Requirements:
    Tested in Python 3.7+
    pip3 install requests
    pip3 install browser_cookie3
    pip3 install xmltodict
    pip3 install atlassian-python-api
    pip3 install bs4
"""

import requests
# from pycookiecheat import chrome_cookies
import browser_cookie3
import xmltodict
from atlassian import Confluence
from bs4 import BeautifulSoup, NavigableString

import json
from datetime import datetime
import pprint
import math
import os
from urllib.parse import urlparse
import unicodedata
import logging

# the wiki id, make sure this isn't the community id... get it from the wiki site
w3_wiki_id = 'Wee8e77102d31_4b60_9453_76f36e281a43'
# number of pages metadata to bring back from W3 index at once | max is 500
w3_number_of_pages = 100
# set to true if you only want to get the first w3_number_of_pages pages for testing
stop_after_first_index_scan = False
# The base URL of the W3 wiki host
w3_host = 'https://w3-connections.ibm.com'

# Do you want to sync the data from W3 to Confluence?
# Set to False if you just want local backup
sync_to_confluence = True
# Do you want to use your existing Confluence home page
# Or create a new home page where this wiki backup will exist?
use_existing_conf_home_page = False

conf_endpoint = "https://conftest.acoustic.co/"
# conf_endpoint = 'https://confluence.acoustic.co/'
conf_space_name = 'CEST'
conf_max_attachment_size = 104857600

# The following settings are only in effect if sync_to_confluence is True
# Add a table to the bottom of the confluence page with W3 page details
# like original author and creation date
append_w3_history_table = True
# Add the comments to bottom of confluence page, after the W3 page details
append_wiki_comments = True
# Look for Table-of-Contents from W3 Wiki and replace with Confluence TOC format
replace_table_of_contents = True
# Look for W3 links that point to other W3 pages in the same wiki and replace them
replace_w3_wiki_links = True
# Find connections file links and download/upload them to Confluence attachments
replace_connections_files = True
# Review any links that are IBM or BOX for later review
find_possible_link_issues = True

# some logs we want to write to file
# and some to both file and console
logger = logging.getLogger('w3scrape')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('w3scrape.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
NEW_FORMAT = '%(asctime)s.%(msecs)03d: %(message)s'
file_logger_format = logging.Formatter(NEW_FORMAT, '%m/%d/%Y %I:%M:%S')
fh.setFormatter(file_logger_format)
ch.setFormatter(file_logger_format)
logger.addHandler(fh)
logger.addHandler(ch)


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
    def __init__(self, wikiid, cookies, headers):
        self.wikiid = wikiid
        self.cookies = cookies
        self.headers = headers
        self.wiki_feed_pages = []
        self.nav_id = ""

    def getIndexUrl(self, w3_number_of_pages):
        w3_index = '{}/wikis/form/api/wiki/{}/feed?ps={}&includeTags=true&sK=modified&sO=dsc' \
            .format(w3_host, self.wikiid, w3_number_of_pages)
        self.w3_wiki_index_url = w3_index
        return w3_index

    def getAttachmentUrl(self, secondid, pageid):
        w3_attachment = '{}/wikis/basic/api/wiki/{}/page/{}/feed?category=attachment' \
            .format(w3_host, secondid, pageid)
        return w3_attachment

    def convertSize(self, size_bytes):
        size_bytes = int(size_bytes)
        if size_bytes == 0:
            return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def getWikiIndexFeed(self, index_url):
        r = requests.get(index_url, cookies=self.cookies, headers=self.headers)
        return r.text

    def get_next_feed_page(self, feed_json):
        next_feed_page = None
        for link in feed_json['feed']['link']:
            if link['@rel'] == 'next':
                next_feed_page = link['@href']
        return next_feed_page

    def add_wiki_feed_pages(self, page_items):
        items_type = type(page_items)
        # if it's a list, there's more than one entry
        # if it's a dict, there's just one entry so we need to change the way we append
        if items_type is list:
            num_of_items = len(page_items)
            for its in page_items:
                self.wiki_feed_pages.append(its)
        else:
            num_of_items = 1
            self.wiki_feed_pages.append(page_items)
        if len(self.wiki_feed_pages) != int(w3_number_of_pages):
            logger.info("Found {} more pages".format(num_of_items))

    def get_wiki_page_comments(self, second_id, page_id):
        feed_page_url = "{}/wikis/form/api/wiki/{}/page/{}/feed".format(w3_host, second_id, page_id)
        r = requests.get(feed_page_url, cookies=self.cookies, headers=self.headers)
        return r.text


# in case we find things that didn't break the script but should be noticed
alert_items = []

# Capture entire process start time
startSyncTime = datetime.now()

# get cookies from your own Chrome, make sure you are currently logged in to W3!
# cookies = chrome_cookies(w3_host)

# get cookies for all domains that include ibm
ibm_cookies = browser_cookie3.chrome(domain_name='ibm.com')

cookies = {}

for co in ibm_cookies:
    # W3 connections needs cookies from these two domains
    if co.domain in ['.ibm.com', 'w3-connections.ibm.com']:
        cookies[co.name] = co.value

# need to present a user-agent for W3 to accept your visit
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/35.0.1916.47 Safari/537.36'}
# make request to the W3 index with the cookies and header created earlier
logger.info("Making request for W3 Wiki Index")

# build out wiki details
wiki_meta = WikiWorker(w3_wiki_id, cookies, headers)
w3_index_url = wiki_meta.getIndexUrl(w3_number_of_pages)

try:
    wiki_feed = wiki_meta.getWikiIndexFeed(w3_index_url)
except Exception as e:
    logger.error("Unable to get the wiki, make sure you're on AnyConnect\nError: {}".format(e), exc_info=True)
    raise SystemExit

# start parsing the W3 index and get additional ids needed
xml_wiki_index = XmlWorker(wiki_feed)
logger.info("Trying to parse XML for Wiki Index")
items = None
try:
    items = xml_wiki_index.getDict()
except Exception as e:
    logger.error("Unable to parse XML, make sure you're logged in to W3 in Chrome\nError: {}".format(e), exc_info=True)
    raise SystemExit

# get the wiki label used in other API calls
wiki_second_id = xml_wiki_index.getWikiSecondId(items)
logger.info("Found secondary/label W3 ID: " + wiki_second_id)

# add the initial load of entries
wiki_meta.add_wiki_feed_pages(items['feed']['entry'])

# determine how many pages are from the index
try:
    number_of_expected_pages = items['feed']['opensearch:totalResults']
    wiki_title = items['feed']['title']['#text']
    logger.info(
        "Expecting {} total pages in Wiki named '{}', getting {} pages at a time".format(number_of_expected_pages,
                                                                                         wiki_title,
                                                                                         w3_number_of_pages))
except Exception as e:
    logger.error(e, exc_info=True)
    raise SystemExit

# Look for more wiki index feeds to download
# by seeing if there's another feed page listed
first_next_url = wiki_meta.get_next_feed_page(items)
if first_next_url is not None and stop_after_first_index_scan is False:
    logger.info("Next page URL: {}".format(first_next_url))
    url_index_to_get = first_next_url
    while True:
        new_feed = wiki_meta.getWikiIndexFeed(url_index_to_get)
        xml_next_wiki_index = XmlWorker(new_feed)
        next_items = xml_next_wiki_index.getDict()
        wiki_meta.add_wiki_feed_pages(next_items['feed']['entry'])
        next_page_url = wiki_meta.get_next_feed_page(next_items)
        if next_page_url is not None:
            logger.info("Next page URL: {}".format(next_page_url))
            # now let's do this again with the next page url
            # essentially making a loop out of it
            url_index_to_get = next_page_url
        else:
            logger.info("All wiki pages found: {}".format(len(wiki_meta.wiki_feed_pages)))
            break
else:
    logger.info("All wiki pages found: {}".format(len(wiki_meta.wiki_feed_pages)))

# setup empty dictionary that we'll append the pages metadata to
pages_to_download = []


class NavigationWorker:
    def __init__(self, pageid, navid):
        self.pageid = pageid
        self.navid = navid

    def get_parent_id(self, the_cookies, the_headers):
        na = requests.get(
            '{}/wikis/basic/api/wiki/{}/navigation/{}/entry?format=json&includeBreadcrumbs=true'.format(w3_host,
                                                                                                        self.navid,
                                                                                                        self.pageid),
            cookies=the_cookies, headers=the_headers)
        pageParentId = json.loads(na.text)['parent']
        return pageParentId


# build out the dictionary with the info of the page
def create_page_append(url, title, link, author, created, modifier, modified, media_link, page_id, attachments,
                       parent_id):
    return {'download_url': url
        , 'title': title
        , 'link': link
        , 'author': author
        , 'created': created.strftime("%m/%d/%Y")
        , 'modifier': modifier
        , 'modified': modified.strftime("%m/%d/%Y")
        , 'media_url': media_link
        , 'page_id': page_id
        , 'attachments': attachments
        , 'parent_id': parent_id}


logger.info("Please wait, indexing Wiki and getting parent page ids for navigation")

for i, it in enumerate(wiki_meta.wiki_feed_pages):
    if i % 20 == 0 and i != 0:
        logger.info("Found {} parent ids so far".format(i))
    x_title = it['title']['#text']
    x_link = it['link'][1]['@href']  # second href has the best url
    x_author = it['author']['name']
    x_created = datetime.strptime(it['td:created'], "%Y-%m-%dT%H:%M:%S.%fZ")
    x_modifier = it['td:modifier']['name']
    x_modified = datetime.strptime(it['td:modified'], "%Y-%m-%dT%H:%M:%S.%fZ")
    x_page_id = it['td:uuid']
    x_download_link = None
    x_media_link = None
    x_attachment_link = wiki_meta.getAttachmentUrl(wiki_second_id, x_page_id)
    nav = NavigationWorker(x_page_id, wiki_second_id)
    x_parent_id = nav.get_parent_id(cookies, headers)

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
            x_download_link, x_title, x_link
            , x_author, x_created, x_modifier
            , x_modified, x_media_link, x_page_id
            , x_attachment_link, x_parent_id
        )
    )

# store the number of pages to download so we can present it later without calculating
number_of_pages_to_download = len(pages_to_download)
logger.info("Finished indexing {} Wiki pages and getting their parent pages".format(number_of_pages_to_download))


def create_conf_page(title, body, parent_id=None):
    logger.info("Trying to create confluence page")
    try:
        if parent_id is not None:
            page_create = confluence.create_page(
                space=conf_space_name,
                title=title,
                body=body,
                parent_id=str(parent_id)
            )
        else:
            page_create = confluence.create_page(
                space=conf_space_name,
                title=title,
                body=body
            )
        if 'id' in page_create:
            new_page_id = page_create['id']
            logger.info("Created page id " + new_page_id)
            return new_page_id
        else:
            logger.info("ERROR: Couldn't create page...")
            logger.debug(pprint.pformat(page_create))
            alert_items.append("Couldn't create page {}, see debug logs".format(title))
            if 'page with this title already exists' in page_create['message']:
                logger.info("Page must have a unique name that isn't already in Confluence")
            else:
                # get the body printed in case there's an error parsing xhtml
                logger.debug(pprint.pformat(body))
            return 0
    except Exception as e:
        logger.error("ERROR: Could not create page: {}".format(e), exc_info=True)
        raise SystemExit


conf_attachment_mapping = []


def create_conf_attachment(page_id, file_name, file):
    logger.info("Trying to create confluence attachment {}".format(file_name))
    try:
        status = confluence.attach_file(file, page_id=str(page_id), space=conf_space_name, name=file_name)
        logger.debug(status)
        logger.info("####ATTACHMENT CREATE####")
        if 'id' in status['results'][0]:
            new_attachment_id = status['results'][0]['id']
            logger.info("Created attachment id " + new_attachment_id)
            conf_attachment_mapping.append({'file_name': file_name, 'page_id': page_id, 'attach_id': new_attachment_id})
        else:
            logger.info("ERROR: Couldn't attach file...")
            logger.debug(status)
    except Exception as e:
        logger.error("ERROR: Could not attach file: {}".format(e), exc_info=True)
        raise SystemExit


existing_conf_pages = []

homepage_id = None
if sync_to_confluence:
    # make sure you're logged into Confluence
    conf_cookies = requests.utils.dict_from_cookiejar(
        browser_cookie3.chrome(
            domain_name=urlparse(conf_endpoint).hostname)
    )
    # conf_cookies = chrome_cookies(conf_endpoint)
    logger.info("Trying to login to Confluence and get space details")
    confluence = Confluence(
        url=conf_endpoint,
        cookies=conf_cookies)

    space_details = confluence.get_space(conf_space_name)
    if 'HTTP Status 401' in space_details:
        logger.info("ERROR: Login incorrect, you are not authorized. Verify your credentials!")
        logger.debug(space_details)
        raise SystemExit

    logger.info("Getting existing page titles from Confluence space")


    def get_conf_page_titles(start=0):
        conf_all_pages = confluence.get_all_pages_from_space(conf_space_name, start=start, limit=100, expand='space')
        for c in conf_all_pages:
            existing_conf_pages.append(c['title'])
        return len(conf_all_pages)


    conf_page_count = 0
    while True:
        num_pages_returned = get_conf_page_titles(conf_page_count)
        if num_pages_returned > 0:
            conf_page_count += 100
            logger.info("Getting more confluence page titles, catalogued {} so far".format(len(existing_conf_pages)))
        else:
            break

    logger.debug(pprint.pformat(existing_conf_pages))
    logger.info("Found {} existing pages in space".format(len(existing_conf_pages)))
    conf_all_pages = None  # release the cursor from memory

    page_titles_that_already_exist = []

    # compare the page titles from W3 to the existing page titles in Confluence space
    for page in pages_to_download:
        if page['title'] in existing_conf_pages:
            page_titles_that_already_exist.append(page['title'])

    if len(page_titles_that_already_exist) > 0:
        logger.info("Shut it all down, you can't have names in W3 that already exist in Confluence!")
        logger.info("Rename these pages you already have in Confluence and then re-run the script:")
        for p in page_titles_that_already_exist:
            logger.info("    {}".format(p))
        raise SystemExit
    else:
        logger.info("All W3 page names will be unique in Confluence space, good to continue")

    try:
        if use_existing_conf_home_page:
            homepage_id = space_details['homepage']['id']
        else:
            homepage_id = create_conf_page('W3 Backup of {}'.format(wiki_title),
                                           'This is the <b>parent</b> page of the wiki backup. Feel free to delete/modify as you see fit!')
        logger.info("Confluence Homepage ID is {}".format(homepage_id))
    except Exception as e:
        if 'message' in space_details:
            logger.debug(pprint.pformat(space_details['message']))
        logger.error("ERROR: Couldn't find or create homepage for space: {}".format(e), exc_info=True)
        raise SystemExit

logger.info("#" * 20)
logger.info("Looking for attachments on pages")
logger.info("#" * 20)

# dictionary to hold attachment metadata using page id as key
attachments_to_download = {}

# start searching for attachments on the pages collected
for i, x in enumerate(pages_to_download):
    if i % 20 == 0 and i != 0:
        logger.info("Searched {} pages for attachments so far, {} pages with attachments".format(i, len(
            attachments_to_download)))
    # download attachment metadata
    attach_data = requests.get(x['attachments'], cookies=cookies, headers=headers)
    attach_xml = XmlWorker(attach_data.text)
    # make the metadata into a dictionary
    attach_meta = attach_xml.getDict()
    # setup empty list to hold dictionaries of the attachment metadata
    attachment_info = []
    num_of_attachments = attach_meta['feed']['opensearch:totalResults']
    logger.debug("{} -- Num of attachments: {}".format(x['title'], num_of_attachments))

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
    elif num_of_attachments not in ['0', '1']:
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

logger.info("Searched {} pages for attachments, {} pages with attachments".format(number_of_pages_to_download,
                                                                                  len(attachments_to_download)))

if sync_to_confluence and append_wiki_comments:
    logger.info("#" * 20)
    logger.info("Looking for comments on pages")
    logger.info("#" * 20)

    wiki_comment_data = {}

    # start searching for comments on the pages collected
    for i, x in enumerate(pages_to_download):
        if i % 20 == 0 and i != 0:
            logger.info(
                "Searched {} pages for comments so far, {} pages with comments".format(i, len(wiki_comment_data)))
        # download comment metadata
        comment_data = wiki_meta.get_wiki_page_comments(wiki_second_id, x['page_id'])
        comment_xml = XmlWorker(comment_data)
        # make the metadata into a dictionary
        comment_meta = comment_xml.getDict()
        # setup empty list to hold dictionaries of the attachment metadata
        comment_info = []
        num_of_comments = comment_meta['feed']['opensearch:totalResults']
        logger.debug("{} -- Num of comments: {}".format(x['title'], num_of_comments))

        # Ughh, the W3 XML changes if there is exactly one comment
        # Duplicating some code here, come back and cleanup later :(
        if num_of_comments == '1':
            at = comment_meta['feed']['entry']
            comment_info.append({'author': at['author']['name']
                                    , 'published': datetime.strptime(at['published'], "%Y-%m-%dT%H:%M:%S.%fZ")
                                    , 'content': at['content']['#text']})

            wiki_comment_data[x['page_id']] = comment_info

        # # assumes there is more than one attachment
        elif num_of_comments not in ['0', '1']:
            for at in comment_meta['feed']['entry']:
                comment_info.append({'author': at['author']['name']
                                        , 'published': datetime.strptime(at['published'], "%Y-%m-%dT%H:%M:%S.%fZ")
                                        , 'content': at['content']['#text']})

            wiki_comment_data[x['page_id']] = comment_info

    logger.info("Searched {} pages for comments, {} pages with comments".format(number_of_pages_to_download,
                                                                                len(wiki_comment_data)))

# setup local copy directory under wikibackup folder and enter it
script_directory = os.getcwd()
local_wiki_directory = os.path.join(script_directory, 'wikibackup', wiki_title)
os.makedirs(local_wiki_directory, exist_ok=True)
os.chdir(local_wiki_directory)

logger.info("#" * 20)
logger.info("Putting wiki files in {}".format(local_wiki_directory))
logger.info("#" * 20)

confluence_page_mapping = {}


def find_attachment_file_in_list(page_id, file_name):
    conf_att_id = None
    for m in conf_attachment_mapping:
        if m['page_id'] == page_id and m['file_name'] == file_name:
            conf_att_id = m['attach_id']

    return conf_att_id


attachments_formatted = []

possible_link_issues = {}

# create folders for each page and download pages/attachments into the folder
for i, page in enumerate(pages_to_download):
    conf_page_id = None
    os.chdir(local_wiki_directory)
    os.makedirs(page['title'], exist_ok=True)
    pd = requests.get(page['download_url'], cookies=cookies, headers=headers)
    logger.info("({}/{}) Downloading HTML for {}".format(i + 1, number_of_pages_to_download, page['title']))
    with open(os.path.join(os.getcwd(), page['title'], 'index.html'), 'wb') as f:
        f.write(pd.content)

    if sync_to_confluence:
        soup = BeautifulSoup(pd.content, 'html.parser')
        # need to remove some items that cause fits for Confluence xhtml parser
        for meta in soup.find_all('meta'):
            meta.decompose()
        for v in soup.find_all('v:rect'):
            v.decompose()

        if append_w3_history_table:
            w3_stats_info = [page['author'], page['created'], page['modifier'], page['modified'], page['link']]
            td_markup = ""
            for i, m in enumerate(w3_stats_info):
                if i != len(w3_stats_info) - 1:
                    td_markup = td_markup + "<td>" + m + "</td>"
                else:
                    td_markup = td_markup + "<td><a href='" + m + "' target='_blank'>Link</a></td>"
            append_to_markup = """<hr/><b>Original W3 Page Metadata</b><br/><table>
                <colgroup><col/><col/><col/><col/><col/></colgroup>
                <tbody>
                  <tr>
                    <th>Author</th>
                    <th>Created</th>
                    <th>Modifier</th>
                    <th>Modified</th>
                    <th>W3 Link</th>
                  </tr>
                  <tr>""" + td_markup + """
                  </tr>
                </tbody>
              </table>"""

            soup_to_append = BeautifulSoup(append_to_markup, 'html.parser')

            soup.body.append(soup_to_append)

        if append_wiki_comments and page['page_id'] in wiki_comment_data:
            page_comments = wiki_comment_data[page['page_id']]
            td_markup = ""
            # this sorts the comments in the order they were published
            sortedArray = sorted(
                page_comments,
                key=lambda x: x['published'], reverse=False
            )
            for c in sortedArray:
                td_markup = td_markup + "<tr>"
                w3_comments = [c['author'], c['published'].strftime("%m/%d/%Y %H:%M:%S"), c['content']]
                for i, m in enumerate(w3_comments):
                    td_markup = td_markup + "<td>" + str(m) + "</td>"
                td_markup = td_markup + "</tr>"
            append_to_markup = """<p><b>Original W3 Comments</b><br/><table>
                <colgroup><col/><col/><col/></colgroup>
                <tbody>
                  <tr>
                    <th>Author</th>
                    <th>Published</th>
                    <th>Comment</th>
                  </tr>""" + td_markup + """
                </tbody>
              </table></p>"""

            soup_comments_to_append = BeautifulSoup(append_to_markup, 'html.parser')

            soup.body.append(soup_comments_to_append)

        if replace_table_of_contents:
            soup_toc_to_append = BeautifulSoup("""<p>
              <ac:structured-macro ac:name="toc" ac:schema-version="1"/>
            </p>""", 'html.parser')

            for toc in soup.find_all('div', attrs={'name': 'intInfo'}):
                found_toc_in_loop = False
                for i, strs in enumerate(toc.stripped_strings):
                    # the string 'Table of Contents:' should be the first stripped string if it's a W3 TOC
                    if strs == 'Table of Contents:':
                        logger.info("Replacing W3 Table of Contents")
                        found_toc_in_loop = True
                if found_toc_in_loop:
                    # need to replace after the stripped strings loop because we get an error otherwise
                    # since we'd be replacing soup that is still being parsed
                    toc.replace_with(soup_toc_to_append)

        logger.debug(soup.prettify())
        conf_page_id = create_conf_page(page['title'], soup.prettify(), homepage_id)
        confluence_page_mapping[page['page_id']] = {'w3_parent_id': page['parent_id']
            , 'conf_page_id': conf_page_id
            , 'page_title': page['title']}

    # setup integers so we can later decide to update
    images_found_to_replace = 0
    links_found_to_replace = 0
    wiki_links_found_to_replace = 0
    old_wiki_links_found_to_replace = 0
    connection_links_found_to_replace = 0

    # only try to get attachments if they exist for a page id
    if page['page_id'] in attachments_to_download.keys():
        for fi in attachments_to_download[page['page_id']]:
            logger.info('    Downloading {} || size - {}'.format(fi['title'], fi['size_human']))
            ad = requests.get(fi['content'], cookies=cookies, headers=headers)
            with open(os.path.join(os.getcwd(), page['title'], fi['title']), 'wb') as f:
                f.write(ad.content)

            if sync_to_confluence and int(conf_page_id) > 0:
                create_conf_attachment(conf_page_id, fi['title'], os.path.join(os.getcwd(), page['title'], fi['title']))

        if sync_to_confluence and int(conf_page_id) > 0:
            logger.info("Looking for images to replace")
            for image_src in soup.find_all('img'):
                src_of_imageprint = image_src['src']
                if 'w3-connections.ibm.com' in src_of_imageprint:
                    a = urlparse(src_of_imageprint)
                    image_file_name = os.path.basename(a.path)
                    found_attach_id = find_attachment_file_in_list(conf_page_id, image_file_name)
                    if found_attach_id is not None:
                        images_found_to_replace += 1
                        attachments_formatted.append(conf_page_id)
                        new_tag = soup.new_tag("ac:image")
                        conf_image_ref = soup.new_tag("ri:attachment")
                        conf_image_ref['ri:filename'] = image_file_name
                        new_tag.append(conf_image_ref)
                        image_src.replace_with(new_tag)
                        logger.info(
                            "-- Image attachment formatted to Confluence markup in HTML: {}".format(found_attach_id))

            logger.info("Looking for link attachments to replace")
            for link_src in soup.find_all('a', attrs={"lconnwikiparamwikiattachment": True}):
                href_of_link = link_src['lconnwikiparamwikiattachment']
                a_link_text = link_src.text
                found_attach_id = find_attachment_file_in_list(conf_page_id, href_of_link)
                if found_attach_id is not None:
                    links_found_to_replace += 1
                    attachments_formatted.append(conf_page_id)
                    link_replace_html = """<ac:link>
                <ri:attachment ri:filename="{}"/>
                <ac:plain-text-link-body><![CDATA[{}]]></ac:plain-text-link-body>
              </ac:link>""".format(href_of_link, a_link_text)
                    wiki_attach_link_soup = BeautifulSoup(link_replace_html, 'html.parser')
                    link_src.replace_with(wiki_attach_link_soup)
                    logger.info("-- Link attachment formatted to Confluence markup in HTML: {}".format(found_attach_id))

    if sync_to_confluence and replace_w3_wiki_links:
        logger.info("Looking for wiki links to replace")
        for link_src in soup.find_all('a', attrs={"lconnwikiparamwikipage": True}):
            href_of_link = link_src['lconnwikiparamwikipage']
            a_link_text = link_src.text
            wiki_links_found_to_replace += 1
            attachments_formatted.append(conf_page_id)
            reformatted_link = """<ac:link>
                  <ri:page ri:content-title="{}"/>
                  <ac:plain-text-link-body><![CDATA[{}]]></ac:plain-text-link-body>
                </ac:link>""".format(href_of_link, a_link_text)
            wiki_link_soup_to_append = BeautifulSoup(reformatted_link, 'html.parser')
            link_src.replace_with(wiki_link_soup_to_append)
            logger.info("-- Wiki link formatted to Confluence markup in HTML: {}".format(href_of_link))

    if sync_to_confluence and replace_w3_wiki_links:
        for link_src in soup.find_all('a', attrs={"wiki": True, "page": True}):
            if link_src['wiki'] == w3_wiki_id:
                href_of_link = link_src['page']
                a_link_text = link_src.text
                old_wiki_links_found_to_replace += 1
                attachments_formatted.append(conf_page_id)
                reformatted_link = """<ac:link>
                      <ri:page ri:content-title="{}"/>
                      <ac:plain-text-link-body><![CDATA[{}]]></ac:plain-text-link-body>
                    </ac:link>""".format(href_of_link, a_link_text)
                wiki_link_soup_to_append = BeautifulSoup(reformatted_link, 'html.parser')
                link_src.replace_with(wiki_link_soup_to_append)
                logger.info("-- Older Wiki link formatted to Confluence markup in HTML: {}".format(href_of_link))

    if sync_to_confluence and replace_connections_files:
        logger.info("Looking for linked connection files to download")
        for link_src in soup.find_all('a', attrs={"_ic_files_uuid": True}):
            href_of_link = link_src['_ic_files_uuid']
            logger.info("-- Found connection file id {}, getting details".format(href_of_link))
            a_link_text = link_src.text
            try:
                conn_link = requests.get("{}/files/basic/anonymous/api/document/{}/entry".format(w3_host, href_of_link),
                                         cookies=cookies, headers=headers)
                logger.debug(conn_link.text)
                conn_details = XmlWorker(conn_link.text)
                conn_meta = conn_details.getDict()
                conn_file_url = conn_meta['entry']['content']['@src']
                conn_file_title = conn_meta['entry']['td:label']
                conn_file_size = conn_meta['entry']['td:versionMediaSize']
                logger.info("-- Connections file named '{}' is size {}".
                            format(conn_file_title, wiki_meta.convertSize(conn_file_size)))
                # Need to capture the rare scenario where a connections file name is already attached to the page
                # Because it would just overwrite the existing file and breaks the create
                check_conn_file_against_attachments = find_attachment_file_in_list(conf_page_id, conn_file_title)
                if check_conn_file_against_attachments is not None:
                    logger.info("Skipping this connections file as there is already a page attachment with same name.")
                    alert_items.append(
                        "Had to skip downloading connections file {} on page {} as an attachment already exists for it".format(
                            conn_file_title, page['title']))
                    break
                # only attempt if file is under confluence attachment size limit
                if int(conn_file_size) < conf_max_attachment_size:
                    logger.info("    Downloading {} bytes, please wait".format(conn_file_size))
                    os.chdir(os.path.join(local_wiki_directory, page['title']))
                    os.makedirs('connections_files', exist_ok=True)
                    cf = requests.get(conn_file_url, cookies=cookies, headers=headers)
                    # make a directory specific to connections files
                    # in case there are names already used by attachments to the page in W3
                    with open(os.path.join(local_wiki_directory, page['title'], 'connections_files', conn_file_title),
                              'wb') as f:
                        f.write(cf.content)
                    create_conf_attachment(conf_page_id, conn_file_title,
                                           os.path.join(local_wiki_directory, page['title'], 'connections_files',
                                                        conn_file_title))
                    os.chdir(local_wiki_directory)
                    connection_links_found_to_replace += 1
                    attachments_formatted.append(conf_page_id)
                    reformatted_link = """<ac:link>
                      <ri:attachment ri:filename="{}"/>
                      <ac:plain-text-link-body><![CDATA[{}]]></ac:plain-text-link-body>
                    </ac:link>""".format(conn_file_title, a_link_text)
                    wiki_link_soup_to_append = BeautifulSoup(reformatted_link, 'html.parser')
                    # By default, W3 Connection file links have extra HTML (a divider and a view details link)
                    # that we want to get rid of as they serve no purpose in Confluence
                    # Put this in try/except block because it's not important enough to break everything
                    try:
                        for sibling in link_src.find_next_siblings():
                            if isinstance(sibling, NavigableString):
                                pass
                            else:
                                if sibling.name == 'span':
                                    if sibling.has_attr('class'):
                                        if 'lotusDivider' in sibling['class']:
                                            sibling.decompose()
                                            logger.info("Removed superfluous connections link divider")
                                if sibling.name == 'a':
                                    if sibling.has_attr('title'):
                                        if 'View details of {}'.format(conn_file_title) in sibling['title']:
                                            sibling.decompose()
                                            logger.info("Removed superfluous connections link view details")
                    except Exception as e:
                        logger.info(
                            "Had issue cleaning up superfluous W3 Connections link HTML. See log for more details")
                        logger.error(e, exc_info=logger.getEffectiveLevel() == logging.DEBUG)
                    link_src.replace_with(wiki_link_soup_to_append)
                    logger.info(
                        "Connections file link formatted to Confluence markup in HTML: {}".format(conn_file_title))
                else:
                    logger.info(
                        "    Unable to download a file of that size as the Confluence limit is {} bytes, skipping".format(
                            conf_max_attachment_size))
                    alert_items.append(
                        "Had to skip downloading connections file '{}' of {} bytes on page '{}' as it exceeds {} bytes".format(
                            conn_file_title, conn_file_size, page['title'], conf_max_attachment_size))
            except Exception as e:
                logger.info("Could not retrieve/upload Connections file. See log for more details")
                alert_items.append(
                    "Had issues downloading/uploading connections file id '{}' on page '{}'".format(
                        href_of_link, page['title'], ))
                logger.error(e, exc_info=logger.getEffectiveLevel() == logging.DEBUG)

    if sync_to_confluence and find_possible_link_issues:
        link_issues_found = 0
        logger.info("Looking for random IBM links that might cause future issues")
        link_issues_data = []
        for link_src in soup.find_all('a', attrs={"href": True}):
            href_of_link = link_src['href']
            if ('ibm' in href_of_link or 'box.com' in href_of_link) and href_of_link != page['link']:
                link_issues_found += 1
                a_link_text = unicodedata.normalize("NFKD", link_src.text)
                logger.info("-- Found URL: {}".format(href_of_link))
                link_issues_data.append({'url': href_of_link, 'text': a_link_text})
        if link_issues_found > 0:
            possible_link_issues[page['page_id']] = link_issues_data

    if images_found_to_replace > 0 or links_found_to_replace > 0 \
            or wiki_links_found_to_replace > 0 or connection_links_found_to_replace > 0 \
            or old_wiki_links_found_to_replace > 0:
        logger.info("Updating confluence page with image/link sources")
        confluence.update_page(conf_page_id, page['title'], soup.prettify())

    logger.info("#" * 20)


def getConfIdFromW3Id(w3_p_id):
    p_id = None
    for c in confluence_page_mapping.keys():
        if c == w3_p_id:
            p_id = confluence_page_mapping[w3_p_id]['conf_page_id']
    return p_id


if sync_to_confluence:
    logger.info("Starting navigation sync to Confluence")
    # need to manually create the session instead of using confluence module
    # because the atlassian module method for update_page requires a body
    # and we only want to update the parent id
    s = requests.Session()
    conf_headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    # need to update the dictionary to include the parent conf id for each conf page created
    for cid in confluence_page_mapping.keys():
        parent_w3_id = confluence_page_mapping[cid]['w3_parent_id']
        confluence_page_mapping[cid]['conf_parent_id'] = getConfIdFromW3Id(parent_w3_id)

    for i, ids in enumerate(confluence_page_mapping.keys()):
        if i % 20 == 0 and i != 0:
            logger.info("Updated {}/{} Confluence parent ids so far".format(i, number_of_pages_to_download))
        child_conf_id = confluence_page_mapping[ids]['conf_page_id']
        parent_conf_id = confluence_page_mapping[ids]['conf_parent_id']
        page_title = confluence_page_mapping[ids]['page_title']
        vers_num = 2
        # need to set higher version number for pages we already updated with attachments
        if child_conf_id in attachments_formatted:
            vers_num = 3
        else:
            vers_num = 2
        # requires id above 0 because 0 indicates the page was not created (usually due to duplicate name error)
        if parent_conf_id is not None and int(parent_conf_id) > 0:
            data_to_update = {
                'id': child_conf_id,
                'type': 'page',
                'ancestors': [{'type': 'page', 'id': str(parent_conf_id)}],
                'title': page_title,
                "version": {
                    "number": vers_num
                }
            }
            url_string = "{}rest/api/content/{}".format(conf_endpoint, child_conf_id)
            r = s.put(url_string
                      , data=json.dumps(data_to_update)
                      , headers=conf_headers
                      , cookies=conf_cookies
                      )

            logger.debug("Update Parent Page for ::{}:: (id: {}) Status: {}"
                         .format(page_title, child_conf_id, r.status_code))
            if r.status_code != 200:
                logger.debug(r.text)
                alert_items.append(
                    "Had issue updating the parent page for ::{}:: in Confluence, so navigation may be incorrect. See debug logs.".format(
                        page_title))

endSyncTime = datetime.now()
logger.info("Process done in %s seconds", round((endSyncTime - startSyncTime).total_seconds()))

if len(alert_items) > 0:
    logger.info("Found these issues that might warrant your attention")
    for alert in alert_items:
        logger.info("--> {}".format(alert))

if len(pages_to_download) > 0:
    os.chdir(local_wiki_directory)
    with open("results.html", "w", encoding='utf-8') as file:
        pages_rows = ""
        for p in pages_to_download:
            attachment_meta_data = ""
            try:
                for atta in attachments_to_download[p['page_id']]:
                    attachment_meta_data = attachment_meta_data + "<strong>" + atta['title'] + "</strong>" + " || " + \
                                           atta['size_human'] + "<br/>"
            except:
                pass

            possible_link_problems = ""
            try:
                for li in possible_link_issues[p['page_id']]:
                    possible_link_problems = possible_link_problems \
                                             + "<strong><a href={} target='_blank'>{}</a></strong> || {}<br/>".format(li['url'], li['text'], li['url'])
            except:
                pass

            conf_page_id_str = confluence_page_mapping[p['page_id']]['conf_page_id']
            conf_page_link = ""
            try:
                conf_page_id_value = confluence_page_mapping[p['page_id']]['conf_page_id']
                conf_page_link = "<a href='{}display/{}/{}' target='_blank'>{}</a>".format(conf_endpoint, conf_space_name, p['title'], conf_page_id_str)
            except:
                pass

            pages_rows = pages_rows + """
                    <tr>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td><a href="{}">Link</a></td>
                        <td>{}</td>
                        <td>{}</td>
                    </tr>
                    """.format(p['title'], p['page_id'], p['author'], p['created'], p['modifier'], p['modified'],
                               p['parent_id'], conf_page_link, p['link'], attachment_meta_data, possible_link_problems)

        html_to_write = """
<!doctype html>
<html lang="en">
  <head>
    <!-- Required meta tags -->
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">

    <!-- Bootstrap CSS -->
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css" integrity="sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh" crossorigin="anonymous">

    <title>Results from W3 to Confluence Sync</title>
  </head>
  <body>        
    <div class="container-fluid">
    <h3>Downloaded Pages from W3</h3>
        <div class="table-responsive">
        <table class="table table-hover">
          <thead class="thead-dark">
            <tr>
              <th scope="col">Title</th>
              <th scope="col">Page ID</th>
              <th scope="col">Author</th>
              <th scope="col">Created</th>
              <th scope="col">Modifier</th>
              <th scope="col">Modified</th>
              <th scope="col">Parent Page ID</th>
              <th scope="col">Confluence Page ID</th>
              <th scope="col">W3 Link</th>
              <th scope="col">Attachments</th>
              <th scope="col">Possible Link Issues</th>
            </tr>
          </thead>
          <tbody>
            {}
          </tbody>
        </table>
        </div>
    </div>
  </body>
</html>""".format(pages_rows)

        file.write(html_to_write)