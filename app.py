import streamlit as st
import re
import requests
import requests.exceptions
from urllib.parse import urlsplit, urljoin
from lxml import html
import sys
import csv
import requests
import urllib
import pandas as pd
from requests_html import HTML
from requests_html import HTMLSession

class EmailCrawler:

    processed_urls = set()
    unprocessed_urls = set()
    emails = set()

    def __init__(self, website: str):
        self.website = website
        self.unprocessed_urls.add(website)
        self.headers = {
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/78.0.3904.70 Chrome/78.0.3904.70 Safari/537.36',
        }
        self.base_url = urlsplit(self.website).netloc
        self.outputfile = self.base_url.replace('.','_')+'.csv'
        # we will use this list to skip urls that contain one of these extension. This will save us a lot of bandwidth and speedup the crawling process
        # for example: www.example.com/image.png --> this url is useless for us. we cannot possibly parse email from images and all other types of files.
        self.garbage_extensions = ['.aif','.cda','.mid','.midi','.mp3','.mpa','.ogg','.wav','.wma','.wpl','.7z','.arj','.deb','.pkg','.rar','.rpm','.tar.gz','.z','.zip','.bin','.dmg','.iso','.toast','.vcd','.csv','.dat','.db','.dbf','.log','.mdb','.sav','.sql','.tar','.apk','.bat','.bin','.cgi','.pl','.exe','.gadget','.jar','.py','.wsf','.fnt','.fon','.otf','.ttf','.ai','.bmp','.gif','.ico','.jpeg','.jpg','.png','.ps','.psd','.svg','.tif','.tiff','.asp','.cer','.cfm','.cgi','.pl','.part','.py','.rss','.key','.odp','.pps','.ppt','.pptx','.c','.class','.cpp','.cs','.h','.java','.sh','.swift','.vb','.ods','.xlr','.xls','.xlsx','.bak','.cab','.cfg','.cpl','.cur','.dll','.dmp','.drv','.icns','.ico','.ini','.lnk','.msi','.sys','.tmp','.3g2','.3gp','.avi','.flv','.h264','.m4v','.mkv','.mov','.mp4','.mpg','.mpeg','.rm','.swf','.vob','.wmv','.doc','.docx','.odt','.pdf','.rtf','.tex','.txt','.wks','.wps','.wpd']
        self.email_count = 0

    def crawl(self):
        """
        It will continue crawling untill the list unprocessed urls list is empty
        """

        url = self.unprocessed_urls.pop()
        st.write("CRAWL : {}".format(url))
        self.parse_url(url)


        if len(self.unprocessed_urls)!=0:
            self.crawl()
        else:
            st.write('End of crawling for {} '.format(self.website))
            st.write('Total urls visited {}'.format(len(self.processed_urls)))
            st.write('Total Emails found {}'.format(self.email_count))
            st.write('Dumping processed urls to {}'.format(self.base_url.replace('.','_')+'.txt'))
            with open(self.base_url.replace('.','_')+'.txt' ,'w') as f:
                f.write('\n'.join(self.processed_urls))

    def parse_url(self, current_url: str):
        """
        It will load and parse a given url. Loads it and finds all the url in this page. 
        It also filters the urls and adds them to unprocessed url list.
        Finally it scrapes the emails if found on the page and the updates the email list

        INPUT:
            current_url: URL to parse
        RETURN:
            None
        """

        #we will retry to visit a url for 5 times in case it fails. after that we will skip it in case if it still fails to load
        response = requests.get(current_url, headers=self.headers)
        tree = html.fromstring(response.content)
        urls = tree.xpath('//a/@href')  # getting all urls in the page
        

        #Here we will make sure that we convert the sub domain to full urls
        # example --> /about.html--> https://www.website.com/about.html
        urls = [urljoin(self.website,url) for url in urls]
        # now lets make sure that we only include the urls that fall under our domain i.e filtering urls that point outside our main website.
        urls = [url for url in urls if self.base_url == urlsplit(url).netloc]


        #removing duplicates
        urls = list(set(urls))
        
        
        #filtering  urls that point to files such as images, videos and other as listed on garbage_extensions
        #Here will loop through all the urls and skip them if they contain one of the extension
        parsed_url = []
        for url in urls:
            skip = False
            for extension in self.garbage_extensions:
                if not url.endswith(extension) and  not url.endswith(extension+'/'):
                    pass
                else:
                    skip = True
                    break
            if not skip:
                parsed_url.append(url)

        # finally filtering urls that are already in queue or already visited
        for url in parsed_url:
            if url not in self.processed_urls and url not in self.unprocessed_urls:
                self.unprocessed_urls.add(url)


        #parsing email
        self.parse_emails(response.text)
        # adding the current url to processed list
        self.processed_urls.add(current_url)

        

    def parse_emails(self, text: str):
        """
        It scans the given texts to find email address and then writes them to csv
        Input:
            text: text to parse emails from
        Returns:
            bool: True or false (True if email was found on page)
        """
        # parsing emails and then saving to csv
        emails = set(re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', text, re.I))
        #TODO: sometime "gFJS3amhZEg_z39D5EErVg@2x.png" gets accepted as email with the above regex. so for now i will check if email ends with jpeg,png and jpg

        for email in emails:
            skip_email = False
            for checker in ['jpg','jpeg','png']:
                if email.endswith(checker):
                    skip_email = True
                    break

            if not skip_email:    
                if email not in self.emails:
                    with open(self.outputfile, 'a', newline='') as csvf:
                        csv_writer = csv.writer(csvf)
                        csv_writer.writerow([email])
                    self.email_count +=1
                    self.emails.add(email)
                    st.write(' {} Email found {}'.format(self.email_count,email))

        if len(emails)!=0:
            return True
        else:
            return False

def get_source(url):
    """Return the source code for the provided URL. 

    Args: 
        url (string): URL of the page to scrape.

    Returns:
        response (object): HTTP response object from requests_html. 
    """

    try:
        session = HTMLSession()
        response = session.get(url)
        return response

    except requests.exceptions.RequestException as e:
        st.write(e)

def scrape_google(query):

    query = urllib.parse.quote_plus(query)
    response = get_source("https://www.google.com.om/search?q=" + query)

    links = list(response.html.absolute_links)
    google_domains = ('https://www.google.', 
                      'https://google.', 
                      'https://webcache.googleusercontent.', 
                      'http://webcache.googleusercontent.', 
                      'https://policies.google.',
                      'https://support.google.',
                      'https://maps.google.')

    for url in links[:]:
        if url.startswith(google_domains):
            links.remove(url)

    return links


st.title('CP Lead Extraction')
#st.image('./header.jpg')
st.write('Powered by Constructions Platform LLC')
query = st.text_input('Enter your Keyword for lead extraction')
type = st.selectbox('Select Algorithm?',('Light_Search', 'Deep_Search'))
count = st.slider('Enter count of websites you want to extract leads from')
if st.button('submit'):
    if type=='Deep_Search':
        urls = scrape_google(query)
        for i in range(0,count):
            crawl = EmailCrawler(urls[i])
            crawl.crawl()
    else:
        def get_urls(tag, n, language):
            for i in range (n):
                urls = search(tag, num_results=i, lang=language)
            return urls

        def remove_dup_email(x):
            return list(dict.fromkeys(x))

        def remove_dup_phone(x):
            return list(dict.fromkeys(x))

        def get_email(html):
            try:
                email = re.findall("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,3}",html)
                nodup_email = remove_dup_email(email)
                return nodup_email
            except:
                 pass

        def get_phone(html):
            try:
                phone = re.findall(r"(\d{2} \d{3,4} \d{3,4})", html)
                phone1= re.findall(r"((?:\d{2,3}|\(\d{2,3}\))?(?:\s|-|\.)?\d{3,4}(?:\s|-|\.)\d{4})",html)
                for p in phone1:
                    phone.append(p)
                nodup_phone = remove_dup_phone(phone)
                return nodup_phone
            except:
                pass
        urls = get_urls(query, count, 'en')

        for url in urls:
            res = requests.get(url,headers=headers)
            st.write('searched home url',res.url)
            if res.status_code == 429:
                time.sleep(int(response.headers["Retry-After"]))

        # parse the response
        
            info = BeautifulSoup(res.text,'lxml')


        # extract contact data from home url

            emails_home = get_email(info.get_text())
            phones_home = get_phone(info.get_text())

            emails_f = emails_home
            phones_f = phones_home

        
        # create a data structure to store the contacts

            contacts_f = {'website':res.url,'Email':'','Phone':''}

        # extract contact of the link if available
        try:
            contact = info.find('a', text = re.compile('contact', re.IGNORECASE))['href']
            if 'http' in contact:
                contact_url = contact
            else:
                contact_url = res.url[0:-1] + contact

            # searching contact URL
            
            res_contact = requests.get(contact_url)

            contact_info = BeautifulSoup(res_contact.text, 'lxml').get_text()


            st.write('searched contact url:', res_contact.url)

            # extract contact data
            
            emails_contact = get_email(contact_info)
            phones_contact = get_phone(contact_info)

            #combining email contacts and email home into a single list

            emails_f = emails_home

            for ele1 in emails_contact:
                emails_f.append(ele1)

            #combining phone contacts and phone contacts into a single list
        
            phones_f = phones_home

            for ele2 in phones_contact:
                phones_f.append(ele2)
            
        except:
            pass


                
        # removing duplicates

        emails_f = remove_dup_email(emails_f)
        phones_f = remove_dup_email(phones_f)

        contacts_f['Email']= emails_f
        contacts_f['Phone']= phones_f
        
        # converting into a data set
        
        st.write('\n', json.dumps(contacts_f, indent=2))

        # dumping the data into the csv file

        with open('organization_info.csv', 'a') as f:

            #creater csv writer object

            writer = csv.DictWriter(f, fieldnames=contacts_f.keys())

            #writer.writeheader()

            #append rows to the csv

            writer.writerow(contacts_f)
        df = pd.read_csv('organization_info.csv')
        st.write(df)
        def get_table_download_link_csv(df):
            csv = df.to_csv().encode()
            b64 = base64.b64encode(csv).decode()
            href = f'<a href="data:file/csv;base64,{b64}" download="captura.csv" target="_blank">Download csv file</a>'
            return href
        st.markdown(get_table_download_link_csv(df), unsafe_allow_html=True)
            
