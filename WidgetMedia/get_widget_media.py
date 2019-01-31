import imaplib, pandas as pd, datetime
import bs4 as bs

#------Configurations------
today = datetime.date.today().strftime("%d-%b-%Y")
user = 'username@gmail.com'
password = 'password'
imap_url = 'imap.gmail.com'
search_query1 = '(FROM "noreply@wigetmedia.com" SUBJECT "Daily WigetMedia Summary" SINCE "'+today+'")'
header=['Day','Ad_Placement','Impressions','Media_Cost','Media_Cost_for_impressions']
df= pd.DataFrame(columns=header)
#------------------------------

#------Main------

#Setting the connection
con = imaplib.IMAP4_SSL(imap_url)
con.login(user,password)
con.select('INBOX')


#Find all e-mails according to specific searching properties
typ, msgnums1 = con.search(None,search_query1)
if msgnums1 != [b'']:
    for num in msgnums1[0].split()[0:]:
            result, data = con.fetch(num,'(RFC822)')
            raw = data[0][1].decode("utf-8")
            soup = bs.BeautifulSoup(raw, 'html.parser')
            for row in soup.find_all('tr')[1:-1]:
                            df = df.append({
                                'Day'                        : datetime.datetime.strptime(row.contents[1].get_text(),"%Y-%m-%d"),
                                'Ad_Placement'               : row.contents[3].get_text(),
                                'Impressions'                : row.contents[5].get_text(),
                                'Media_Cost'                 : row.contents[7].get_text(),
                                'Media_Cost_for_impressions' : row.contents[9].get_text()}, ignore_index=True)

coolaResult = df