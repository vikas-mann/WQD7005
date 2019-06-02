# Adapted from Yong's initial version
# !/usr/bin/env python3
import requests
import lxml.html as lh
import pandas as pd
from pandas_gbq import to_gbq
from pandas_gbq import read_gbq
from google.oauth2 import service_account
from twython import Twython
import re
import feedparser
from dateutil.parser import parse

import datetime
import time


def ScrapMyStock():  # scrapper for MalaysiaStocks.biz

    ## Initialize variables
    letters = list('3ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    ##letters = list('DEF')
    
    companies = pd.DataFrame()

    ## loop over multiple HTML pages
    for p in letters:
        ## construct the URL request that loops through multiple pages
        ## HTTP get the url returning string
        url = 'https://www.malaysiastock.biz/Listed-Companies.aspx'
        PARAMS = {'value': p, 'type': 'A'}
        page = requests.get(url, params=PARAMS)
        ## Parse the page into lxml object
        ## extract only desired section (html table) and convert to string
        root = lh.fromstring(page.content)
        table = lh.tostring(root.xpath('//*[@id="MainContent_tStock"]')[0])

        ## convert the 'first found' html table string into dataframe
        ## merge them into final dataframe
        srapdf = pd.read_html(table, header=0)[0]
        frames = [companies, srapdf]
        companies = pd.concat(frames, ignore_index=True)

    ## Split Company into 3 columns
    companies[['Quote', 'Code', 'Name']] = companies.Company.str.extract('(.*?)\s\((.*?)\)(.*)', expand=True)

    ## Convert '-' to None, then numbers
    cols = ['PE', 'ROE']
    for i in cols:
        companies[i] = companies[i].replace({'-': None}).astype(float)

    ## MarketCap variable treatment, Convert '-' to None, then Convert b to '000 then integer
    num_replace = {  ## Convert B to '000
        'b': 'e3',
        'm': 'e0',
    }

    def str_to_num(s):  # standardize unit in 'million'
        if s == None: return None
        if s[-1] in num_replace:
            s = s[:-1] + num_replace[s[-1]]
        return int(float(s))

    companies['Market Cap'] = companies['Market Cap'].replace({'-': None})
    companies['Market Cap'] = companies['Market Cap'].apply(str_to_num)

    ## Reformat, Rename Cols before returning
    companies = companies.replace({pd.np.nan: None})  # convert NaN to none, otherwise GBQ insert will fail
    companies = companies.rename(columns={'Last Price': 'LastPrice',
                                          'Market Cap': 'MarketCap'})  # convert NaN to none, otherwise GBQ insert will fail
    cols = ['Quote', 'Code', 'Name', 'Sector', 'MarketCap', 'LastPrice', 'PE', 'DY', 'ROE']
    return companies[cols]


def ScrapTheStar(quotes):  # scapper for TheStar.com

    ## initialize vars
    companies = pd.DataFrame()

    ## Loop through each quote page, build the dataframe
    for q in quotes:
        url = 'https://www.thestar.com.my/business/marketwatch/stocks/'
        PARAMS = {'qcounter': q}
        page = requests.get(url, params=PARAMS)
        root = lh.fromstring(page.content)
        table = lh.tostring(root.xpath('//*[@id="slcontent_0_ileft_0_info"]/table')[0])
        srapdf = pd.read_html(table, header=0)[0]
        frames = [companies, srapdf]
        companies = pd.concat(frames, ignore_index=True)
        # if q == 'ADVPKG': break

    ## naming quote, needed for joinining
    companies['Quote'] = quotes

    ## Split Buy and Sell columns
    companies[['BuyAt', 'BuyVol']] = companies["Buy/Vol ('00)"].str.extract('(.*?)\s/\s(.*?$)', expand=True)
    companies[['SellAt', 'SellVol']] = companies["Sell/Vol ('00)"].str.extract('(.*?)\s/\s(.*?$)', expand=True)

    ## Convert '- to None, then convert to number
    cols = ['Open', 'Low', 'High']
    for i in cols:
        companies[i] = companies[i].replace({'-': None}).astype(float)

    ## Remove comma, then convert to number,
    cols = ['BuyAt', 'BuyVol', 'SellAt', 'SellVol']
    for i in cols:
        companies[i] = companies[i].str.replace(',', '').astype(float)

    ## Reformat DataFrame before returning
    companies = companies.replace({pd.np.nan: None})  # convert NaN to none, otherwise GBQ insert will fail
    companies.rename(columns={"Vol ('00)": 'Volume', 'Chg %': 'Chg%'}, inplace=True)
    cols = ['Quote', 'Open', 'Low', 'High', 'Chg', 'Chg%', 'Volume', 'BuyAt', 'BuyVol', 'SellAt', 'SellVol']
    return companies[cols]


def ScrapMajorIndices(project_id, dataset_id, cred ):
    
    table_id   = 'StocksTweetFeed'
    
    # Scrape data from url and place in df
    url = 'https://finance.yahoo.com/world-indices/'
    page = requests.get(url)
    root = lh.fromstring(page.content)
    table = lh.tostring(root.xpath('//*[@id="yfin-list"]/div[2]/div/div/table')[0])
    df3 = pd.read_html(table, header=0)[0]
    
    ## Volume variable treatment, Convert b to '000 then integer
    num_replace = {  ## Convert B to '000
        'B': 'e3',
        'M': 'e0',
    }
    
    def str_to_num(s):  # standardize unit in 'million'
        if s == None: return None
        if s[-1] in num_replace:
            s = s[:-1] + num_replace[s[-1]]
        return float(s)
    
    df3['Volume'] = df3['Volume'].apply(str_to_num)
    
    ## Remove % in '% Change' variable and comma in 'Change' variable
    df3['% Change'] = df3['% Change'].str.replace('%', '')
    df3['Change'] = df3['Change'].replace(',', '')
    
    ## Rename columns and change data type
    df3.rename(columns={'Last Price': 'LastPrice'}, inplace=True)
    df3.rename(columns={'% Change': 'ChgPct'}, inplace=True)
    
    ## Type To Number Conversion
    df3['ChgPct'] = df3['ChgPct'].astype(float)
    #df3['LastPrice'] = df3['LastPrice'].astype(float)
    df3['Change'] = df3['Change'].astype(float)
    #df3['Volume'] = df3['Volume'].astype(float)
    
    ## Remove empty columns
    cols = ['Symbol','Name','LastPrice','Change','ChgPct','Volume']
    df3 = df3[cols]
    
    ## Create Timestamp
    df3['UpdateDate'] = pd.Timestamp.utcnow()   #timestamp
    df3['UpdateDate'] = df3.UpdateDate.astype('datetime64[ms]') #convert type from object to datetime64
    
    ## Upload to GBQ
    InsertgBQ(df3, project_id, dataset_id, table_id, cred)
    
    return df3


def ScrapTweets(quotesDF, project_id, dataset_id, cred ):
    
    table_id   = 'StocksTweetFeed'
    
    ## initialize vars
    tweets = pd.DataFrame([], columns = ['Quote', 'Date', 'TweetFullText', 'ReTweetCount', 'FavoriteCount', 'ID'])

    twitter1 = Twython('BiqNFiTnYLGxlV7ysFHn75TKe', 'HaX8s6kjJQyngl66wgtnagkqubzXSIc1UTs9AmNEobZIg64OnU',
                  '64235844-2WETYggByFwH1ysWl8P9BvKQlM5tKHQStquhBeOgx', 'Ds4P61ZPzBA3mxzNr6TKVuXsWFpaEl5QIGMTfrbVPH6Nl')
    
    twitter2 = Twython('O6fWirONrJ0xutYLmVH0EqfH6', 'CsE9OLL2zQNT9YtMt8w6Mbz23ptBGHMqE6sBLr8EC13ZCDAcrw',
                  '3246441265-0QjgreDhywHo23SxKrrAVqCkaPQRTJ4z3rLcFd9', 'gIG2tc9xu2Pen3pkNxvKjZ8QLYu7Xle2P7ZLleuFnsoGH')
    
    twitter3 = Twython('jkAOYhfIkoTMskWYJMMuJvfDg', '1tBiVCj51toO9PED2IXQROjaDevE60Z8Md9etotrvJFUsFBvyU',
                  '286598071-X11nsQjyR393qZXd9AOWjdNLRU7EkmrCqhYkYOG0', 'w4srsI5elbXaxQGXLUvvYPaiHfNmWuDLsMzN7moMQlvzZ')
                
    
    try: 
        
        twitter = twitter1
        
        # default since_id to use
        SinceID = '1099010461440315394'
        
        ## read all existing enteries and find since_id to use
        sql = 'SELECT  max(ID) MaxID, Quote FROM '+dataset_id+'.'+table_id+' group by Quote'        
        MaxQuoteIDs = read_gbq(sql, project_id=project_id, credentials=cred)     
        
        # counter to switch Tweeter authorization
        twCount = 0
        switchCount = 125 # threshold for tweeter api limit
       
        ## Loop through each quote page, build the dataframe
        for index, row in quotesDF.iterrows():
            
            twCount +=1
            
            ## keep switching twitter auth after every n API calls
            if twCount > switchCount and twCount <= 2*switchCount:                
                twitter = twitter2 
            elif twCount > (2*switchCount) and twCount <= 3*switchCount :
                twitter = twitter3
            elif twCount > (3*switchCount):
                # make it sleep for 15 mins
                time.sleep(15 *60)
                twitter = twitter1
                twCount = 0
                        
            SinceIDlocal = SinceID
            
            # does entry exists already, if yes.. choose the max since_id
            if (len(MaxQuoteIDs.loc[MaxQuoteIDs.Quote == row['Quote']]) > 0):
                #print(MaxQuoteIDs.loc[MaxQuoteIDs.Quote == row['Quote'], 'MaxID'].iloc[0].astype(str))
                SinceIDlocal = MaxQuoteIDs.loc[MaxQuoteIDs.Quote == row['Quote'], 'MaxID'].iloc[0].astype(str)
                            
            ##print(row['Quote'], row['Name'])    
            searchString = '%24' + row['Quote'] ##+ ' OR ' + row['Name']
                        
            # load the search results
            results = twitter.cursor(twitter.search, q= searchString, since_id=SinceIDlocal)            
            
            # iterate and save to BQ
            count=0
            
            try:
                for result in results:   
                    print(twCount, ' - ',result["created_at"], result["text"])
                    count +=1
                    
                    tweetRow = {'Quote':row['Quote'],'Date':result["created_at"],'TweetFullText':result["text"], 'ReTweetCount':result["retweet_count"], 'FavoriteCount':result["favorite_count"] , 'ID':result["id"]}
                    tweets = tweets.append(tweetRow, ignore_index=True)
                    
                    # limit, else difficult to control for few stocks
                    if count == 150:
                        break
                    
            except Exception as e:
                print(twCount, ' - ', 'No More tweets. ' + str(e)) 
               
        # convert date
        tweets['Date'] = tweets.Date.astype('datetime64[ms]') #convert type from object to datetime64
        tweets['Quote'] = tweets['Quote'].astype(str)
        tweets['ReTweetCount'] = tweets['ReTweetCount'].astype(int)
        tweets['FavoriteCount'] = tweets['FavoriteCount'].astype(int)
        tweets['ID'] = tweets['ID'].astype('int64')
        tweets['TweetFullText'] = tweets['TweetFullText'].astype(str)
        
        # print(len(tweets.index))
                    
         # do the insert now..
        InsertgBQ(tweets, project_id, dataset_id, table_id, cred)
        
    except Exception as e:
           print(str(e))
        
    
    return tweets    

def ScrapBusinessNews(project_id, dataset_id, cred):
    
    table_id = 'BusinessNews'
    
    output_to_csv = False
    rss_source = {'thestarbusiness':'http://www.thestar.com.my/rss/business/business-news/',
              'malaysiakini':'https://www.malaysiakini.com/en/columns.rss',
              'theedge':'http://www.theedgemarkets.com/mymalaysia.rss',
              'sunbusiness':'https://www.thesundaily.my/rss/business'}
    
    news_url = 'https://www.malaysiastock.biz/Corporate-Infomation.aspx?securityCode='
    stocklist_df=pd.read_csv('stockinfo.csv')
    today = datetime.date.today()
    timestamp = pd.Timestamp.utcnow()

    #initialize main variables
    all_rss = []
    rssDF = pd.DataFrame(columns=['Code','UpdateDate','Title','Link'])
    newsDF = pd.DataFrame(columns=['Code','UpdateDate','Title','Link'])
    
    #get rss news and only keep those that are from current date
    for source in rss_source.keys():
        for item in getrss(rss_source[source]):
            print('INFO: Getting rss from ',rss_source[source])
            if parse(item['published']).date()==today:
                all_rss.append(item)
        
    #save all_rss into rssDF dataframe
    for i in range(0,len(all_rss)):
        rssDF.loc[i] = [None ,timestamp ,all_rss[i]['title'],all_rss[i]['links'][0]['href']]
    
    #get news headlines for each stock code
    for code in list(stocklist_df['Code']):
        if code == '5099': break
        tmpdf = getnews(str(code),news_url)
        for i in range(0, len(tmpdf['UpdateDate'])):
            if list(tmpdf['UpdateDate'])[i]==today.strftime('%d %b'):
                tmpdf['UpdateDate'][i] = timestamp
                newsDF = newsDF.append(tmpdf.iloc[[i]])

    #enable output to csv
    if output_to_csv:
        tdate = datetime.date.today().isoformat()
        filename1 = 'rss-'+str(tdate)+'.csv'
        filename2 = 'stocknews-'+str(tdate)+'.csv'
        rssDF.to_csv(filename1, encoding='utf-8', index=False)
        newsDF.to_csv(filename2, encoding='utf-8', index=False)

    ## Combine both RSS and News Into one DataFrame
    frames    = [rssDF, newsDF]
    df = pd.concat(frames, ignore_index=True)
    df['UpdateDate'] = pd.Timestamp.utcnow()   #timestamp
    df['UpdateDate'] = df.UpdateDate.astype('datetime64[ms]') #convert type from object to datetime64
    
    
    ## Upload to GBQ
    InsertgBQ(df, project_id, dataset_id, table_id, cred )
    
    return df

def ScrapForex(project_id, dataset_id, cred):  # Scrapper for major currency forex
    
    table_id   = 'MajorForex'
    
    # get current date
    #theDateStr = datetime.date.today().strftime("%Y-%m-%d")
    today = pd.to_datetime(datetime.date.today().strftime("%Y-%m-%d"))
    theDate = today
    
    # default date to use
    MaxDate = pd.to_datetime("2019-03-01")
        
    # get the last date from DB?
    sql = 'SELECT  max(Date) Date FROM '+dataset_id+'.'+table_id        
    MaxDates = read_gbq(sql, project_id=project_id, credentials=cred)
        
    if (pd.isnull(MaxDates['Date'][0])):
        theDate = MaxDate
    else:
        MaxDate = MaxDates['Date'][0]
        theDate = MaxDate + pd.DateOffset(1)
                             
    while theDate <= today:
        
        # Scrap data from url and place in dataframe
        url = 'https://www.xe.com/currencytables/?from=MYR&date=' + theDate.strftime("%Y-%m-%d")        
        if theDate == today:
            url = 'https://www.xe.com/currencytables/?from=MYR'
            
        print(url)
        page = requests.get(url)
        root = lh.fromstring(page.content)
        table = lh.tostring(root.xpath('//*[@id="historicalRateTbl"]')[0])
        srapdf = pd.read_html(table, header=0)[0]
        dfForex = pd.DataFrame(srapdf)
            
        dfForex = dfForex.rename(columns={list(dfForex)[0]: 'CurrencyCode',
                                              list(dfForex)[1]: 'CurrencyName',
                                              'Units per MYR': 'UnitsPerMYR',
                                              'MYR per Unit': 'MYRPerUnit'})
            
                
        dfForex['UnitsPerMYR'] = dfForex['UnitsPerMYR'].astype(float)
        dfForex['MYRPerUnit'] = dfForex['MYRPerUnit'].astype(float)
        
        dfForex['Date'] = theDate 
        
        print(dfForex)
        
        # do the insert now..
        InsertgBQ(dfForex, project_id, dataset_id, table_id, cred)
        
        # increment Date
        theDate =  theDate + pd.DateOffset(1)  
        
    
    return True

def ScrapForumPosts(project_id, dataset_id, cred):
    
    table_id = 'ForumPosts'
    
    header = {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    timestamp = pd.Timestamp.utcnow()
    df = pd.DataFrame(columns=['UpdateDate','StockName','Posts'])
    count = 0
    
    for i in range(1,50):
        #time.sleep(1)
        url = "https://klse.i3investor.com/jsp/scl/forum.jsp?fp="+str(i)+"&c=1"
        page = requests.get(url, headers=header)
        root = lh.fromstring(page.content)
        table = lh.tostring(root.xpath("//div//table[@id='mainforum']")[0])
        scrapdf = pd.read_html(table, header=0)[0]
    
    for s,p in zip(scrapdf['Discussion Threads'], scrapdf['Posts']):
        try:
            stockname = re.search(":\s*\[(.*)\]\s*:",s).group(1)
            df.loc[count] = [timestamp, stockname, p]
            count+=1
        except:
            continue
        
    if len(df) > 0:
    
        ## Define schema
        schema = [{'name': 'UpdateDate', 'type': 'TIMESTAMP'},
                  {'name': 'Quote', 'type': 'STRING'},
                  {'name': 'Posts', 'type': 'INTEGER'}]
    
        ## Name columns
        #cols = ['UpdateDate', 'Quote', 'Posts']
    
        #insert into BQ
        InsertgBQ(df, project_id, dataset_id, table_id, cred, schema )
                
    return df

def ScrapCommodities(project_id, dataset_id, cred):
    
    table_id = 'Commodities'
    
    # Scrape data from url and place in df
    url = 'https://finance.yahoo.com/commodities'
    page = requests.get(url)
    root = lh.fromstring(page.content)
    table = lh.tostring(root.xpath('//*[@id="yfin-list"]/div[2]/div/div/table')[0])
    df3 = pd.read_html(table, header=0)[0]

    ## Volume variable treatment, Convert b to '000 then integer
    # num_replace = {  ## Convert B to '000
    #     'B': 'e3',
    #     'M': 'e0',
    # }

    # def str_to_num(s):  # standardize unit in 'million'
    #     if s == None: return None
    #     if s[-1] in num_replace:
    #         s = s[:-1] + num_replace[s[-1]]
    #     return float(s)

    # df3['Volume'] = df3['Volume'].apply(str_to_num)

    ## Remove % in '% Change' variable
    df3['% Change'] = df3['% Change'].str.replace('%', '')

    ## Rename columns and change data type
    df3.rename(columns={'Last Price': 'LastPrice'}, inplace=True)
    df3.rename(columns={'% Change': 'ChgPct'}, inplace=True)

    ## Type To Number Conversion
    df3['ChgPct'] = df3['ChgPct'].astype(float)

    ## Remove empty columns
    cols = ['Symbol', 'Name', 'LastPrice', 'Change', 'ChgPct', 'Volume']
    df3 = df3[cols]

    ## Define schema
    schema = [{'name': 'Symbol', 'type': 'STRING'},
              {'name': 'Name', 'type': 'STRING'},
              {'name': 'LastPrice', 'type': 'FLOAT'},
              {'name': 'Change', 'type': 'FLOAT'},
              {'name': 'ChgPct', 'type': 'FLOAT'},
              {'name': 'Volume', 'type': 'INTEGER'},
              {'name': 'UpdateDate', 'type': 'TIMESTAMP'}]

    ## Create Timestamp
    df3['UpdateDate'] = pd.Timestamp.utcnow()  # timestamp

    ## Name columns
    cols = ['Symbol', 'Name', 'LastPrice', 'Change', 'ChgPct', 'Volume', 'UpdateDate']

    ## Upload to GBQ
    InsertgBQ(df3[cols], project_id, dataset_id, table_id, cred, schema )
    
    return df3

def _getrss(url):
    newsfeed = feedparser.parse(url)
    result = newsfeed.entries
    return result

def _getnews(stocknum, news_url):
    url=news_url+stocknum
    page=requests.get(url)
    root=lh.fromstring(page.content)
    charset='iso-8859-1'
    #table=lh.tostring(root.xpath('//*[@id="ctl17_tbCorpHeadline"]')[0], encoding='iso-8859-1')
    
    try:
        tblink = root.xpath('//*[@id="ctl17_tbCorpHeadline"]')[0]
        date = [l.encode(charset).decode('utf8') for l in tblink.xpath('//tr[@class="line"]//td/text()')]
        title = [l.xpath('text()')[0].encode(charset).decode('utf8') for l in tblink.xpath('//tr[@class="line"]//a[contains(@href,"newsID")]')]
        href = [l.encode(charset).decode('utf8') for l in tblink.xpath('//tr[@class="line"]//a[contains(@href,"newsID")]/@href')]
        print('INFO: Getting stock ',stocknum)
        stack=[[stocknum,x,y,z] for x,y,z in zip(date, title, href)]
        df=pd.DataFrame(stack, columns=['Code' ,'UpdateDate','Title','Link'])
    #some stock codes don't exist or the page exists but there are no news items
    #in this case, return empty dataframe and print error
    except:
        df=pd.DataFrame(columns=['Code','UpdateDate','Title','Link'])
        print("ERROR: could not get news items for %s" %stocknum)
    return df




def _InsertgBQ(theDF, project_id, dataset_id, table_id, cred, schema = None):
    
    if len(theDF.index) > 0:
        
        if schema is None:
        
            # insert into BQ
            to_gbq( theDF, project_id = project_id,
                destination_table = dataset_id + "." + table_id,
                if_exists = 'append',
                credentials  = cred)
        else:
            
            # insert into BQ
            to_gbq( theDF, project_id = project_id,
                destination_table = dataset_id + "." + table_id,
                if_exists = 'append',
                table_schema=schema,
                credentials  = cred)
        

### Let's Run It
########################################################################

#  BQ initialization - YONG
project_id = 'datamining-118118'
dataset_id = 'Stocks'
cred = service_account.Credentials.from_service_account_file (
    'gbqadminsa-datamining.json',
)

#  BQ initialization - VIKAS
#project_id = 'vikas-220314'
#dataset_id = 'DataMining'
#cred = service_account.Credentials.from_service_account_file (
#        'Vikas-846a090a47f3.json',
#        )
        
# scrab all stocks
df1 = ScrapMyStock()  # get first df
df2 = ScrapTheStar(df1.Quote)  # get second df
dfStocks = pd.merge(df1, df2, on='Quote')  # join both df

# get all tweets
dfTweet = ScrapTweets(df1, project_id, dataset_id,  cred)   

# get all major Indices
dfMajorIndices = ScrapMajorIndices()  # get third f

# get all major Forex
ScrapForex(project_id, dataset_id, cred)

# get forum posts
dfForumPost = ForumPosts(project_id, dataset_id, cred)

# get commodities 
dfCommodities = Commodities(project_id, dataset_id, cred)

# get business News
dfNews = BusinessNews(project_id, dataset_id, cred)