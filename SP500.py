#!/usr/bin/env python
# coding: utf-8

# # Libraries

# In[1]:


# data science
import pandas as pd
import numpy as np

# finacials 
import yfinance as yf
from yahoofinancials import YahooFinancials

# sql 
import mysql.connector

# other
from datetime import datetime
from tqdm import tqdm, trange


# # SQL Connection

# In[2]:


db = mysql.connector.connect(host        = "localhost",
                             user        = "root",
                             passwd      = "xxxxxxxx",
                             auth_plugin = "mysql_native_password",
                             database    = "investing"
                            )

my_cursor = db.cursor(buffered = True)

today    = datetime.today()


# # Data Collection

# ## Stock Information from SQL

# In[3]:


# selecting stock info
my_cursor.execute("SELECT stock_id, ticker FROM stock")

# placeholder list
full_stk = []

for x in my_cursor:
    full_stk.append(x)
    
# transform to df
full_stk = pd.DataFrame(full_stk, columns = ["stock_id", "ticker"])
full_stk.head()

# ## Financial Information

# In[4]:


# list of stocks to loop over
stocks = list(full_stk["ticker"])
print(stocks[:10])

# empty list for failed extractions
fails  = []

# empty item for df
all_stats = None

# looping over stocks
for i in trange(len(stocks)):
    
    try:
        # downloading statistical data
        stock_stats = pd.json_normalize(YahooFinancials(stocks[i]).get_key_statistics_data()[stocks[i]])

        # add additional metrics

        # stock_stats["pricetoSales"] = YahooFinancials(stocks[i]).get_price_to_sales()
        
        stock_stats["ticker"]       = stocks[i]


        # either create or add to df
        if all_stats is None:
            all_stats = stock_stats
        else:
            all_stats = pd.concat([all_stats, stock_stats], axis = 0)

    
    except:
        
        # listing failed attempts
        fails.append(stocks[i])


# In[5]:
print(len(fails))
# making copy of data
stats = all_stats.copy()

# converting to numeric values
for i in stats:
    try:
        stats[i] = pd.to_numeric(stats[i])
    except:
        continue
        
# dropping columns with no information
for col in stats:
    if stats[col].isnull().sum() == len(stats):
        stats = stats.drop(col, axis = 1)
        
# adding trailing P/E
stats["trailingPE"] = stats["forwardPE"]*stats["forwardEps"]/stats["trailingEps"]


# # Storing in SQL

# In[6]:


# merging statistical information with stock information
stats = stats.merge(full_stk, on = "ticker", how = "left")

# filtering column
stats = stats.loc[:,["stock_id", "enterpriseToRevenue",
                     "enterpriseToEbitda", "enterpriseValue",
                     "profitMargins", "netIncomeToCommon",
                     "bookValue", "sharesOutstanding",
                     "sharesPercentSharesOut", "heldPercentInstitutions",
                     "heldPercentInsiders", "sharesShort",
                     "shortRatio", "floatShares", "forwardEps",
                     "trailingEps", "forwardPE", "trailingPE",
                     "pegRatio","priceToBook", "pricetoSales", "beta"]]

# dropping all null values
stats = stats.dropna()


# In[7]:


for index, value in stats.iterrows():
    my_cursor.execute("INSERT INTO metrics (stock_id, date, enterpriseToRevenue,                                            enterpriseToEbitda, enterpriseValue,                                            profitMargins, netIncomeToCommon,                                            bookValue, sharesOutstanding,                                            sharesPercentSharesOut, heldPercentInstitutions,                                            heldPercentInsiders, sharesShort,                                            shortRatio, floatShares, forwardEps,                                            trailingEps, forwardPE, trailingPE,                                            pegRatio,priceToBook, pricetoSales, beta)                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                      (int(stats.loc[index, "stock_id"]),
                       today,
                       float(stats.loc[index, 'enterpriseToRevenue']),
                       float(stats.loc[index, 'enterpriseToEbitda']),
                       int(stats.loc[index, 'enterpriseValue']),
                       float(stats.loc[index, 'profitMargins']),
                       int(stats.loc[index, 'netIncomeToCommon']),
                       float(stats.loc[index, 'bookValue']),
                       int(stats.loc[index, 'sharesOutstanding']),
                       float(stats.loc[index, 'sharesPercentSharesOut']),
                       float(stats.loc[index, 'heldPercentInstitutions']),
                       float(stats.loc[index, 'heldPercentInsiders']),
                       int(stats.loc[index, 'sharesShort']),
                       float(stats.loc[index, 'shortRatio']),
                       int(stats.loc[index, 'floatShares']),
                       float(stats.loc[index, 'forwardEps']),
                       float(stats.loc[index, 'trailingEps']),
                       float(stats.loc[index, 'forwardPE']),
                       float(stats.loc[index, 'trailingPE']),
                       float(stats.loc[index, 'pegRatio']),
                       float(stats.loc[index, 'priceToBook']),
                       float(stats.loc[index, 'pricetoSales']),
                       float(stats.loc[index, 'beta']),
                      )
                     )
    
db.commit()

