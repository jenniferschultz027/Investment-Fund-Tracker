import pandas as pd
import numpy as np
import urllib.request, urllib.parse, urllib.error
import urllib3
import requests
import certifi

# cd PIRWork
# python3 write_csv.py

http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

#urlVUG = "https://query1.finance.yahoo.com/v7/finance/download/VUG?period1=1104537600&period2=1596931200&interval=1d&events=history"
#urlVTV = "https://query1.finance.yahoo.com/v7/finance/download/VTV?period1=1104537600&period2=1596931200&interval=1d&events=history"
#urlIWP = "https://query1.finance.yahoo.com/v7/finance/download/IWP?period1=1104537600&period2=1596931200&interval=1d&events=history"
#urlIWS = "https://query1.finance.yahoo.com/v7/finance/download/IWS?period1=1104537600&period2=1596931200&interval=1d&events=history"
#urlVBK = "https://query1.finance.yahoo.com/v7/finance/download/VBK?period1=1104537600&period2=1596931200&interval=1d&events=history"
#urlVBR = "https://query1.finance.yahoo.com/v7/finance/download/VBR?period1=1104537600&period2=1596931200&interval=1d&events=history"

### Requested VUG & VTV urls from Yahoo Finance and created .csv files
def downloadCSV(idx, csvName):
    url = ''.join(['https://query1.finance.yahoo.com/v7/finance/download/', idx, '?period1=1104537600&period2=1596931200&interval=1d&events=history'])
    http.request('GET', url)
    csv = urllib.request.urlopen(url).read().decode()
    with open(csvName, 'w') as fi:
        fi.write(csv)

size = input("Enter 'Large', 'Mid', or 'Small' Company Size:")
if (size == 'Large'):
    firstIndex = "VUG"
    secondIndex = "VTV"
if (size == "Mid"):
    firstIndex = "IWP"
    secondIndex = "IWS"
if (size == "Small"):
    firstIndex = "VBK"
    secondIndex = "VBR"

index1 = firstIndex
csv1 = 'def1.csv'
downloadCSV(index1, csv1)
index2 = secondIndex
csv2 = 'def2.csv'
downloadCSV(index2, csv2)

### Read VUG Data
df1 = pd.read_csv('def1.csv')
vug = df1[['Date', 'Adj Close']].copy()
vug_new = vug.rename(columns={'Adj Close':'VUG Adj Close'})
vug_new['VUG Adj Close'] = vug_new['VUG Adj Close'].astype(float)

### Read VTV Data
df2 = pd.read_csv('def2.csv')
vtv = df2[['Adj Close']].copy()
vtv_new = vtv.rename(columns={'Adj Close':'VTV Adj Close'})
vtv_new['VTV Adj Close'] = vtv_new['VTV Adj Close'].astype(float)

#Combined dataframes (contains date column & adj close column for VUG & VTV)
frames = [vug_new, vtv_new]
new_df = pd.concat(frames, axis=1)
length = len(new_df)

#Input values
numOfDaysER = int(input("Enter number of days for Efficiency Ratio:"))
numOfDaysFema = int(input("Enter number of days for FEMA:"))
numOfDaysSema = int(input("Enter number of days for SEMA:"))
emaND = int(input("Enter Number Of Days For Exponential Moving Average:"))
emaCT = float(input("Enter Clearance Threshold Pct For Exponential Moving Average:"))

#Set new columns in dataframe to null
NaN = np.nan

#Created VUG/VTV column
def ratioAdjClose():
    new_df['VUG/VTV'] = new_df['VUG Adj Close'] / new_df['VTV Adj Close']

ratioAdjClose()

#Created ABS ER Delta column
new_df['ABS ER Delta'] = NaN
def absErDelta():
    x=0
    y=numOfDaysER
    l=numOfDaysER
    while (y<length):
        initValue = new_df.at[x,'VUG/VTV']
        finalValue = new_df.at[y,'VUG/VTV']
        difference = abs(finalValue-initValue)
        new_df.at[l,'ABS ER Delta'] = difference
        x=x+1
        y=y+1
        l=l+1

absErDelta()

#Created ABS Daily Delta column
new_df['ABS Daily Delta'] = NaN
def absDailyDelta():
    x=0
    y=1
    l=1
    while (y<length):
        initValue = new_df.at[x,'VUG/VTV']
        finalValue = new_df.at[y,'VUG/VTV']
        difference = abs(finalValue-initValue)
        new_df.at[l,'ABS Daily Delta'] = difference
        x=x+1
        y=y+1
        l=l+1

absDailyDelta()

#Created Efficiency Ratio column
new_df['Efficiency Ratio'] = NaN
def efficiencyRatio(numOfDays):
    x=1
    #y=5
    y=numOfDays+1
    l=numOfDaysER
    while (y<(length+1)):
        dailyDelta = new_df.iloc[x:y,5]
        sumOfRows = dailyDelta.sum()
        erDelta = new_df.at[l,'ABS ER Delta']
        eRatio = erDelta/sumOfRows
        new_df.at[l,'Efficiency Ratio'] = eRatio
        x=x+1
        y=y+1
        l=l+1

efficiencyRatio(numOfDaysER)

#Created FEMA SM Constant column
new_df['FEMA SM Constant'] = NaN
def femaSmConstant(numOfDays):
    l=numOfDaysER
    #f=10
    f = numOfDays
    while (l<length):
        constant = 2/(f+1)
        new_df.at[l, 'FEMA SM Constant'] = constant
        l=l+1

femaSmConstant(numOfDaysFema)

#Created SEMA SM Constant column
new_df['SEMA SM Constant'] = NaN
def semaSmConstant(numOfDays):
    l=numOfDaysER
    #s=2
    s = numOfDays
    while (l<length):
        constant = 2/(s+1)
        new_df.at[l, 'SEMA SM Constant'] = constant
        l=l+1

semaSmConstant(numOfDaysSema)

#Created Smoothing Constant column
new_df['Smoothing Constant'] = NaN
def smoothingConstant():
    l=numOfDaysER
    while (l<length):
        eRatio = new_df.at[l,'Efficiency Ratio']
        femaConstant = new_df.at[l,'FEMA SM Constant']
        semaConstant = new_df.at[l,'SEMA SM Constant']
        base = eRatio*(semaConstant-femaConstant) + femaConstant
        exponent = 2
        smoothConstant = pow(base, exponent)
        new_df.at[l, 'Smoothing Constant'] = smoothConstant
        l=l+1

smoothingConstant()

#Created KAMA column
new_df['KAMA'] = NaN
def kama():
    l=numOfDaysER
    value = new_df.at[l-1,'VUG/VTV']
    new_df.at[l-1, 'KAMA'] = value
    while (l<length):
        initValue = new_df.at[l-1, 'KAMA']
        smoothValue = new_df.at[l, 'Smoothing Constant']
        ratioValue = new_df.at[l, 'VUG/VTV']
        kama = initValue + smoothValue*(ratioValue-initValue)
        new_df.at[l, 'KAMA'] = kama
        l=l+1

kama()

#Created SMA(ND) column
new_df['SMA(ND)'] = NaN
def smaND(emaND):
    #emaND = 14
    #emaCT = 0.33
    x=0
    y=emaND
    l=emaND-1
    while (y<(length+1)):
        ratio = new_df.iloc[x:y,3]
        sumOfRows = ratio.sum()
        smaND = sumOfRows/emaND
        new_df.at[l, 'SMA(ND)'] = smaND
        x=x+1
        y=y+1
        l=l+1

smaND(emaND)

#Created EMA(ND) column
new_df['EMA(ND)'] = NaN
def emaNDvalue(emaND):
    l=emaND
    while(l<(emaND+1)):
        finalSmaND = new_df.at[l, "SMA(ND)"]
        initSmaND = new_df.at[l-1, "SMA(ND)"]
        emaNDvalue = finalSmaND*(2/(emaND+1)) + initSmaND*(1-(2/(emaND+1)))
        new_df.at[l, "EMA(ND)"] = emaNDvalue
        l=l+1
    m=emaND+1
    while (m<length):
        smaND = new_df.at[m, 'SMA(ND)']
        initValue = new_df.at[m-1, 'EMA(ND)']
        emaNDvalue = smaND*(2/(emaND+1)) + initValue*(1-(2/(emaND+1)))
        new_df.at[m, 'EMA(ND)'] = emaNDvalue
        m=m+1

emaNDvalue(emaND)

#Created EMA(ND)-CT column
new_df['EMA(ND)-CT'] = NaN
def emaMinusCT():
    l=emaND
    while (l<length):
        emaNDvalue = new_df.at[l, "EMA(ND)"]
        minusCT = emaNDvalue*(1-0.01*emaCT)
        new_df.at[l, "EMA(ND)-CT"] = minusCT
        l=l+1

emaMinusCT()

#Created EMA(ND)+CT column
new_df['EMA(ND)+CT'] = NaN
def emaPlusCT():
    l=emaND
    while (l<length):
        emaNDvalue = new_df.at[l, "EMA(ND)"]
        plusCT = emaNDvalue*(1+0.01*emaCT)
        new_df.at[l, "EMA(ND)+CT"] = plusCT
        l=l+1

emaPlusCT()

#Created No Switch = 0 column
new_df['No Switch = 0'] = NaN
def noSwitch():
    l=emaND
    while (l<length):
        kama = new_df.at[l, 'KAMA']
        minusCT = new_df.at[l, 'EMA(ND)-CT']
        plusCT = new_df.at[l, 'EMA(ND)+CT']
        if (kama > minusCT and kama < plusCT):
            new_df.at[l, 'No Switch = 0'] = 0
        else:
            new_df.at[l, 'No Switch = 0'] = 1
        l=l+1

noSwitch()

#Created Above column
new_df['Above'] = NaN
def above():
    l=emaND
    while (l<length):
        kama = new_df.at[l, 'KAMA']
        plusCT = new_df.at[l, 'EMA(ND)+CT']
        switchValue = new_df.at[l, 'No Switch = 0']
        if (kama > plusCT and switchValue == 1):
            new_df.at[l, 'Above'] = 1
        else:
            new_df.at[l, 'Above'] = 0
        l=l+1

above()

#Created Below column
new_df['Below'] = NaN
def below():
    l=emaND
    while (l<length):
        kama = new_df.at[l, 'KAMA']
        minusCT = new_df.at[l, 'EMA(ND)-CT']
        switchValue = new_df.at[l, 'No Switch = 0']
        if (kama < minusCT and switchValue == 1):
            new_df.at[l, 'Below'] = 1
        else:
            new_df.at[l, 'Below'] = 0
        l=l+1

below()

#Created Own VTV column
new_df['Own VTV'] = NaN
def ownVTV():
    l=emaND
    new_df.at[l-1, 'Own VTV'] = 1
    while (l<length):
        initValue = new_df.at[l-1, 'Own VTV']
        switchValue = new_df.at[l, 'No Switch = 0']
        belowValue = new_df.at[l, 'Below']
        aboveValue = new_df.at[l, 'Above']
        if ((switchValue==1 and belowValue==1) or (initValue==1 and aboveValue==0)):
            new_df.at[l, 'Own VTV'] = 1
        else:
            new_df.at[l, 'Own VTV'] = 0
        l=l+1

ownVTV()

#Created Own VUG column
new_df['Own VUG'] = NaN
def ownVUG():
    l=emaND
    new_df.at[l-1, 'Own VUG'] = 1
    while (l<length):
        initValue = new_df.at[l-1, 'Own VUG']
        switchValue = new_df.at[l, 'No Switch = 0']
        aboveValue = new_df.at[l, 'Above']
        ownVTV = new_df.at[l, 'Own VTV']
        if ((switchValue==1 and aboveValue==1) or (initValue==1 and ownVTV==0)):
            new_df.at[l, 'Own VUG'] = 1
        else:
            new_df.at[l, 'Own VUG'] = 0
        l=l+1

ownVUG()

#Created Use VTV Price column
new_df['Use VTV Price'] = NaN
def usePriceVTV():
    l=emaND
    while (l<length):
        ownVTV = new_df.at[l, 'Own VTV']
        finalAdjClose = new_df.at[l, 'VTV Adj Close']
        initAdjClose = new_df.at[l-1, 'VTV Adj Close']
        quotient = finalAdjClose/initAdjClose
        if (ownVTV==1):
            new_df.at[l, 'Use VTV Price'] = quotient
        else:
            new_df.at[l, 'Use VTV Price'] = 0
        l=l+1

usePriceVTV()

#Created Use VUG Price column
new_df['Use VUG Price'] = NaN
def usePriceVUG():
    l=emaND
    while (l<length):
        ownVUG = new_df.at[l, 'Own VUG']
        finalAdjClose = new_df.at[l, 'VUG Adj Close']
        initAdjClose = new_df.at[l-1, 'VUG Adj Close']
        quotient = finalAdjClose/initAdjClose
        if (ownVUG==1):
            new_df.at[l, 'Use VUG Price'] = quotient
        else:
            new_df.at[l, 'Use VUG Price'] = 0
        l=l+1

usePriceVUG()

#Created Daily Return Multiple column
new_df['Daily Return Multiple'] = NaN
def dailyReturnMultiple():
    l=emaND
    while (l<length):
        useVUG = new_df.at[l, 'Use VUG Price']
        useVTV = new_df.at[l, 'Use VTV Price']
        sum = useVUG + useVTV
        new_df.at[l, 'Daily Return Multiple'] = sum
        l=l+1

dailyReturnMultiple()

#Created Cumulative Return column
new_df['Cumulative Return'] = NaN
def cumulativeReturn():
    l=emaND
    new_df.at[l-1, 'Cumulative Return'] = 100
    while (l<length):
        dailyReturn = new_df.at[l, 'Daily Return Multiple']
        initValue = new_df.at[l-1, 'Cumulative Return']
        multiply = dailyReturn  * initValue
        new_df.at[l, 'Cumulative Return'] = multiply
        l=l+1

cumulativeReturn()

# Print dataframe
print(" ")
print(new_df.iloc[:,20:25])
print(" ")

# Loop through each row to check year
i = 0
year = 2005
for key,value in new_df.iteritems():
    while (i<length and year<2020):
        if (key != 'Date'):
            break
        cell = new_df.at[i, 'Date']
        new_value = str(cell)
        date = int(new_value[0:4])
        if (date == year):
            i=i+1
            continue
        else:
            eachLength = i-1
            # Print return value for each year
            def returnValue():
                returnValue = str(new_df.at[eachLength, 'Cumulative Return'] - 100)
                print(year, 'Return Value:', ''.join([returnValue, '%']))

            returnValue()
            year=year+1
            i=i-1
        i=i+1

# Print final return value
eachLength = length-1
returnValue()

print(" ")

def switchValue():
    l=emaND
    while (l<length):
        initial = new_df.at[l-1, 'Own VTV']
        final = new_df.at[l, 'Own VTV']
        if (initial==final):
            l=l+1
            continue
        else:
            date = new_df.at[l, 'Date']
            if (final==1):
                print("Switch from VUG to VTV on", date)
            else:
                print("Switch from VTV to VUG on", date)
            l=l+1

switchValue()

print(" ")
print('Finished')
print(" ")
