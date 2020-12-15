import pandas as pd
import numpy as np
import urllib.request, urllib.parse, urllib.error
import urllib3
import requests
import certifi

http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

#urlVUG = "https://query1.finance.yahoo.com/v7/finance/download/VUG?period1=1104537600&period2=1596931200&interval=1d&events=history"
#urlVTV = "https://query1.finance.yahoo.com/v7/finance/download/VTV?period1=1104537600&period2=1596931200&interval=1d&events=history"

### Requested VUG & VTV urls from Yahoo Finance and created .csv files
def downloadCSV(idx, csvName):
    url = ''.join(['https://query1.finance.yahoo.com/v7/finance/download/', idx, '?period1=1104537600&period2=1596931200&interval=1d&events=history'])
    http.request('GET', url)
    csv = urllib.request.urlopen(url).read().decode()
    with open(csvName, 'w') as fi:
        fi.write(csv)

indexVUG = 'VUG'
csvVUG = 'defVUG.csv'
downloadCSV(indexVUG, csvVUG)
indexVTV = 'VTV'
csvVTV = 'defVTV.csv'
downloadCSV(indexVTV, csvVTV)

### Read VUG Data
df1 = pd.read_csv('defVUG.csv')
vug = df1[['Date', 'Adj Close']].copy()
vug_new = vug.rename(columns={'Adj Close':'VUG Adj Close'})
vug_new['VUG Adj Close'] = vug_new['VUG Adj Close'].astype(float)

### Read VTV Data
df2 = pd.read_csv('defVTV.csv')
vtv = df2[['Adj Close']].copy()
vtv_new = vtv.rename(columns={'Adj Close':'VTV Adj Close'})
vtv_new['VTV Adj Close'] = vtv_new['VTV Adj Close'].astype(float)

#Combined dataframes (contains date column & adj close column for VUG & VTV)
frames = [vug_new, vtv_new]
new_df = pd.concat(frames, axis=1)
length = len(new_df)

#Set new columns in dataframe to null
NaN = np.nan

#Created a new dataframe and initialized columns for loop
loop_df = pd.DataFrame()
loop_df['Date'] = NaN
loop_df['VUG Adj Close'] = NaN
loop_df['VTV Adj Close'] = NaN

#Input values
numOfDaysER = int(input("Enter number of days for Efficiency Ratio:"))
numOfDaysFema = int(input("Enter number of days for FEMA:"))
numOfDaysSema = int(input("Enter number of days for SEMA:"))
emaNDValue = int(input("Enter Number Of Days For Exponential Moving Average:"))
emaCTValue = float(input("Enter Clearance Threshold Pct For Exponential Moving Average:"))

#Created a for loop for each year
print(" ")
i = 0
year = 2005
for key,value in new_df.iteritems():
    while (i<length and year<2008):
        if (key != 'Date'):
            break
        cell = new_df.at[i, 'Date']
        new_value = str(cell)
        date = int(new_value[0:4])
        if (date == year):
            vugDate = new_df.at[i,'Date']
            vugValue = new_df.at[i,'VUG Adj Close']
            vtvValue = new_df.at[i,'VTV Adj Close']
            loop_df.at[i, 'Date'] = vugDate
            loop_df.at[i, 'VUG Adj Close'] = vugValue
            loop_df.at[i, 'VTV Adj Close'] = vtvValue
        else:
            new_length = loop_df['Date'].index[[-1]]

            #Created VUG/VTV column
            def ratioAdjClose():
                loop_df['VUG/VTV'] = loop_df['VUG Adj Close'] / loop_df['VTV Adj Close']

            ratioAdjClose()

            #Created ABS ER Delta column
            loop_df['ABS ER Delta'] = NaN
            def absErDelta():
                for row in loop_df.index:
                    x=row
                    y=row+numOfDaysER
                    l=row+numOfDaysER
                    while (y<new_length+1):
                        initValue = loop_df.at[x,'VUG/VTV']
                        finalValue = loop_df.at[y,'VUG/VTV']
                        difference = abs(finalValue-initValue)
                        loop_df.at[l,'ABS ER Delta'] = difference
                        x=x+1
                        y=y+1
                        l=l+1
                    break

            absErDelta()

            #Created ABS Daily Delta column
            loop_df['ABS Daily Delta'] = NaN
            def absDailyDelta():
                for row in loop_df.index:
                    x=row
                    y=row+1
                    l=row+1
                    while (y<new_length+1):
                        initValue = loop_df.at[x,'VUG/VTV']
                        finalValue = loop_df.at[y,'VUG/VTV']
                        difference = abs(finalValue-initValue)
                        loop_df.at[l,'ABS Daily Delta'] = difference
                        x=x+1
                        y=y+1
                        l=l+1
                    break

            absDailyDelta()

            #Created Efficiency Ratio column
            loop_df['Efficiency Ratio'] = NaN
            def efficiencyRatio(numOfDays):
                for row in loop_df.index:
                    if (row == new_length-3):
                        break
                    x=row+1
                    l=row+numOfDays
                    n = row+1+numOfDays   #n=row+5
                    sumOfRows = 0
                    while (l<new_length+1):
                        dailyDelta = loop_df.at[x,'ABS Daily Delta']
                        sumOfRows = sumOfRows + dailyDelta
                        x=x+1
                        if (x != n):
                            continue
                        else:
                            erDelta = loop_df.at[l,'ABS ER Delta']
                            eRatio = erDelta/sumOfRows
                            loop_df.at[l,'Efficiency Ratio'] = eRatio
                            break

            efficiencyRatio(numOfDaysER)

            #Created FEMA SM Constant column
            loop_df['FEMA SM Constant'] = NaN
            def femaSmConstant(numOfDays):
                for row in loop_df.index:
                    l=row+numOfDaysER
                    f = numOfDays   #f=10
                    while (l<new_length+1):
                        constant = 2/(f+1)
                        loop_df.at[l, 'FEMA SM Constant'] = constant
                        l=l+1
                    break

            femaSmConstant(numOfDaysFema)

            #Created SEMA SM Constant column
            loop_df['SEMA SM Constant'] = NaN
            def semaSmConstant(numOfDays):
                for row in loop_df.index:
                    l=row+numOfDaysER
                    s = numOfDays   #s=2
                    while (l<new_length+1):
                        constant = 2/(s+1)
                        loop_df.at[l, 'SEMA SM Constant'] = constant
                        l=l+1
                    break

            semaSmConstant(numOfDaysSema)

            #Created Smoothing Constant column
            loop_df['Smoothing Constant'] = NaN
            def smoothingConstant():
                for row in loop_df.index:
                    l=row+numOfDaysER
                    while (l<new_length+1):
                        eRatio = loop_df.at[l,'Efficiency Ratio']
                        femaConstant = loop_df.at[l,'FEMA SM Constant']
                        semaConstant = loop_df.at[l,'SEMA SM Constant']
                        base = eRatio*(semaConstant-femaConstant) + femaConstant
                        exponent = 2
                        smoothConstant = pow(base, exponent)
                        loop_df.at[l, 'Smoothing Constant'] = smoothConstant
                        l=l+1
                    break

            smoothingConstant()

            #Created KAMA column
            loop_df['KAMA'] = NaN
            def kama():
                for row in loop_df.index:
                    l=row+numOfDaysER
                    value = loop_df.at[l-1,'VUG/VTV']
                    loop_df.at[l-1, 'KAMA'] = value
                    while (l<new_length+1):
                        initValue = loop_df.at[l-1, 'KAMA']
                        smoothValue = loop_df.at[l, 'Smoothing Constant']
                        ratioValue = loop_df.at[l, 'VUG/VTV']
                        kama = initValue + smoothValue*(ratioValue-initValue)
                        loop_df.at[l, 'KAMA'] = kama
                        l=l+1
                    break

            kama()

            emaND = emaNDValue   #emaND = 14
            emaCT = emaCTValue   #emaCT = .33

            #Created SMA(ND) column
            loop_df['SMA(ND)'] = NaN
            def smaND(emaND):
                for row in loop_df.index:
                    if (row == new_length-12):
                        break
                    x=row
                    l=row+emaND-1
                    n=row+emaND
                    #n=row+14
                    sumOfRows = 0
                    while (l<new_length+1):
                        ratio = loop_df.at[x,'VUG/VTV']
                        sumOfRows = sumOfRows + ratio
                        x=x+1
                        if(x != n):
                            continue
                        else:
                            smaND = sumOfRows/emaND
                            loop_df.at[l, 'SMA(ND)'] = smaND
                            break

            smaND(emaNDValue)

            #Created EMA(ND) column
            loop_df['EMA(ND)'] = NaN
            def emaNDvalue(emaND):
                for row in loop_df.index:
                    l=row+emaND
                    while(l<(row+emaND+1)):
                        finalSmaND = loop_df.at[l, "SMA(ND)"]
                        initSmaND = loop_df.at[l-1, "SMA(ND)"]
                        emaNDvalue = finalSmaND*(2/(emaND+1)) + initSmaND*(1-(2/(emaND+1)))
                        loop_df.at[l, "EMA(ND)"] = emaNDvalue
                        l=l+1
                    m=row+emaND+1
                    while (m<new_length+1):
                        smaND = loop_df.at[m, 'SMA(ND)']
                        initValue = loop_df.at[m-1, 'EMA(ND)']
                        emaNDvalue = smaND*(2/(emaND+1)) + initValue*(1-(2/(emaND+1)))
                        loop_df.at[m, 'EMA(ND)'] = emaNDvalue
                        m=m+1
                    break

            emaNDvalue(emaNDValue)

            #Created EMA(ND)-CT column
            loop_df['EMA(ND)-CT'] = NaN
            def emaMinusCT(emaCT):
                for row in loop_df.index:
                    l=row+emaND
                    while (l<new_length+1):
                        emaNDvalue = loop_df.at[l, "EMA(ND)"]
                        minusCT = emaNDvalue*(1-0.01*emaCT)
                        loop_df.at[l, "EMA(ND)-CT"] = minusCT
                        l=l+1
                    break

            emaMinusCT(emaCTValue)

            #Created EMA(ND)+CT column
            loop_df['EMA(ND)+CT'] = NaN
            def emaPlusCT(emaCT):
                for row in loop_df.index:
                    l=row+emaND
                    while (l<new_length+1):
                        emaNDvalue = loop_df.at[l, "EMA(ND)"]
                        plusCT = emaNDvalue*(1+0.01*emaCT)
                        loop_df.at[l, "EMA(ND)+CT"] = plusCT
                        l=l+1
                    break

            emaPlusCT(emaCTValue)

            #Created No Switch = 0 column
            new_df['No Switch = 0'] = NaN
            def noSwitch():
                for row in loop_df.index:
                    l=row+emaND
                    while (l<new_length+1):
                        kama = loop_df.at[l, 'KAMA']
                        minusCT = loop_df.at[l, 'EMA(ND)-CT']
                        plusCT = loop_df.at[l, 'EMA(ND)+CT']
                        if (kama > minusCT and kama < plusCT):
                            loop_df.at[l, 'No Switch = 0'] = 0
                        else:
                            loop_df.at[l, 'No Switch = 0'] = 1
                        l=l+1
                    break

            noSwitch()

            #Created Above column
            loop_df['Above'] = NaN
            def above():
                for row in loop_df.index:
                    l=row+emaND
                    while (l<new_length+1):
                        kama = loop_df.at[l, 'KAMA']
                        plusCT = loop_df.at[l, 'EMA(ND)+CT']
                        switchValue = loop_df.at[l, 'No Switch = 0']
                        if (kama > plusCT and switchValue == 1):
                            loop_df.at[l, 'Above'] = 1
                        else:
                            loop_df.at[l, 'Above'] = 0
                        l=l+1
                    break

            above()

            #Created Below column
            loop_df['Below'] = NaN
            def below():
                for row in loop_df.index:
                    l=row+emaND
                    while (l<new_length+1):
                        kama = loop_df.at[l, 'KAMA']
                        minusCT = loop_df.at[l, 'EMA(ND)-CT']
                        switchValue = loop_df.at[l, 'No Switch = 0']
                        if (kama < minusCT and switchValue == 1):
                            loop_df.at[l, 'Below'] = 1
                        else:
                            loop_df.at[l, 'Below'] = 0
                        l=l+1
                    break

            below()

            #Created Own VTV column
            loop_df['Own VTV'] = NaN
            def ownVTV():
                for row in loop_df.index:
                    l=row+emaND
                    loop_df.at[l-1, 'Own VTV'] = 1
                    while (l<new_length+1):
                        initValue = loop_df.at[l-1, 'Own VTV']
                        switchValue = loop_df.at[l, 'No Switch = 0']
                        belowValue = loop_df.at[l, 'Below']
                        aboveValue = loop_df.at[l, 'Above']
                        if ((switchValue==1 and belowValue==1) or (initValue==1 and aboveValue==0)):
                            loop_df.at[l, 'Own VTV'] = 1
                        else:
                            loop_df.at[l, 'Own VTV'] = 0
                        l=l+1
                    break

            ownVTV()

            #Created Own VUG column
            loop_df['Own VUG'] = NaN
            def ownVUG():
                for row in loop_df.index:
                    l=row+emaND
                    loop_df.at[l-1, 'Own VUG'] = 1
                    while (l<new_length+1):
                        initValue = loop_df.at[l-1, 'Own VUG']
                        switchValue = loop_df.at[l, 'No Switch = 0']
                        aboveValue = loop_df.at[l, 'Above']
                        ownVTV = loop_df.at[l, 'Own VTV']
                        if ((switchValue==1 and aboveValue==1) or (initValue==1 and ownVTV==0)):
                            loop_df.at[l, 'Own VUG'] = 1
                        else:
                            loop_df.at[l, 'Own VUG'] = 0
                        l=l+1
                    break

            ownVUG()

            #Created Use VTV Price column
            loop_df['Use VTV Price'] = NaN
            def usePriceVTV():
                for row in loop_df.index:
                    l=row+emaND
                    while (l<new_length+1):
                        ownVTV = loop_df.at[l, 'Own VTV']
                        finalAdjClose = loop_df.at[l, 'VTV Adj Close']
                        initAdjClose = loop_df.at[l-1, 'VTV Adj Close']
                        quotient = finalAdjClose/initAdjClose
                        if (ownVTV==1):
                            loop_df.at[l, 'Use VTV Price'] = quotient
                        else:
                            loop_df.at[l, 'Use VTV Price'] = 0
                        l=l+1
                    break

            usePriceVTV()

            #Created Use VUG Price column
            loop_df['Use VUG Price'] = NaN
            def usePriceVUG():
                for row in loop_df.index:
                    l=row+emaND
                    while (l<new_length+1):
                        ownVUG = loop_df.at[l, 'Own VUG']
                        finalAdjClose = loop_df.at[l, 'VUG Adj Close']
                        initAdjClose = loop_df.at[l-1, 'VUG Adj Close']
                        quotient = finalAdjClose/initAdjClose
                        if (ownVUG==1):
                            loop_df.at[l, 'Use VUG Price'] = quotient
                        else:
                            loop_df.at[l, 'Use VUG Price'] = 0
                        l=l+1
                    break

            usePriceVUG()

            #Created Daily Return Multiple column
            loop_df['Daily Return Multiple'] = NaN
            def dailyReturnMultiple():
                for row in loop_df.index:
                    l=row+emaND
                    while (l<new_length+1):
                        useVUG = loop_df.at[l, 'Use VUG Price']
                        useVTV = loop_df.at[l, 'Use VTV Price']
                        sum = useVUG + useVTV
                        loop_df.at[l, 'Daily Return Multiple'] = sum
                        l=l+1
                    break

            dailyReturnMultiple()

            #Created Cumulative Return column
            loop_df['Cumulative Return'] = NaN
            def cumulativeReturn():
                for row in loop_df.index:
                    l=row+emaND
                    value = 100
                    loop_df.at[l-1, 'Cumulative Return'] = value
                    while (l<new_length+1):
                        dailyReturn = loop_df.at[l, 'Daily Return Multiple']
                        initValue = loop_df.at[l-1, 'Cumulative Return']
                        multiply = dailyReturn  * initValue
                        loop_df.at[l, 'Cumulative Return'] = multiply
                        l=l+1
                    break

            cumulativeReturn()

            #Print dataframe
            #print(" ")
            #print(loop_df.iloc[:,0:5])
            #print(" ")

            # Print return value for each year
            def returnValue():
                for row in loop_df.index:
                    l = row
                    if (l == new_length):
                        returnValue = str(loop_df.at[l, 'Cumulative Return'] - 100)
                        print(year, 'Return Value:', ''.join([returnValue, '%']))
                    else:
                        continue

            returnValue()

            loop_df = loop_df[0:0]
            year=year+1
            i=i-1
        i=i+1

print(" ")
print('Finished')
print(" ")
