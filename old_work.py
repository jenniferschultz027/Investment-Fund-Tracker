import requests
import lxml.html as lh
import pandas as pd
import numpy as np

### VUG Data
url='https://finance.yahoo.com/quote/VUG/history?p=VUG'
page=requests.get(url)
doc=lh.fromstring(page.content)

tr_elements = doc.xpath('//tr')
for T in tr_elements:
    if len(T) < 7:
        tr_elements.remove(T)

tr_elements = doc.xpath('//tr')
col=[]
i=0

for t in tr_elements[0]:
    i+=1
    name=t.text_content()
    col.append((name,[]))

for j in range(1,len(tr_elements)):
    T=tr_elements[j]
    if len(T) != 7:
        continue
    i=0
    for t in T.iterchildren():
        data=t.text_content()
        if i>0:
            try:
                data=int(data)
            except:
                pass
        col[i][1].append(data)
        i+=1

Dict1 = {title:val for (title,val) in col}
df1=pd.DataFrame(Dict1)
vug = df1[['Date', 'Adj Close**']].copy()
vug_new = vug.rename(columns={'Date':'VUG Date', 'Adj Close**':'VUG Adj Close'})
vug_new['VUG Adj Close'] = vug_new['VUG Adj Close'].astype(float)

### VTV Data
url='https://finance.yahoo.com/quote/vtv/history?p=vtv'
page=requests.get(url)
doc=lh.fromstring(page.content)

tr_elements = doc.xpath('//tr')
for T in tr_elements:
    if len(T) < 7:
        tr_elements.remove(T)

tr_elements = doc.xpath('//tr')
col=[]
i=0

for t in tr_elements[0]:
    i+=1
    name=t.text_content()
    col.append((name,[]))

for j in range(1,len(tr_elements)):
    T=tr_elements[j]
    if len(T) != 7:
        continue
    i=0
    for t in T.iterchildren():
        data=t.text_content()
        if i>0:
            try:
                data=int(data)
            except:
                pass
        col[i][1].append(data)
        i+=1

Dict2 = {title:val for (title,val) in col}
df2=pd.DataFrame(Dict2)
vtv = df2[['Date', 'Adj Close**']].copy()
vtv_new = vtv.rename(columns={'Date':'VTV Date', 'Adj Close**':'VTV Adj Close'})
vtv_new['VTV Adj Close'] = vtv_new['VTV Adj Close'].astype(float)

#Combined Dataframes (contains date column & adj close column for VUG & VTV)
frames = [vug_new, vtv_new]
new_df = pd.concat(frames, axis=1)
length = len(new_df)

#Created VUG/VTV
def ratioAdjClose():
    new_df['VUG/VTV'] = new_df['VUG Adj Close'] / new_df['VTV Adj Close']

ratioAdjClose()

#Set new columns in dataframe to null
NaN = np.nan

#Created Fast MA (10 days)
new_df['Fast Ma'] = NaN
def fastMA():
    x=0
    y=10
    l=9
    while (y<(length+1)):
        ratio = new_df.iloc[x:y,4]
        sumOfRows = ratio.sum()
        fastMA = sumOfRows/10
        new_df.at[l,'Fast Ma'] = fastMA
        x=x+1
        y=y+1
        l=l+1

fastMA()

#Created Slow MA (40 days)
new_df['Slow Ma'] = NaN
def slowMA():
    x=0
    y=40
    l=39
    while (y<(length+1)):
        ratio = new_df.iloc[x:y,4]
        sumOfRows = ratio.sum()
        slowMA = sumOfRows/40
        new_df.at[l,'Slow Ma'] = slowMA
        x=x+1
        y=y+1
        l=l+1

slowMA()

#Created Fast-Slow
new_df['Fast-Slow'] = NaN
def fastMinusSlow():
    new_df['Fast-Slow'] = new_df['Fast Ma'] - new_df['Slow Ma']

fastMinusSlow()

#Created Buy VUG Return
new_df['Buy VUG Return'] = NaN
def buyReturnVUG():
    x=38
    y=39
    l=39
    while (y<length):
        minusValue = new_df.at[l,'Fast-Slow']
        currVUG = new_df.at[y, 'VUG Adj Close']
        initVUG = new_df.at[x, 'VUG Adj Close']
        if minusValue > 0:
            buyVUG = (currVUG/initVUG) - 1
            new_df.at[l,'Buy VUG Return'] = buyVUG
        else:
            new_df.at[l,'Buy VUG Return'] = 0
        x=x+1
        y=y+1
        l=l+1

buyReturnVUG()

#Created Buy VTV Return
new_df['Buy VTV Return'] = NaN
def buyReturnVTV():
    x=38
    y=39
    l=39
    while (y<length):
        minusValue = new_df.at[l,'Fast-Slow']
        currVTV = new_df.at[y, 'VTV Adj Close']
        initVTV = new_df.at[x, 'VTV Adj Close']
        if minusValue <= 0:
            buyVTV = (currVTV/initVTV) - 1
            new_df.at[l,'Buy VTV Return'] = buyVTV
        else:
            new_df.at[l,'Buy VTV Return'] = 0
        x=x+1
        y=y+1
        l=l+1

buyReturnVTV()

#Created Combined Return
new_df['Combined Return'] = NaN
def combinedReturn():
    l=39
    while (l<length):
        returnVUG = new_df.at[l, 'Buy VUG Return']
        returnVTV = new_df.at[l, 'Buy VTV Return']
        combinedReturn = returnVUG + returnVTV
        new_df.at[l, 'Combined Return'] = combinedReturn
        l=l+1

combinedReturn()

#Created Running Total Return
new_df['Running Total Return'] = NaN
def totalReturn():
    new_df.at[38, 'Running Total Return'] = 100
    l=39
    while (l<length):
        initValue = new_df.at[l-1, 'Running Total Return']
        combinedReturn = new_df.at[l, 'Combined Return']
        runningReturn = initValue*(1+combinedReturn)
        new_df.at[l, 'Running Total Return'] = runningReturn
        l=l+1

totalReturn()

# Show dataframe
print(new_df)

# Print return value
def returnValue():
    returnValue = new_df.at[length-1, 'Running Total Return'] - 100
    print(returnValue,'%')

returnValue()
