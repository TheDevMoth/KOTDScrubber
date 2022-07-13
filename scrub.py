import praw
import os
import re
import threading
import time
import numpy as np
import pandas as pd
import requests
import configparser

def scrub_comment_body(body):
    # Is attack?
    hp = re.search(r"The boss has \*\*(-?\d+)\*\*", body)
    if(hp != None):
        # Determine Weapon and extra damage
        wepPat = re.search(r"You used the item '(.+)' and did (\d+)", body)
        if(wepPat == None):
            wep = "Barehand"
            extDmg = 0
        else:
            wep = wepPat.group(1)
            extDmg = int(wepPat.group(2))
        # Determine Damage XP and Gold
        stat = re.search(r"\| (\d+).* \| (\d+).*\| (\d+) \|", body)
        # Determine W/N/R
        statString = stat.group(0)
        affect = "N"
        if (re.search(r"\*\*\(WEAK!\)\*\*", statString) != None): affect = "W"
        elif (re.search(r"\*\*\(RESIST!\)\*\*", statString) != None): affect = "R"
        # Determine Crit
        crit = (re.search(r"\*\*\(CRIT!\)\*\*", statString) != None)
        # Determine Kill
        kill = (re.search(r"\*\*\(KILL!\)\*\*", statString) != None)
        # Determine Level
        lvl = int(re.search(r"level (\d+)", body).group(1))
        
        return [wep, int(stat.group(1)), int(stat.group(2)), int(stat.group(3)), extDmg, affect, crit, kill, lvl, int(hp.group(1))]
    else:
        return None

def get_author_comments(**kwargs):
    r = requests.get("https://api.pushshift.io/reddit/comment/search/",params=kwargs)
    data = r.json()
    return data['data']

try:
    df = pd.read_pickle(".pkl") # WRITE IN THE NAME OF THE FILE
except: 
    print("Making new Dataframe")
    df = pd.DataFrame(columns=['Time', 'Weapon', 'Damage', 'XP', 'Gold', 'Extra Damage', 'Weak/Noot/Resist', 'Crit', 'Kill', 'Attacker Level', 'Hp left', 'Attacker Race', 'Time to reply', 'Comment ID', 'Parent ID', 'Post ID'])

# Create Control thread 
quit = False
def control():
    global quit
    while not quit:
        i = input()
        if (i == "q"):
            quit = True
        elif(i == "s"):
            os.abort()

thread = threading.Thread(target=control)
thread.daemon = True
thread.start()

# Start reading
after = 1657003379 # ADD THIS
if(len(df) == 0):
    before = None
else:
    before = df['Time'].min()

rubs = []
tstart = time.perf_counter()
while not quit:
    comments = get_author_comments(author="KickOpenTheDoorBot",size=100,before=before,sort='desc',sort_type='created_utc')
    if not comments: break #check if we reached the end

    for comment in comments:
        before = comment['created_utc']
        if (before <= after):
            quit = True
            break
        try:
            textScrb = scrub_comment_body(comment["body"])
        except Exception as e:
            print(e)
            print(comment["body"])
            print(comment["id"])
            textScrb = None
        if(textScrb!=None):
            #Determine non-attack attributes
            t = comment["created_utc"]
            postid = comment["link_id"]
            comid = comment["id"]
            parid = comment["parent_id"]
            attRace = None
            tdif = None
            rubs.append([t]+textScrb+[attRace, tdif, comid, parid, postid])
    if(len(rubs) > 5000):
        print(F"Processed {len(rubs)} new Entries in {time.perf_counter() - tstart} seconds")
        df = pd.concat([df, pd.DataFrame(rubs, columns=['Time', 'Weapon', 'Damage', 'XP', 'Gold', 'Extra Damage', 'Weak/Noot/Resist', 'Crit', 'Kill', 'Attacker Level', 'Hp left', 'Attacker Race', 'Time to reply', 'Comment ID', 'Parent ID', 'Post ID'])], ignore_index=True)
        print(F"New size: {len(df)}")
        print(F"Saving as Scrubdata [{time.ctime()[4:13]}].pkl".replace(":","-"))
        df.to_pickle(F"Scrubdata [{time.ctime()[4:13]}].pkl".replace(":","-"))
        rubs = []
        tstart = time.perf_counter()
    time.sleep(0.5)
if(len(rubs) > 0):
    print(F"Processed {len(rubs)} new Entries in {time.perf_counter() - tstart} seconds")
    df = pd.concat([df, pd.DataFrame(rubs, columns=['Time', 'Weapon', 'Damage', 'XP', 'Gold', 'Extra Damage', 'Weak/Noot/Resist', 'Crit', 'Kill', 'Attacker Level', 'Hp left', 'Attacker Race', 'Time to reply', 'Comment ID', 'Parent ID', 'Post ID'])], ignore_index=True)
    print(F"New size: {len(df)}")
    print(F"Saving as Scrubdata [{time.ctime()[4:13]}].pkl".replace(":","-"))
    df.to_pickle(F"Scrubdata [{time.ctime()[4:13]}].pkl".replace(":","-"))


### Reading parent info ###
# prepare reddit token
loginfo = configparser.ConfigParser()
loginfo.read('praw.ini')
CLIENT_ID = loginfo['WhatIsBaitBot']['client_id']
CLIENT_SECRET = loginfo['WhatIsBaitBot']['client_secret']
USERNAME = loginfo['WhatIsBaitBot']['username']
PASSWORD = loginfo['WhatIsBaitBot']['password']

auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
data = {
	'grant_type' : 'password',
	'username' : USERNAME,
	'password' : PASSWORD
}
headers = {'User-Agent':'<Scrubbot1.4>'}
res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
TOKEN = res.json()['access_token']
headers['Authorization'] = F'bearer {TOKEN}'
confirm = requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)
if (confirm.text.find("403 Forbidden")!=-1):
	print("Auth failed")
else:
	print("Auth successful")

#scrubbing flairs and time to reply
def get_comments_from_reddit_api(ids,headers):
	params = {}
	params['id'] = ','.join([i if i.startswith('t1_') else f't1_{i}' for i in ids])
	r = requests.get("https://oauth.reddit.com/api/info",params=params,headers=headers)
	data = r.json()
	return data['data']['children']

print("Starting to process parent comments")
rdf = df[df['Attacker Race'].isnull()]
inds = rdf.index.to_list() #get a list of indecies where Attacker race is none

ts = time.perf_counter()
row = 0
endrow = len(inds)-1
while (row <= endrow):
    idlist = list(rdf["Parent ID"].loc[inds[row]:inds[min(row+100-1, len(inds)-1)]])

    comments = get_comments_from_reddit_api(idlist,headers)
    for i, comment in enumerate(comments):
        comment = comment['data']

        df["Attacker Race"][inds[i+row]] = (comment["author_flair_text"] if comment["author_flair_text"] != None else comment["author"])
        df["Time to reply"][inds[i+row]] = df["Time"][inds[i+row]] - comment["created_utc"]
    row += len(comments)
    if(row%2000 >= 1900):
        print(f"at row {row}")
        print(F"Saving as Scrubdata [{time.ctime()[4:13]}].pkl".replace(":","-"))
        df.to_pickle(F"Scrubdata [{time.ctime()[4:13]}].pkl".replace(":","-"))

    time.sleep(1)
print("\nScrubbing done")
print(F"Saving as Scrubdata [{time.ctime()[4:13]}].pkl".replace(":","-"))
df.to_pickle(F"Scrubdata [{time.ctime()[4:13]}].pkl".replace(":","-"))