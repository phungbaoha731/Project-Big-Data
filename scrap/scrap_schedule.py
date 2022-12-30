import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlencode

urlId=-1
schedule_time = 4    # Số ngày trước để lấy data

def scrapTournament(url):
  params = {'api_key': 'ec2530e4965b509b02935dc29ca764d4', 'url': url}
  data = requests.get('http://api.scraperapi.com/', params=urlencode(params))  # Fake ip
  # data = requests.get(url)      # K fake ip
  soup = BeautifulSoup(data.text, 'html.parser')
  matches = pd.read_html(data.text)[0]
  
  indexDrop = matches[matches['Match Report'] !=  'Match Report'].index
  matches.drop(indexDrop, inplace=True)
  indexDrop = matches[matches['Score'] ==  'Score'].index
  matches.drop(indexDrop, inplace=True)

  # Lấy số lượng trận đấu cần crawl
  import datetime
  matches['Date'] = pd.to_datetime(matches['Date'], format='%Y-%m-%d')
  today = pd.to_datetime('now')
  countLinks = 0
  for i in matches['Date']:
    if today - i < datetime.timedelta(days = schedule_time):
      countLinks+=1
  if countLinks == 0: 
    return
  
  # Lấy số lượng link các trận 
  table = soup.find_all("table")[0]
  links = table.find_all("a")
  matchReports = []
  for link in links: 
    if link.text == "Match Report":
      matchReports.append(link)
  matchReports = matchReports[len(matchReports)-countLinks:]
  matchReportLinks = ["https://fbref.com" + l.get("href") for l in matchReports]
  global urlId
  matchReportLinks = matchReportLinks[urlId+1:]
  print(len(matchReportLinks))

  # Lấy data từng trận đấu
  for url in matchReportLinks:
    urlId+=1
    params = {'api_key': 'ec2530e4965b509b02935dc29ca764d4', 'url': url}
    data = requests.get('http://api.scraperapi.com/', params=urlencode(params))
    # data = requests.get(url)
    soup = BeautifulSoup(data.text, 'html.parser')
    matchID = url.split("/")[-2]
    
    try:
      dataPD = pd.read_html(data.text)
    except:
      urlId-=1
    
    table = soup.select("table.stats_table")
    tourName = soup.select("#content a")[0].text
    if len(dataPD) < 18:
      continue
    extraPD = 20 - len(dataPD)
    print(tourName, matchID, urlId)

    # shots
    shots = dataPD[extraPD-3]
    shots.insert(0 ,"Match ID", matchID, True)
    shots.insert(0 ,"Tournament", tourName, True)

    # playerStats
    links = table[0].find_all('a')
    links = [l.get("href") for l in links]
    playersID1 = []
    for link in links:
      if link.split("/")[2] == "players":
        playersID1.append(link.split("/")[-2])

    links = table[7].find_all('a')
    links = [l.get("href") for l in links]
    playersID2 = []
    for link in links:
      if link.split("/")[2] == "players":
        playersID2.append(link.split("/")[-2])

    playerStats1 = dataPD[extraPD-17]
    playerStats1 = playerStats1.drop([playerStats1.shape[0]-1])
    playerStats1.insert(0 ,"Player ID", playersID1, True)

    playerStats2 = dataPD[extraPD-10]
    playerStats2 = playerStats2.drop([playerStats2.shape[0]-1])
    playerStats2.insert(0 ,"Player ID", playersID2, True)

    playerStats = pd.concat([playerStats1, playerStats2])
    playerStats.insert(0 ,"Match ID", matchID, True)
    playerStats.insert(0 ,"Tournament", tourName, True)


    passingStats1 = dataPD[extraPD-16]
    passingStats1 = passingStats1.drop([passingStats1.shape[0]-1])
    passingStats1.insert(0 ,"Player ID", playersID1, True)

    passingStats2 = dataPD[extraPD-9]
    passingStats2 = passingStats2.drop([passingStats2.shape[0]-1])
    passingStats2.insert(0 ,"Player ID", playersID2, True)

    passingStats = pd.concat([passingStats1, passingStats2])
    passingStats.insert(0 ,"Match ID", matchID, True)
    passingStats.insert(0 ,"Tournament", tourName, True)


    passTypeStats1 = dataPD[extraPD-15]
    passTypeStats1 = passTypeStats1.drop([passTypeStats1.shape[0]-1])
    passTypeStats1.insert(0 ,"Player ID", playersID1, True)

    passTypeStats2 = dataPD[extraPD-8]
    passTypeStats2 = passTypeStats2.drop([passTypeStats2.shape[0]-1])
    passTypeStats2.insert(0 ,"Player ID", playersID2, True)

    passTypeStats = pd.concat([passTypeStats1, passTypeStats2])
    passTypeStats.insert(0 ,"Match ID", matchID, True)
    passTypeStats.insert(0 ,"Tournament", tourName, True)


    defensiveStats1 = dataPD[extraPD-14]
    defensiveStats1 = defensiveStats1.drop([defensiveStats1.shape[0]-1])
    defensiveStats1.insert(0 ,"Player ID", playersID1, True)

    defensiveStats2 = dataPD[extraPD-7]
    defensiveStats2 = defensiveStats2.drop([defensiveStats2.shape[0]-1])
    defensiveStats2.insert(0 ,"Player ID", playersID2, True)

    defensiveStats = pd.concat([defensiveStats1, defensiveStats2])
    defensiveStats.insert(0 ,"Match ID", matchID, True)
    defensiveStats.insert(0 ,"Tournament", tourName, True)


    possessionStats1 = dataPD[extraPD-13]
    possessionStats1 = possessionStats1.drop([possessionStats1.shape[0]-1])
    possessionStats1.insert(0 ,"Player ID", playersID1, True)

    possessionStats2 = dataPD[extraPD-6]
    possessionStats2 = possessionStats2.drop([possessionStats2.shape[0]-1])
    possessionStats2.insert(0 ,"Player ID", playersID2, True)

    possessionStats = pd.concat([possessionStats1, possessionStats2])
    possessionStats.insert(0 ,"Match ID", matchID, True)
    possessionStats.insert(0 ,"Tournament", tourName, True)


    miscellaneousStats1 = dataPD[extraPD-13]
    miscellaneousStats1 = miscellaneousStats1.drop([miscellaneousStats1.shape[0]-1])
    miscellaneousStats1.insert(0 ,"Player ID", playersID1, True)

    miscellaneousStats2 = dataPD[extraPD-6]
    miscellaneousStats2 = miscellaneousStats2.drop([miscellaneousStats2.shape[0]-1])
    miscellaneousStats2.insert(0 ,"Player ID", playersID2, True)

    miscellaneousStats = pd.concat([miscellaneousStats1, miscellaneousStats2])
    miscellaneousStats.insert(0 ,"Match ID", matchID, True)
    miscellaneousStats.insert(0 ,"Tournament", tourName, True)

    # gkStats
    gkStats1 = dataPD[extraPD-11]
    links = table[6].find_all('a')
    links = [l.get("href") for l in links]
    playersID = []
    for link in links:
      if link.split("/")[-3] == "players":
        playersID.append(link.split("/")[-2])
    gkStats1.insert(0 ,"Player ID", playersID, True)

    gkStats2 = dataPD[extraPD-4]
    links = table[13].find_all('a')
    links = [l.get("href") for l in links]
    playersID = []
    for link in links:
      if link.split("/")[-3] == "players":
        playersID.append(link.split("/")[-2])
    gkStats2.insert(0 ,"Player ID", playersID, True)

    gkStats = pd.concat([gkStats1, gkStats2])
    gkStats.insert(0 ,"Match ID", matchID, True)
    gkStats.insert(0 ,"Tournament", tourName, True)


    # Overview
    stats = soup.select(".score")
    scores = []
    for score in stats:
      scores.append(score.text)

    stats = soup.select("#team_stats")[0].find_all("tr")
    teams = []
    for th in stats[0]:
      if not th.text == "\n":
        teams.append(th.text.replace("\n","").replace("\t","").replace(" ",""))

    dataframe = {}
    for index in range(2, len(stats)-2, 2):
      dataframe[stats[index-1].text] = []
    for index in range(2, len(stats)-2, 2):
      for td in stats[index].find_all("td"):
        dataframe[stats[index-1].text].append(td.text.replace("\n","").replace("\xa0",""))

    dataframe[stats[len(stats)-2].text] = []
    for td in stats[-1].find_all("td"):
      dataframe[stats[len(stats)-2].text].append({"Yellow Card":len(td.select(".yellow_card")), "Red Card":len(td.select(".red_card"))})

    teamStats = pd.DataFrame(dataframe)
    teamStats.insert(0 ,"Squad", teams, True)
    teamStats.insert(0 ,"Score", scores, True)
    teamStats.insert(0 ,"Match ID", matchID, True)
    teamStats.insert(0 ,"Tournament", tourName, True)

    extras = soup.select("#team_stats_extra>div")
    dataframe = {}
    for extra in extras:
      extra = extra.find_all("div")
      for index in range(3, len(extra), 3):
        dataframe[extra[index+1].text] = []
      for index in range(3, len(extra), 3):
        dataframe[extra[index+1].text].append(extra[index].text)
        dataframe[extra[index+1].text].append(extra[index+2].text)

    extraStats = pd.DataFrame(dataframe)
    teamStats = pd.concat([teamStats, extraStats], axis=1)

    # Read to file
    all = [shots, playerStats, passingStats, passTypeStats, defensiveStats, possessionStats, miscellaneousStats, gkStats, teamStats]
    index=1
    for stat in all:
      df = pd.DataFrame(stat)
      df.to_csv("stat_{stat}-{today}.csv".format(stat=index, today = datetime.datetime.now().strftime('%Y-%m-%d')), index=False, header=False, mode="a")
      index+=1
    print("Read to csv file completed")
        
def schedule_scrap(urls):
  global urlId
  for url in urls:  
    while 1:
      try:
        scrapTournament(url)
        urlId=-1
        break
      except:
        print('Access Failed!')
        continue

if __name__ == "__main__":
  url3 = 'https://fbref.com/en/comps/8/schedule/Champions-League-Scores-and-Fixtures'
  url2 = 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures'
  url4 = 'https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures'
  url6 = 'https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures'
  url8 = 'https://fbref.com/en/comps/10/schedule/Championship-Scores-and-Fixtures'
  url5 = 'https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures'
  url10 = 'https://fbref.com/en/comps/14/schedule/Copa-Libertadores-Scores-and-Fixtures'
  url12 = 'https://fbref.com/en/comps/19/schedule/Europa-League-Scores-and-Fixtures'
  url11 = 'https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures'
  url7 = 'https://fbref.com/en/comps/22/schedule/Major-League-Soccer-Scores-and-Fixtures'
  url13 = 'https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures'
  url9 = 'https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures'
  url14 = 'https://fbref.com/en/comps/31/schedule/Liga-MX-Scores-and-Fixtures'
  url15 = 'https://fbref.com/en/comps/32/schedule/Primeira-Liga-Scores-and-Fixtures'

  urls = [url2, url3, url4, url5, url6, url7, url8, url9, url10, url11, url12, url13, url14, url15]
  schedule_scrap(urls)
  