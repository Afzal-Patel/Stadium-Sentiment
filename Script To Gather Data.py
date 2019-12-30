import os
import csv
import zipfile
import datetime
import rapidjson as json # pip3 install python-rapidjson
import sys


def traverse(filename): 
    #gathered tweets
    counter = 0

    #file data
     #days between 10/26/17 - 11/16/19
    #good_files = 0 #checked
    #bad_files = 0
    #bad_tweets = 0

#REMOVE duplicates from data.csv......
    keywords = ['new stadium',
        'stadium construction',
        'sofistadium',
        'sofi stadium', 'nfl la', 'la football',
        'ramsNFL', 
        'RamsHouse',  
        'LARams',  
        'la rams', 'rams fan', 
        '@Chargers',  
        'BoltUp', 
        'LAChargers',
        'la chargers', 'chargers fan', 

        'AllegiantStadm',
        'allegiantstadium',
        'allegiant stadium',
        'Las Vegas Raiders',
        'Lv raiders',
        'Oakland raiders',
        '@Raiders', '#Raiders', 'RaiderNation',
        'raider nation',
        'raider fans' ,
        'raiders stadium' ,

        'Protectivestadium',
        'protective stadium',
        'protectivestdm',       
        'University of Alabama at Burmingham Football',
        'UAB football', 'go blazers',
        'goblazers',
        'UABgreengang',
        'UAB_FB',
        'BJCC' ,

        'globelifefield',
        'globe life field',
        'globe field',
        'globefield',
        'rangers',
        'rangers fan',
        'rangers fans',
        'rangers ballpark',
        'rangers stadium',
        'rangers field',
        'rangersballpark',
        'rangersstadium',
        'rangersfield' ,

        'oakstadium',
        'oak stadium',
        'oaklandstadium',
        'oakland stadium',
        'oaklandathletics',
        'oakland athletics',
        '@athletics',
        'bjarke ingels',
        'howard terminal',
        'howardterminal',
        'jack london square' ,

        'san diego west stadium',
        'SDSUWest',
        'sd west stadium',
        'san diego stadium',
        'sandiegostadium',
        'weststadium',
        'sdweststadium',
        'san diego football',
        'sd football',
        'sdfootball',
        'mission valley stadium',
        'sdsu mission valley',
        'sdccu stadium', 

        'new aloha stadium',
        'aloha stadium',
        'newaloha',
        'Aloha Stadium Hawaii',
        'hawaiifb',
        'AlohaStadiumHI',
        'newalohastadium',
        'aloha stadium',
        'hawaii football',
        'hawaiifootball',
        'u of hawaii football',
        'hawaiifb' ,

        'nashville fairgrouds',
        'FAIRGROUNDSNASH',
        'nashvillefairgrounds',
        'nashville fairground',
        'nashvillefairgound',
        'fc nashville',
        'fcnashville',
        'nashville soccer',
        'nashvillesoccer',
        'nashville sc',
        'nashvillesc' ,

        'cincinnati stadium',
        'cincinnati stadium',
        'cincinati stadium',
        'FCCincy',
        'cincinatistadium',
        'west end stadium',
        'westendstadium',
        'westendstdm',
        'cincinnati fc',
        'cincinnatifc',
        'cincinatifc',
        'fc cincinnati',
        'fccincinnati',
        'fc cincinati',
        'fccincinati' ,

        'miami freedom stadium',
        'miafreedompark',
        'miamifreedomstadium',
        'miami stadium',
        'Inter Miami CF stadium',
        'miamistadium',
        'inter miami',
        'inter maimi',
        'miami cf',
        'miami fc',
        'intermiami',
        'intermaimi',
        'miamicf',
        'miamifc',
        'intermiami',
        'intermaimi',
        'InterMiamiCF',
        'freedom stadium',
        'freedomstadium']


    try:
    
        datadir = '/data/Twitter dataset/'
        with zipfile.ZipFile(datadir+filename, 'r') as archive:
            print(datetime.datetime.now(),filename)
            with open('data.csv', mode='a') as csv_file:

                names = [ 'Time' , 'Timestamp' , 'Text' , 'Place' , 'Lang' , 'Filter' , 'Sensitive?', 'Coords','Stadium' ]
                writer = csv.DictWriter(csv_file, fieldnames = names)
                #writer.writeheader()

                #good_files += 1
                for subfilename in archive.namelist():
                    print(datetime.datetime.now(),filename,subfilename)
                    with archive.open(subfilename) as f:
                        #line changed to tweet
                        for line in f:
                            try:
                                tweet = json.loads(line)

                            # check to see if the entry actually corresponds to a tweet
                                try:
                                    tweet['place']
                                    tweet['text']
                                except KeyError:
                               # bad_tweets +=1
                                    continue

                                time = str(tweet['created_at'])
                                tStamp = str(tweet['timestamp_ms'])
                                text = str(tweet['text'])
                                place = str(tweet['place']['full_name'])
                                lang = str(tweet['lang'])
                                filt = str(tweet['filter_level'])
                                sens = str(tweet['possibly_sensitive'])

                                coords = str(tweet['coordinates'])
                                
                                for keyword in keywords:
                                    a = keyword.upper()
                                    b = text.upper()

                                    if(a in b):
                                        #later used to arrange data
                                        index = str(keywords.index(keyword))
                                        writer.writerow({'Time':time, 'Timestamp':tStamp, 'Text':text, 'Place':place, 'Lang':lang, 'Filter':filt, 'Sensitive?':sens, 'Coords':coords, 'Stadium':index})                                        
                                        counter+=1
                            except:
                                continue
             #written to nohup.txt
                print()
                print("mining... ")
                print("# tweets gathered: " + str(counter))

            #bad zip files
    except zipfile.BadZipFile:
        print("...skipping file")
        #bad_files += 1
        
        

def main():
    first_arg = sys.argv[1]
    traverse(first_arg)
  #  print("Complete...")

if __name__ == "__main__":
    main()
