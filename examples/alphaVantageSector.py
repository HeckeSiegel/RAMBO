from alpha_vantage.sectorperformance import SectorPerformances
from datetime import datetime
from pytz import timezone
from elasticsearch import Elasticsearch
import time

# reads sector performance from alpha vantage and writes it into elasticsearch
# every 10 seconds

es=Elasticsearch([{'host':'localhost','port':9200}])

while True:
    # get sector performance from alpha vantage                
    sp = SectorPerformances(key='JLD6KKU8CQTZD02V',output_format='json')
    data, meta_data = sp.get_sector()
    performance = data['Rank A: Real-Time Performance']
    
    #change timezone and bring into right format for elasticsearch
    date_us = timezone('US/Eastern').localize(datetime.strptime(meta_data['Last Refreshed'][:19], "%Y-%m-%d %H:%M:%S"))
    date_utc = date_us.astimezone(timezone('UTC'))
    date_es = date_utc.strftime("%Y-%m-%d"'T'"%H:%M:%S")
    
    print(performance)
    
    for sector in performance:
        doc = {}
        doc['sector'] = sector
        doc['change'] = performance[sector]
        doc['date'] = date_es
        res = es.index(index="sector", body=doc)
        
    time.sleep(60)
        
