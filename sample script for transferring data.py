
import pandas as pd
import numpy as np

from datetime import (
    datetime,
    timedelta
) 
from bson.tz_util import FixedOffset
from mongo_manager import MongoDBManager
from database_manager import DatabaseManager

from helper import (
    add_quote,
    write_to_db,
)

def process_assigned_zone(x,field):
    if isinstance(x,dict):
        return x[field]
    return None



mongo_manager = MongoDBManager()
agent_online_offline_logs = mongo_manager.get_collection(
    dbname = 'SampleDatabase',
    collection_name='SampleCollection'
)
agent_lists = mongo_manager.get_collection(
    dbname='SampleDatabase',
    collection_name='SampleCollections'
)
# filtering criteria
CURRENT_DATE = datetime.utcnow()
LAST_DATE = (CURRENT_DATE - timedelta(1))
#LAST_DATE = LAST_DATE.replace('T',' ')

query_for_logs = {}
query_for_logs['DemoCreatedDate'] = {
    u"$gte" : LAST_DATE
}
projection_for_logs = {
    '_id' : 1,
    'DemoCreatedDate' : 1,
    'AgentId' : 1,
    'OnlineDurationMinutes' : 1,
    'AssignedZone' : 1,
    
    
}

cursor_logs = agent_online_offline_logs.find(query_for_logs,projection_for_logs)
output_logs = pd.DataFrame(list(cursor_logs))
unique_agent_id = output_logs['AgentId'].unique().tolist()

query_for_agent = {
    '_id' : {
        '$in' : unique_agent_id
    }
}
projection_for_agents = {
    '_id' : 1,
    'FirstName' : 1,
    'LastName' : 1,
    'PhoneNumber' : 1,
    'Email' : 1
}
cursor_agent = agent_lists.find(query_for_agent,projection_for_agents)
output_agent = pd.DataFrame(list(cursor_agent))
output_agent.set_index('_id',inplace=True)

joined_output = output_logs.join(output_agent,on='AgentId')
joined_output['zone_id'] = joined_output['AssignedZone'].apply(lambda x : process_assigned_zone(x,'_id'))
joined_output['zone_name'] = joined_output['AssignedZone'].apply(lambda x : process_assigned_zone(x,'Name'))
joined_output['provider_name'] = joined_output['FirstName'] + ' '+ joined_output['LastName']

joined_output.rename(
    columns={
        '_id' : 'id',
        'AgentId' : 'provider_id',
        'DemoCreatedDate' : 'date',
        'PhoneNumber' : 'provider_mobile',
        'OnlineDurationMinutes' : 'total_time',
        
    },
    inplace=True
)

joined_output['latitude'] = np.nan
joined_output['longitude'] = np.nan
joined_output['online_at'] = np.nan
joined_output['offline_at'] = np.nan

final_df = joined_output[
    [
        'id',
        'date',
        'provider_id',
        'provider_name',
        'provider_mobile',
        'latitude',
        'longitude',
        'online_at',
        'offline_at',
        'total_time',
        'zone_id',
        'zone_name'
        
    ]
] 

file_creds ='live_foods_db.json'
dbmanager = DatabaseManager(file=file_creds)
write_to_db(
    table='tablename',
    db=dbmanager,
    df=final_df
)
dbmanager.close()






