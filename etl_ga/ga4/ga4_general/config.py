import psycopg2
import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from dotenv import load_dotenv
load_dotenv()


# Set up the credentials
creds = Credentials.from_service_account_file('etl_ga/ga4/credentials/service_key.json')

# Set up the Analytics Admin API
analytics = build('analyticsdata', 'v1beta', credentials=creds)


def dwh_conn_psycopg2():
    conn = psycopg2.connect(
        host=os.environ.get('DWH_HOST'),
        database=os.environ.get('DWH_DB'),
        user=os.environ.get('DWH_USER'),
        password=os.environ.get('DWH_PASSWORD'))
    return conn



countries_list = [
["ua.jooble.org", 1], 
["ae.jooble.org", 64], 
["my.jooble.org", 62],
["de.jooble.org", 2], ["uk.jooble.org", 3], ["fr.jooble.org", 4], ["ca.jooble.org", 5], 
["us.jooble.org", 6], 
["id.jooble.com", 7], ["ru.jooble.org", 8], ["pl.jooble.org", 9], ["hu.jooble.org", 10], 
["ro.jooble.org", 11], ["es.jooble.org", 12], ["at.jooble.org", 13], ["be.jooble.org", 14], ["br.jooble.org", 15], 
["ch.jooble.org", 16], ["cz.jooble.org", 17], ["in.jooble.org", 18], ["it.jooble.org", 19], ["nl.jooble.org", 20], 
["tr.jooble.org", 21], ["by.jooble.org", 22], ["cl.jooble.org", 23], ["co.jooble.org", 24], ["gr.jooble.org", 25], 
["sk.jooble.org", 26], ["th.jooble.org", 27], ["tw.jooble.org", 28], ["ve.jooble.org", 29], ["bg.jooble.org", 30], 
["hr.jooble.org", 31], ["kz.jooble.org", 32], ["no.jooble.org", 33], ["rs.jooble.org", 34], ["se.jooble.org", 35], 
["nz.jooble.org", 36], ["ng.jooble.org", 37], ["ar.jooble.org", 38], ["mx.jooble.org", 39], ["pe.jooble.org", 40], 
["www.cn.jooble.org", 41], ["hk.jooble.org", 42], ["kr.jooble.org", 43], ["ph.jooble.org", 44], ["pk.jooble.org", 45], 
["jp.jooble.org", 46], ["cu.jooble.org", 47], ["pr.jooble.org", 48], ["sv.jooble.org", 49], ["cr.jooble.org", 50], 
["au.jooble.org", 51], ["do.jooble.org", 52], ["uy.jooble.org", 53], ["ec.jooble.org", 54], ["sg.jooble.org", 55], 
["az.jooble.org", 56], ["fi.jooble.org", 57], ["ba.jooble.org", 58], ["pt.jooble.org", 59], ["dk.jooble.org", 60], 
["ie.jooble.org", 61], ["za.jooble.org", 63], ["qa.jooble.org", 65], 
["sa.jooble.org", 66], ["kw.jooble.org", 67], ["bh.jooble.org", 68], ["eg.jooble.org", 69], ["ma.jooble.org", 70], 
["uz.jooble.org", 71],
]

country_domain_to_country_id = dict(countries_list)

# (domain, property_id)
property_list = [
 ["my.jooble.org", "properties/357473059"],
["ua.jooble.org", "properties/354983946"],
["ae.jooble.org", "properties/357164452"], 
["ar.jooble.org", "properties/356179970"], ["at.jooble.org", "properties/356598115"], 
["au.jooble.org", "properties/356594630"], ["az.jooble.org", "properties/357181715"], ["ba.jooble.org", "properties/357218545"], 
["be.jooble.org", "properties/357191130"], ["bg.jooble.org", "properties/357190427"], ["bh.jooble.org", "properties/357151116"], 
["br.jooble.org", "properties/356631340"], ["ca.jooble.org", "properties/356638251"], ["ch.jooble.org", "properties/356642861"], 
["cl.jooble.org", "properties/356621266"], ["co.jooble.org", "properties/356610561"], ["cr.jooble.org", "properties/357152636"], 
 ["cz.jooble.org", "properties/356642920"], ["de.jooble.org", "properties/356639269"], 
["dk.jooble.org", "properties/357184656"], ["do.jooble.org", "properties/357192918"], ["ec.jooble.org", "properties/357191132"], 
["eg.jooble.org", "properties/357186984"], ["es.jooble.org", "properties/357154839"], ["fi.jooble.org", "properties/356633079"], 
["fr.jooble.org", "properties/356631342"], ["gr.jooble.org", "properties/356631343"], ["hk.jooble.org", "properties/356639107"], 
["hr.jooble.org", "properties/357216912"], ["hu.jooble.org", "properties/355019595"], ["id.jooble.com", "properties/322914742"], 
["ie.jooble.org", "properties/356612188"], ["in.jooble.org", "properties/357220097"], ["it.jooble.org", "properties/356862062"], 
["jp.jooble.org", "properties/357497706"], ["kr.jooble.org", "properties/356842255"], ["kw.jooble.org", "properties/357496808"], 
["kz.jooble.org", "properties/356851459"], ["ma.jooble.org", "properties/357480056"], ["mx.jooble.org", "properties/356863984"], 
["ng.jooble.org", "properties/356871575"], ["nl.jooble.org", "properties/356880961"], 
["no.jooble.org", "properties/356840857"], ["nz.jooble.org", "properties/357481663"], ["pe.jooble.org", "properties/356842048"], 
["ph.jooble.org", "properties/357488518"], ["pk.jooble.org", "properties/357453568"], ["pl.jooble.org", "properties/357485014"], 
["pr.jooble.org", "properties/357488522"], ["pt.jooble.org", "properties/356871578"], ["qa.jooble.org", "properties/357490482"], 
["ro.jooble.org", "properties/355014349"], ["rs.jooble.org", "properties/357446398"], ["sa.jooble.org", "properties/357445521"], 
["se.jooble.org", "properties/356851065"], ["sg.jooble.org", "properties/357496251"], ["sk.jooble.org", "properties/357517302"], 
["sv.jooble.org", "properties/357477822"], ["th.jooble.org", "properties/356874398"], ["tr.jooble.org", "properties/356866298"], 
["tw.jooble.org", "properties/356854289"], ["uk.jooble.org", "properties/356849346"], 
["us.jooble.org", "properties/356846637"], 
["uy.jooble.org", "properties/357501984"], ["uz.jooble.org", "properties/357491168"], 
["www.cn.jooble.org", "properties/356638701"], ["za.jooble.org", "properties/357484099"],
]

country_domain_to_property_id = dict(property_list)

# key: Table name in DWH
request_params = {

    'ga4_general': {
        'dimensions': [
            {'name': 'date'},
            {'name': 'sessionDefaultChannelGrouping'},
            {'name': 'sessionSource'},
            {'name': 'sessionMedium'},
            {'name': 'deviceCategory'},
            {'name': 'country'},
        ],
        'metrics': [
            {'name': 'activeUsers'},
            {'name': 'sessions'},
            {'name': 'userEngagementDuration'},
            {'name': 'engagedSessions'},
            {'name': 'screenPageViews'},
            {'name': 'totalUsers'},
        ],
    },

    'ga4_user_and_adsense_tests': {
        'dimensions': [
            {'name': 'date'},
            {'name': 'customUser:dimension2'},
            {'name': 'customUser:dimension7'},
        ],
        'metrics': [
            {'name': 'sessions'},
            {'name': 'averageSessionDuration'},
            {'name': 'bounceRate'}
        ],
    },
}
