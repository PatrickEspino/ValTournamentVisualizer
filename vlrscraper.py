import re
import psycopg2
import requests
from bs4 import BeautifulSoup

# create a connection to the PostgreSQL database
conn = psycopg2.connect(
    database="valstatsdb",
    user="postgres",
    password="Passw0rd!"
)


cursor = conn.cursor()


player_stats_url = "https://www.vlr.gg/stats/?event_group_id=45&event_id=all&region=all&country=all&min_rounds=50&min_rating=1550&agent=all&map_id=all&timespan=60d"
america_stats_url = "https://www.vlr.gg/event/1189/champions-tour-2023-americas-league"
emea_stats_url = "https://www.vlr.gg/event/1190/champions-tour-2023-emea-league"
pacific_stats_url = "https://www.vlr.gg/event/1191/champions-tour-2023-pacific-league"

team_map = {'Cloud9':'C9', 'Leviatán':'LEV', 'NRG Esports':'NRG', 'FURIA':'FUR', 'Evil Geniuses':'EG', '100 Thieves':'100T', 'Sentinels':'SEN', 'KRÜ Esports':'KRÜ', 'FNATIC':'FNC', 'Natus Vincere':'NAVI', 'Giants Gaming':'GIA', 'Team Liquid':'TL', 'FUT Esports':'FUT',
            'BBL Esports':'BBL', 'Team Vitality':'VIT', 'Team Heretics':'TH', 'Karmine Corp':'KC', 'Paper Rex':'PRX', 'Gen.G':'GEN', 'ZETA DIVISION':'ZETA', 'Rex Regum Qeon':'RRQ', 'Team Secret':'TS', 'Global Esports':'GES', 'Talon Esports':'TLN', 'DetonatioN FocusMe':'DFM'}


def create_tables():
    player_table = "player_stats"
    team_table = "team_stats"
    drop_table(player_table)
    drop_table(team_table)

    create_player_stats_query = """
    CREATE TABLE IF NOT EXISTS player_stats (
        id SERIAL PRIMARY KEY,
        player_name VARCHAR(255),
        team VARCHAR(255),
        rounds INTEGER,
        rating FLOAT,
        ACS FLOAT,
        kd FLOAT,
        kast FLOAT,
        ADR FLOAT,
        KPR FLOAT,
        APR FLOAT,
        FKPR FLOAT,
        FDPR FLOAT,
        HS FLOAT,
        CL FLOAT,
        KMAX INTEGER,
        kills INTEGER,
        deaths INTEGER,
        assists INTEGER,
        FK INTEGER,
        FD INTEGER,
        fkfd INTEGER
    );
    """
    cursor.execute(create_player_stats_query)

    create_team_stats_query = """
    CREATE TABLE IF NOT EXISTS team_stats (
        team VARCHAR(255) PRIMARY KEY,
        win INTEGER,
        loss INTEGER,
        tie INTEGER,
        maps VARCHAR(255),
        rounds VARCHAR(255),
        delta INTEGER
    )
    """
    cursor.execute(create_team_stats_query)


def drop_table(table_name):
    drop_table_query = """
    DROP TABLE IF EXISTS {}
    """.format(table_name)
    cursor.execute(drop_table_query)

def scrape_player_data(url):
    response= requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", {"class": "wf-table mod-stats mod-scroll"})

    name = []
    team = []
    data = []

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) == 21:
            row_data = []
            if cells[0]:
                value = cells[0].get_text().strip().split('\n')
                name.append(value[0])
                team.append(value[1])
            for cell in cells:
                row_data.append(cell.get_text().strip().split('\n'))
            data.append(row_data)


    for i,player in enumerate(data):
        name = [player[0][0]]
        team = [player[0][1]]
        del data[i][0]
        data[i].insert(0,name)
        data[i].insert(1,team)
        # data[i][2] = re.findall(r'\d+', player[2][0])
        del data[i][2]
        data[i][6] = [float(int(data[i][6][0].replace('%','').strip())/100)]
        data[i][12] = [float(int(data[i][12][0].replace('%','').strip())/100)]
        if '' in data[i][13]:
            data[i][13] = [0]
        else:
            data[i][13] = [float(int(data[i][13][0].replace('%', '').strip())/100)]

        if name == ['s0m']:
            data[i][1] = ['NRG']

    # for player in data:
    #     if player[0][0] == 'yay':
    #         data.remove(player)
    return data

def scrape_team_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", {"class": "wf-table mod-simple mod-group"})

    team_data = []

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) == 7:
            row_data = []
            if cells[0]:
                team_name = cells[0].get_text().strip().split('\n')[0].replace('\t','')
                if team_name in team_map:
                    team_name = team_map[team_name]
                row_data.append(team_name)
            for cell in cells[1:]:
                row_data.append(cell.get_text().strip().replace('\t',''))
            team_data.append(row_data)
    return team_data

def add_player_table(player_stats_data):
    for row in player_stats_data:
        new_data = [item for sublist in row for item in sublist]
        player_name = new_data[0]
        team_name = new_data[1]
        rounds = int(new_data[2])
        rating = float(new_data[3])
        acs = float(new_data[4])
        kd = float(new_data[5])
        kast = float(new_data[6])
        adr = float(new_data[7])
        kpr = float(new_data[8])
        apr = float(new_data[9])
        fkpr = float(new_data[10])
        fdpr = float(new_data[11])
        hs = float(new_data[12])
        cl = float(new_data[13])
        kmax = int(new_data[15])
        kills = int(new_data[16])
        deaths = int(new_data[17])
        assists = int(new_data[18])
        fk = int(new_data[19])
        fd = int(new_data[20])
        fkfd = fk-fd

        insert_query = """
        INSERT INTO player_stats (player_name, team, rounds, rating, acs, kd, kast, adr, kpr, apr, fkpr, fdpr, hs, cl, kmax, kills, deaths, assists, fk, fd, fkfd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (player_name, team_name, rounds, rating, acs, kd, kast, adr, kpr, apr, fkpr, fdpr, hs, cl, kmax, kills, deaths, assists, fk, fd, fkfd)
        try:
            cursor.execute(insert_query, values)
            conn.commit()
            print('Data inserted successfully')
        except Exception as e:
            print('Error inserting data:', e) 


def add_team_table(team_data):
    for new_data in team_data:
        # new_data = [item for sublist in row for item in sublist]
        # print(new_data)
        team_name = new_data[0]
        win = new_data[1]
        loss = new_data[2]
        tie = new_data[3]
        maps = new_data[4]
        rounds = new_data[5]
        ratio = rounds.split('/')
        rwin = int(ratio[0])
        rloss = int(ratio[1])
        delta = rwin-rloss

        insert_query = """
        INSERT INTO team_stats(team, win, loss, tie, maps, rounds, delta) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        values = (team_name, win, loss, tie, maps, rounds, delta)

        try:
            cursor.execute(insert_query, values)
            conn.commit()
            print("Data inserted Successfully")
        except Exception as e:
            print("Error inserting data: ", e)
    

player_data = scrape_player_data(player_stats_url)
america_data = scrape_team_data(america_stats_url)
emea_data = scrape_team_data(emea_stats_url)
pacific_data = scrape_team_data(pacific_stats_url)

team_data = america_data + emea_data + pacific_data
# print(player_data[46][1][0] == team_data[26][0])

create_tables()
add_player_table(player_data)
add_team_table(team_data)

conn.commit()

cursor.close()
conn.close()


