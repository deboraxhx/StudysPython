import requests
import pandas as pd
import os
import time
from itertools import product

class IngestorApi():
    DATA_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
    FILE_PATH = "C:\\Users\\Debora\\Desktop\\arquivos\\table.parquet"
    SEASON_ID = {'BL1': 1592, 'SA': 1600, 'PPL': 1603, 'BSA': 1557,
                'PL': 1564, 'PD': 1577, 'FL1': 1595, 'ELC': 1573}
    HEADERS = {'X-Auth-Token': 'ef3c92f51f654f2ca751c60b44ccd527'}
    def __init__(self):
        self.url = 'https://api.football-data.org/v4'
        self.season = [2020, 2021, 2022, 2023]
        self.leagues = ['BL1', 'BSA', 'PD', 'FL1', 'ELC', 'PPL', 'SA', 'PL']
        self.current_season = 2023
               
    def resp_currentMatchday(self, league, year, **kwargs): # essa parte a API não está me retornando nada
        try:
            resp = requests.get(f"{self.url}/competitions/{league}/standings?season={year}", headers=self.HEADERS)
            while resp.status_code != 200:
                time.sleep(60)
                resp = self.resp_currentMatchday(league, year)
            return resp
        
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição HTTP: {e}")
            return None
    
    def currentMatchday(self, league, year, **kwargs):
        resp = self.resp_currentMatchday(league, year)
        return resp.json()['season']['currentMatchday']
    
        
    def resp_standings(self, league, year, matchday, **kwargs):
        try:
            resp = requests.get(f"{self.url}/competitions/{league}/standings?season={year}&matchday={matchday}", headers=self.HEADERS)
            while resp.status_code != 200:
                time.sleep(60)
                resp = self.resp_standings(league, year, matchday)
            return resp
        
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição HTTP: {e}")
            return None
                            
    def save_data(self, data):
        if isinstance(data, pd.DataFrame):
            pass
        else:
            print('Não é um dataframe')
        if os.path.exists(self.FILE_PATH):
            existing_df = pd.read_parquet(self.FILE_PATH)            
            combined_df = pd.concat([existing_df, data], ignore_index=True)
            sorted_df = combined_df.sort_values(by=['season', 'playedGames'], ascending=False)
            df = sorted_df.drop_duplicates(subset='id', keep='first')
            df.to_parquet(self.FILE_PATH, index=False)               
        else:
                # Se o arquivo Parquet não existir, crie um novo com os novos dados
            data.to_parquet(self.FILE_PATH, index=False) 
                            
    def create_dataframe(self, data, matchday, league, year):
        df = pd.DataFrame.from_dict(pd.json_normalize(data['standings'][0]['table']), orient='columns')
        df[['league','season','matchday']] = league, int(year), int(matchday)
        df['id'] = df['id'] = df['league'] + df['season'].astype(str) + df['team.tla'] + df['matchday'].astype(str)
        return df                                                    
                            
    def get_and_save_data_complete(self, **kwargs):
        table = []
        league_year_combinations = product(self.leagues, self.season)

        for league, year in league_year_combinations:
            current_matchday = self.resp_currentMatchday(league, year).json()['season']['currentMatchday']
            
            if current_matchday is not None:
                for i in range(1, current_matchday + 1):
                    print(f'{league}, {year}, {i}')
                    resp_standings = self.resp_standings(league, year, matchday=i)
                    data_json = resp_standings.json()
                    data = self.create_dataframe(data=data_json, matchday=i, league=league, year=year)
                    table.append(data)
            
        data = pd.concat(table, ignore_index=True)
        self.save_data(data)
        
    def filtrar(self, data, league):
        partida_recente = data.query(f'league=="{league}" & season=={self.current_season}')['matchday'].max()
        return int(partida_recente)
    
    def last_matchday(self):
        data = pd.read_parquet(self.FILE_PATH)
        last_matchday = {league: int(self.filtrar(data, league)) for league in self.leagues}
        return last_matchday
        
    def get_att(self, **kwargs):
        table = []
        for league in self.leagues:
            api_last_matchday = self.currentMatchday(league, self.current_season)
            lastmatchday = self.last_matchday()
            if lastmatchday[league] < api_last_matchday:
                for i in range(lastmatchday[league], api_last_matchday+1):
                    print(f"Atualizando a liga {league}")
                    resp_standings = self.resp_standings(league, self.current_season, matchday=i)
                    data_json = resp_standings.json()
                    data = self.create_dataframe(data=data_json, matchday=i, league=league, year=self.current_season)
                    table.append(data)
                data = pd.concat(table, ignore_index=True)
                self.save_data(data)
            else:
                print(f'Os dados da {league} estão atualizados')

    def auto_save(self):
        if os.path.exists(self.FILE_PATH):
            print('Os dados existem, pegando atualização')
            self.get_att()
        else:
            self.get_and_save_data_complete()
            print('Os dados não existem, pegando todos os dados disponiveis')

        

        