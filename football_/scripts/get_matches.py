import requests
import pandas as pd
from datetime import date, timedelta 
import os

class IngestorApi():
    FILE_NAME = "partidas.parquet"
    PATH = "C:\\Users\\Debora\\Desktop\\arquivos"
    SEASON_ID = {'BL1': 1592, 'SA': 1600, 'PPL': 1603, 'BSA': 1557,
                'PL': 1564, 'PD': 1577, 'FL1': 1595, 'ELC': 1573}
    
    def __init__(self):
        self.url = 'https://api.football-data.org/v4/'
        self.headers = { 'X-Auth-Token': 'ef3c92f51f654f2ca751c60b44ccd527' }
        self.season = [2020, 2021, 2022, 2023]
        self.leagues = ['BL1', 'BSA', 'PD', 'FL1', 'ELC', 'PPL', 'SA', 'PL']
        self.file_path = os.path.join(self.PATH, self.FILE_NAME)
        self.current_season = 2023
    
    def get_resp_season_complete(self, year, league, **kwargs):
        resp_season = requests.get(f"{self.url}competitions/{league}/matches?season={year}&status=FINISHED", headers=self.headers)
        return resp_season
     
    def last_matchday(self, league):       
        last_matchday = pd.read_parquet(self.file_path)[pd.read_parquet(self.file_path)['season.id'] == self.SEASON_ID[league]]['season.currentMatchday'].max()
        return last_matchday
    
    def resp_matchday(self, league, season, n_matchdays):
        resp_matchday = requests.get(f"{self.url}competitions/{league}/matches?season={season}&matchday={self.last_matchday(league)+n_matchdays}", headers=self.headers)
        return resp_matchday
    
    def create_dataframe(self, data):
        return pd.DataFrame.from_dict(pd.json_normalize(data['matches']), orient='columns')
    
    def save_data(self, data):
        if isinstance(data, pd.DataFrame):
            pass
        else:
            df = self.create_dataframe(data)
            if os.path.exists(self.file_path):
                existing_df = pd.read_parquet(self.file_path)            
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset='id')
                combined_df.to_parquet(self.file_path, index=False)               
            else:
                # Se o arquivo Parquet não existir, crie um novo com os novos dados
                df.to_parquet(self.file_path, index=False) 
    
    def get_and_save_season(self, **kwargs):
        for league in self.leagues:
            for year in self.season:
                resp_season = self.get_resp_season_complete(year=year, league=league, **kwargs)
                data = resp_season.json()
                if 'matches' in data:
                    self.save_data(data)
                    print(f"Ano {year} da Liga {league} salvo.")
                else:
                    print(f"Ano {year} da Liga {league} não possui dados disponíveis.")
                    
    
    def get_and_save_att(self, **kwargs):
        for league in self.leagues:
            resp_season = self.get_resp_season_complete(year=self.current_season, league=league, **kwargs)
            data = resp_season.json()
            if 'matches' in data:
                self.save_data(data)
                print(f"Temporada atual da Liga {league} salva.")
            else:
                print('Erro')
    
    def get_next_matchdays(self, n_matchdays, **kwargs):
        data_list = []
        
        for league in self.leagues:
            resp_matcheday = self.resp_matchday(league, self.current_season, n_matchdays)
            data = resp_matcheday.json()  
            if 'matches' in data:
                df = self.create_dataframe(data)
                data_list.append(df)
            else:
                print(f"Erro league {league}")
                pass
            
        return pd.concat(data_list, ignore_index=True)
    

    def auto_save(self):
        if os.path.exists(self.file_path):
            print('Os dados existem, pegando atualização')
            self.get_and_save_att()

        else:
            print('Os dados não existem, pegando todos os dados disponiveis')
            self.get_and_save_season()
            
