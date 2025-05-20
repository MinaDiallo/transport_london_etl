import requests
import pandas as pd
import os
from datetime import datetime
import logging
from dotenv import load_dotenv

# Création des dossiers nécessaires
os.makedirs('data/raw', exist_ok=True)
os.makedirs('data/external', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extract.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()
API_KEY = os.getenv('TFL_API_KEY')
APP_ID = os.getenv('TFL_APP_ID')

def test_api_connection():
    url = "https://api.tfl.gov.uk/Line/Mode/tube"
    params = {
        'app_id': APP_ID,
        'app_key': API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logger.info("Connexion à l'API réussie")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur de connexion à l'API: {e}")
        return False

def get_tube_status():
    """
    Récupère le statut actuel des lignes de métro de Londres
    """
    url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
    params = {
        'app_key': API_KEY
    }
    
    try:
        logger.info("Récupération des données de statut du métro")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Transformation en DataFrame
        tube_status = []
        for line in data:
            for status in line['lineStatuses']:
                tube_status.append({
                    'line_id': line['id'],
                    'line_name': line['name'],
                    'status_severity': status.get('statusSeverity', None),
                    'status_description': status.get('statusSeverityDescription', None),
                    'reason': status.get('reason', None),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        
        df = pd.DataFrame(tube_status)
        
        # Sauvegarde des données
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'data/raw/tube_status_{timestamp}.csv'
        df.to_csv(filename, index=False)
        
        logger.info(f"Données sauvegardées dans {filename}")
        return df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la récupération des données: {e}")
        return None

def get_tube_arrivals(line_id=None):
    """
    Récupère les informations d'arrivée des trains pour une ligne spécifique
    ou pour toutes les lignes si line_id est None
    """
    params = {
        'app_id': APP_ID,
        'app_key': API_KEY
    }
    
    try:
        # Si une ligne spécifique est demandée
        if line_id:
            logger.info(f"Récupération des données d'arrivée pour la ligne {line_id}")
            url = f"https://api.tfl.gov.uk/Line/{line_id}/Arrivals"
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        
        # Si toutes les lignes sont demandées
        else:
            logger.info("Récupération de la liste des lignes de métro")
            # 1. D'abord récupérer la liste des lignes
            lines_url = "https://api.tfl.gov.uk/Line/Mode/tube"
            lines_response = requests.get(lines_url, params=params)
            lines_response.raise_for_status()
            lines_data = lines_response.json()
            
            # 2. Ensuite récupérer les arrivées pour chaque ligne
            data = []
            for line in lines_data:
                line_id = line['id']
                logger.info(f"Récupération des arrivées pour la ligne: {line_id}")
                arrivals_url = f"https://api.tfl.gov.uk/Line/{line_id}/Arrivals"
                arrivals_response = requests.get(arrivals_url, params=params)
                
                if arrivals_response.status_code == 200:
                    line_arrivals = arrivals_response.json()
                    data.extend(line_arrivals)
                    logger.info(f"Récupéré {len(line_arrivals)} arrivées pour la ligne {line_id}")
                else:
                    logger.warning(f"Échec de récupération des arrivées pour la ligne {line_id}: {arrivals_response.status_code}")
        
        # Transformation en DataFrame
        arrivals = []
        for arrival in data:
            arrivals.append({
                'line_id': arrival.get('lineId', None),
                'line_name': arrival.get('lineName', None),
                'station_name': arrival.get('stationName', None),
                'platform_name': arrival.get('platformName', None),
                'direction': arrival.get('direction', None),
                'destination_name': arrival.get('destinationName', None),
                'time_to_station': arrival.get('timeToStation', None),
                'expected_arrival': arrival.get('expectedArrival', None),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        df = pd.DataFrame(arrivals)
        
        # Sauvegarde des données
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        line_suffix = f"_{line_id}" if line_id else ""
        filename = f'data/raw/tube_arrivals{line_suffix}_{timestamp}.csv'
        df.to_csv(filename, index=False)
        
        logger.info(f"Données sauvegardées dans {filename}")
        return df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la récupération des données: {e}")
        return None

def get_stations_info():
    """
    Récupère les informations sur toutes les stations du réseau
    """
    url = "https://api.tfl.gov.uk/StopPoint/Mode/tube"
    params = {
        'app_key': API_KEY
    }
    
    try:
        logger.info("Récupération des informations sur les stations")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Transformation en DataFrame
        stations = []
        for stop_point in data.get('stopPoints', []):
            stations.append({
                'station_id': stop_point.get('id', None),
                'station_name': stop_point.get('commonName', None),
                'latitude': stop_point.get('lat', None),
                'longitude': stop_point.get('lon', None),
                'zone': stop_point.get('zone', None),
                'lines': [line.get('name', None) for line in stop_point.get('lines', [])],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        df = pd.DataFrame(stations)
        
        # Sauvegarde des données
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'data/raw/tube_stations_{timestamp}.csv'
        df.to_csv(filename, index=False)
        
        logger.info(f"Données sauvegardées dans {filename}")
        return df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors de la récupération des données: {e}")
        return None


def load_historical_ridership_data(excel_path='data/external/ridership_data.xls'):
    """
    Charge les données historiques de fréquentation des stations depuis un fichier Excel
    """
    try:
        logger.info(f"Chargement des données historiques de fréquentation depuis {excel_path}")
        # Utilisation de pd.read_excel au lieu de pd.read_csv
        df = pd.read_excel(excel_path)
        
        # Vérification que la colonne timestamp existe et conversion en datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        logger.info(f"Données de fréquentation chargées avec succès: {len(df)} entrées")
        return df
    except Exception as e:
        logger.error(f"Erreur de chargement des données de fréquentation: {e}")
        return None

def enrich_arrivals_with_ridership(arrivals_df, ridership_df):
    """
    Enrichit les données d'arrivées avec les données de fréquentation
    """
    try:
        if arrivals_df is None or ridership_df is None:
            logger.error("Impossible d'enrichir les données: données manquantes")
            return None
            
        logger.info("Enrichissement des données d'arrivées avec les données de fréquentation")
        
        # Convertir les timestamps en datetime pour la jointure
        arrivals_df['timestamp'] = pd.to_datetime(arrivals_df['timestamp'])
        
        # Extraire l'heure et le jour de la semaine pour la jointure
        arrivals_df['hour'] = arrivals_df['timestamp'].dt.hour
        arrivals_df['day_of_week'] = arrivals_df['timestamp'].dt.dayofweek
        
        # Jointure des données
        # Note: cette jointure suppose que ridership_df a des colonnes 'station_name', 'hour', 'day_of_week' et 'ridership'
        merged_df = pd.merge(
            arrivals_df,
            ridership_df[['station_name', 'hour', 'day_of_week', 'ridership']],
            on=['station_name', 'hour', 'day_of_week'],
            how='left'
        )
        
        # Sauvegarde des données enrichies
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'data/processed/enriched_arrivals_{timestamp}.csv'
        os.makedirs('data/processed', exist_ok=True)
        merged_df.to_csv(filename, index=False)
        
        logger.info(f"Données enrichies sauvegardées dans {filename}")
        return merged_df
        
    except Exception as e:
        logger.error(f"Erreur lors de l'enrichissement des données: {e}")
        return None

if __name__ == "__main__":
    if test_api_connection():
        # Récupération des données
        tube_status_df = get_tube_status()
        tube_arrivals_df = get_tube_arrivals()
        stations_info_df = get_stations_info()
        
        # Chargement des données historiques de fréquentation
        ridership_df = load_historical_ridership_data()
        
        # Enrichissement des données d'arrivées avec les données de fréquentation
        if tube_arrivals_df is not None and ridership_df is not None:
            enriched_df = enrich_arrivals_with_ridership(tube_arrivals_df, ridership_df)
            if enriched_df is not None:
                logger.info(f"Données enrichies créées avec succès: {len(enriched_df)} entrées")
        
        # Affichage des résultats
        if tube_status_df is not None:
            logger.info(f"Nombre de statuts de ligne récupérés: {len(tube_status_df)}")
        
        if tube_arrivals_df is not None:
            logger.info(f"Nombre d'arrivées récupérées: {len(tube_arrivals_df)}")
        
        if stations_info_df is not None:
            logger.info(f"Nombre de stations récupérées: {len(stations_info_df)}")
    else:
        logger.error("Impossible de se connecter à l'API TfL. Vérifiez vos identifiants.")