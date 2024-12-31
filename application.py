import os
import sys

from streamlit import Page, navigation, set_page_config

# ajouter le dossier racine a la liste de recherche de chemin des modules pour y accéder depuis le code streamlit
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# variables d'environement a mettre pour que pyspark fonctionne
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# choisir le layout wide pour prendre tout l'espace disponible sur l'écran
set_page_config(layout="wide")

main_view = Page("app/main_view.py", title="Dashboard", icon="📊")
roi_finder = Page("app/roi_finder.py", title="Recherche de stocks profitables", icon="🔍")

# liste des pages de l'application streamlit
nav = navigation([main_view, roi_finder])
nav.run()





