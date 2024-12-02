import os
import sys

from streamlit import Page, navigation

# ajouter le dossier racine a la liste de recherche de chemin des modules pour y accéder depuis le code streamlit
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# variables d'environement a mettre pour que pyspark fonctionne
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

main_view = Page("app/main_view.py", title="Vue principale", icon="💹")

# liste des pages de l'application streamlit
nav = navigation([main_view])
nav.run()





