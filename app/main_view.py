import streamlit as st
from app.globals import get_logger

st.title("Salut l'équipe!")

st.write("Moi c philibert")

logger = get_logger()
logger.info("Hello par ici")