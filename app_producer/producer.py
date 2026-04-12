import streamlit as st
import json
import os
import random
import requests
from datetime import datetime
from kafka import KafkaProducer

# Config
TRIVIA_API_URL = "https://the-trivia-api.com/v2/questions"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "d7dr83mgq0q78n6tjdvg.any.eu-west-2.mpx.prd.cloud.redpanda.com:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "quiz-reponses")

KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "kafka")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "uXoYCCvqPLeD8ZOq7jQFUDawQaJwaT")

CATEGORIES = {
    "🌍 Toutes": None,
    "🌐 Culture générale": "general_knowledge",
    "🏛️ Histoire": "history",
    "🗺️ Géographie": "geography",
    "🔬 Science": "science",
    "🎬 Cinéma & TV": "film_and_tv",
    "🎵 Musique": "music",
    "🍕 Cuisine": "food_and_drink",
    "⚽ Sport & Loisirs": "sport_and_leisure",
}

DIFFICULTES = {
    "Facile": "easy",
    "Moyen": "medium",
    "Difficile": "hard",
}

def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
    except Exception as e:
        st.error(f"Kafka non connecté : {e}")
        return None

def fetch_questions(nb, difficulte, categorie):
    params = {"limit": nb, "difficulty": difficulte}
    if categorie:
        params["categories"] = categorie
    try:
        resp = requests.get(TRIVIA_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        questions = []
        for item in resp.json():
            choix = item["incorrectAnswers"] + [item["correctAnswer"]]
            random.shuffle(choix)
            questions.append({
                "id": item["id"],
                "question": item["question"]["text"],
                "choix": choix,
                "bonne_reponse": item["correctAnswer"],
                "categorie": item.get("category", "—"),
                "difficulte": item.get("difficulty", "—"),
            })
        return questions
    except Exception as e:
        st.error(f"Erreur API : {e}")
        return []

# Session state pour navigation
defaults = {
    "questions": [],
    "index": 0,
    "quiz_lance": False,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

st.set_page_config(page_title="Quiz Simple Kafka", page_icon="🧠", layout="centered")
st.title("Quiz Simple — Envoi Kafka")

with st.sidebar:
    utilisateur = st.text_input("Votre nom", value="Joueur1")
    nb_questions = st.slider("Nombre de questions", 1, 10, 3)
    cat_label = st.selectbox("Catégorie", list(CATEGORIES.keys()))
    diff_label = st.selectbox("Difficulté", list(DIFFICULTES.keys()))
    cat_value = CATEGORIES[cat_label]
    diff_value = DIFFICULTES[diff_label]
    if st.button("Lancer le Quiz", type="primary"):
        qs = fetch_questions(nb_questions, diff_value, cat_value)
        if qs:
            st.session_state.questions = qs
            st.session_state.index = 0
            st.session_state.quiz_lance = True
            st.rerun()

questions = st.session_state.questions
index = st.session_state.index

if not st.session_state.quiz_lance:
    st.info("Configure et lance le quiz depuis la barre latérale.")
elif index < len(questions):
    q = questions[index]
    # Progression visuelle
    st.progress(index / len(questions))
    # Indicateur de progression
    st.markdown(
        f"**Progression :** Question **{index + 1}** sur **{len(questions)}** "
        f"— Il reste **{len(questions) - index - 1}** question(s)"
    )
    st.divider()
    st.subheader(q["question"])
    st.caption(f"Difficulté : **{q['difficulte']}** | Catégorie : **{q['categorie']}**")
    reponse_choisie = st.radio("Choisissez votre réponse :", q["choix"], key=f"radio_{index}", index=None)
    if st.button("Valider", disabled=(reponse_choisie is None)):
        producer = get_producer()
        if not producer:
            st.stop()
        message = {
            "timestamp": datetime.now().isoformat(),
            "utilisateur": utilisateur,
            "question_id": q["id"],
            "question": q["question"],
            "categorie": q["categorie"],
            "difficulte": q["difficulte"],
            "reponse_choisie": reponse_choisie,
            "bonne_reponse": q["bonne_reponse"]
        }
        producer.send(KAFKA_TOPIC, key=utilisateur, value=message)
       
        producer.flush()
        st.success("Message envoyé à Kafka !")
        st.json(message)
        st.session_state.index += 1
        st.rerun()
else:
    st.balloons()
    st.success("Quiz terminé ! Relance pour rejouer.")
    if st.button("Nouveau Quiz", type="primary"):
        for k in defaults:
            st.session_state[k] = defaults[k]
        st.rerun()
