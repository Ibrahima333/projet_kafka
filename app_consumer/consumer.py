import streamlit as st
from kafka import KafkaConsumer
import json
import os
import time
from collections import defaultdict

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "d7dr83mgq0q78n6tjdvg.any.eu-west-2.mpx.prd.cloud.redpanda.com:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "quiz-reponses")

KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "kafka")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "uXoYCCvqPLeD8ZOq7jQFUDawQaJwaT")

st.set_page_config(page_title="Résultats Quiz Kafka", page_icon="📊", layout="centered")
st.title("Résultats en direct — Quiz Kafka")

st.info("Affichage en direct des réponses reçues via Kafka. Ouvre cette page pendant que le quiz est en cours.")

@st.cache_resource(show_spinner=False)
def get_consumer():
	try:
		return KafkaConsumer(
			KAFKA_TOPIC,
			bootstrap_servers=[KAFKA_BROKER],
			security_protocol="SASL_SSL",
			sasl_mechanism="SCRAM-SHA-256",
			sasl_plain_username=KAFKA_USERNAME,
			sasl_plain_password=KAFKA_PASSWORD,
			value_deserializer=lambda v: json.loads(v.decode("utf-8")),
			key_deserializer=lambda k: k.decode("utf-8") if k else None,
			# Comportement
			auto_offset_reset="latest",
			enable_auto_commit=True,
			group_id="streamlit-result-viewer",
			session_timeout_ms=120000,
			request_timeout_ms=180000,
			consumer_timeout_ms=60000,   # délai augmenté pour Render
		)
	except Exception as e:
		st.error(f"Kafka non connecté : {e}")
		return None

consumer = get_consumer()

if not consumer:
	st.stop()



score_placeholder = st.empty()
progress_placeholder = st.empty()
scores = defaultdict(int)
total_questions = defaultdict(int)
messages = []

while True:
	for msg in consumer.poll(timeout_ms=8000, max_records=10).values():
		for record in msg:
			m = record.value
			messages.append(m)
			user = m.get("utilisateur", "Inconnu")
			if m.get("reponse_choisie") == m.get("bonne_reponse"):
				scores[user] += 1
			total_questions[user] += 1
	if total_questions:
		with score_placeholder.container():
			st.subheader("Scores en direct :")
			for user, score in scores.items():
				st.write(f"**{user}** : {score} / {total_questions[user]}")
		with progress_placeholder.container():
			st.subheader("Progression :")
			for user, total in total_questions.items():
				user_score = scores[user]
				st.progress(user_score / total if total else 0, text=f"{user} : {user_score} / {total}")
	time.sleep(2)
