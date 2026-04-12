import streamlit as st
from kafka import KafkaConsumer, TopicPartition
import json, os, time
from collections import defaultdict

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "d7dr83mgq0q78n6tjdvg.any.eu-west-2.mpx.prd.cloud.redpanda.com:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC", "quiz-reponses")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "kafka")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "uXoYCCvqPLeD8ZOq7jQFUDawQaJwaT")

st.set_page_config(page_title="Résultats Quiz", page_icon="📊", layout="centered")
st.title("Résultats en direct — Quiz Kafka")

# Initialiser la session
if "scores" not in st.session_state:
    st.session_state.scores = defaultdict(int)
if "totals" not in st.session_state:
    st.session_state.totals = defaultdict(int)

def get_consumer():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BROKER],
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            enable_auto_commit=False,
            session_timeout_ms=30000,
            request_timeout_ms=40000,
        )
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        consumer.assign(tps)
        consumer.seek_to_end(*tps)  # ← seulement les nouveaux messages
        return consumer
    except Exception as e:
        st.error(f"Kafka non connecté : {e}")
        return None

# Poll une seule fois par rerun
consumer = get_consumer()
if consumer:
    polled = consumer.poll(timeout_ms=3000, max_records=50)
    for msgs in polled.values():
        for record in msgs:
            m = record.value
            user = m.get("utilisateur", "Inconnu")
            st.session_state.totals[user] += 1
            if m.get("reponse_choisie") == m.get("bonne_reponse"):
                st.session_state.scores[user] += 1
    consumer.close()

# Affichage
if st.session_state.totals:
    st.subheader("Scores en direct :")
    for user, total in st.session_state.totals.items():
        score = st.session_state.scores[user]
        st.write(f"**{user}** : {score} / {total}")
        st.progress(score / total if total else 0, text=f"{user} : {score} / {total}")
else:
    st.info("En attente de réponses...")

# Rafraîchir toutes les 3 secondes
time.sleep(3)
st.rerun()