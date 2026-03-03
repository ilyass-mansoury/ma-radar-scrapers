"""
M&A Radar Maroc — Scoring Engine (Gemini API)
Gratuit : 1500 requêtes/jour, 15/minute
"""

import os
import json
import requests
from datetime import datetime
from loguru import logger

GEMINI_KEY = os.getenv("GEMINI_API_KEY", "COLLE-TA-CLÉ-ICI")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

# ─── FILTRE M&A STRICT ────────────────────────────────────────────────────
# Mots-clés qui CONFIRMENT un signal M&A marocain
SIGNAL_FORT = [
    "fusion", "acquisition", "rachat", "cession", "apport",
    "augmentation de capital", "levée de fonds", "prise de participation",
    "transmission", "succession", "dissolution", "liquidation",
    "concentration", "offre publique", "introduction en bourse",
    "scission", "absorption", "restructuration", "désinvestissement",
]

# Mots-clés qui EXCLUENT un signal (bruit)
BRUIT = [
    "football", "sport", "match", "joueur", "équipe", "tournoi",
    "champion", "goal", "ligue", "coupe", "mondial", "transfert sportif",
    "météo", "culture", "musique", "cinéma", "people", "célébrité",
    "politique", "élection", "parti", "ministre", "discours",
    "recette", "cuisine", "voyage", "tourisme",
]

# Mots-clés contexte Maroc obligatoire
CONTEXTE_MAROC = [
    "maroc", "marocain", "casablanca", "rabat", "tanger", "fès",
    "agadir", "marrakech", "sa", "sarl", "sas", "mad", "dirham",
    "ompic", "ammc", "bourse de casablanca", "bulletin officiel",
    "conseil de la concurrence",
]


def filtrer_signal(texte: str) -> tuple[bool, str]:
    """
    Filtre strict avant d'envoyer à l'IA.
    Retourne (pertinent: bool, raison: str)
    """
    if not texte or len(texte) < 20:
        return False, "texte trop court"

    texte_lower = texte.lower()

    # Exclure le bruit évident
    for mot in BRUIT:
        if mot in texte_lower:
            return False, f"bruit détecté: {mot}"

    # Vérifier présence d'un signal M&A
    has_signal = any(s in texte_lower for s in SIGNAL_FORT)

    # Vérifier contexte marocain
    has_maroc = any(m in texte_lower for m in CONTEXTE_MAROC)

    if not has_signal and not has_maroc:
        return False, "pas de signal M&A ni contexte marocain"

    return True, "signal pertinent"


def appeler_gemini(prompt: str, max_tokens: int = 800) -> str:
    """Appelle Gemini 1.5 Flash."""
    try:
        response = requests.post(
            f"{GEMINI_URL}?key={GEMINI_KEY}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": max_tokens,
                    "temperature": 0.3,
                }
            },
            timeout=30
        )

        if response.status_code != 200:
            logger.error(f"Gemini API error {response.status_code}: {response.text[:200]}")
            return ""

        data = response.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]

    except Exception as e:
        logger.error(f"Gemini error: {e}")
        return ""


class ScoringEngine:

    SEUIL_CRITIQUE  = 75
    SEUIL_VIGILANCE = 50
    SEUIL_RADAR     = 30

    def analyser_batch(self, signaux: list) -> list:
        """Score un batch de signaux avec filtre strict + Gemini."""
        resultats = []
        filtres   = 0
        scores    = 0

        logger.info(f"   🔍 Filtrage de {len(signaux)} signaux...")

        for signal in signaux:
            texte = f"{signal.get('titre','')} {signal.get('raw_text','')}"

            # Filtre rapide avant IA
            pertinent, raison = filtrer_signal(texte)
            if not pertinent:
                logger.debug(f"   ⛔ Filtré — {raison}: {texte[:60]}")
                filtres += 1
                continue

            # Score IA via Gemini
            scored = self._scorer_signal(signal)
            if scored.get("score_final", 0) >= self.SEUIL_RADAR:
                resultats.append(scored)
                scores += 1

        logger.info(f"   📊 {filtres} signaux filtrés | {scores} opportunités retenues")
        return resultats

    def _scorer_signal(self, signal: dict) -> dict:
        """Score un signal individuel avec Gemini."""
        texte = f"{signal.get('titre', '')} {signal.get('raw_text', '')}"

        prompt = f"""Tu es un banquier M&A senior spécialisé sur le marché marocain (PME, family businesses).

Analyse ce signal et retourne UNIQUEMENT un JSON valide, sans texte avant ou après :

Signal : {texte[:600]}
Source : {signal.get('source', 'N/A')}

JSON attendu :
{{
  "pertinent_ma": true/false,
  "score_final": 0-100,
  "niveau_alerte": "CRITIQUE|VIGILANCE|RADAR|FAIBLE",
  "type_deal_probable": "acquisition|cession|levee_fonds|pre_ipo|transmission|restructuring|inconnu",
  "entreprise": "nom ou null",
  "secteur": "secteur ou N/A",
  "signaux_identifies": ["signal1", "signal2"],
  "recommandation": "action concrète en 1 phrase"
}}

Règles scoring :
- 80-100 : deal imminent, action urgente
- 60-79  : signal fort, surveillance active
- 40-59  : signal modéré, à surveiller
- 0-39   : bruit, ignorer

Si le signal n'est pas lié au M&A marocain, retourne pertinent_ma: false et score_final: 0."""

        reponse = appeler_gemini(prompt)

        if not reponse:
            return {**signal, "score_final": 0, "niveau_alerte": "FAIBLE"}

        try:
            # Nettoyer le JSON (Gemini ajoute parfois des backticks)
            clean = reponse.strip()
            if "```" in clean:
                clean = clean.split("```")[1]
                if clean.startswith("json"):
                    clean = clean[4:]
            clean = clean.strip()

            result = json.loads(clean)

            # Si Gemini dit que c'est pas pertinent → score 0
            if not result.get("pertinent_ma", True):
                return {**signal, "score_final": 0, "niveau_alerte": "FAIBLE"}

            score = result.get("score_final", 0)
            niveau = (
                "CRITIQUE"  if score >= self.SEUIL_CRITIQUE  else
                "VIGILANCE" if score >= self.SEUIL_VIGILANCE else
                "RADAR"     if score >= self.SEUIL_RADAR     else
                "FAIBLE"
            )

            return {
                **signal,
                "score_final":        score,
                "score_ia":           score,
                "niveau_alerte":      niveau,
                "type_deal_probable": result.get("type_deal_probable", "inconnu"),
                "entreprise":         result.get("entreprise") or signal.get("entreprise"),
                "secteur":            result.get("secteur", signal.get("secteur", "N/A")),
                "signaux_identifies": result.get("signaux_identifies", []),
                "recommandation":     result.get("recommandation", ""),
            }

        except json.JSONDecodeError as e:
            logger.warning(f"   ⚠️ JSON invalide de Gemini: {e} — {reponse[:100]}")
            return {**signal, "score_final": 0, "niveau_alerte": "FAIBLE"}

    def generer_memo(self, signal: dict) -> str:
        """Génère un mémo d'origination complet avec Gemini."""
        prompt = f"""Tu es un banquier M&A senior. Rédige un mémo d'origination professionnel et concis.

Entreprise : {signal.get('entreprise', 'N/A')}
Secteur    : {signal.get('secteur', 'N/A')}
Score M&A  : {signal.get('score_final', 0)}/100
Signaux    : {', '.join(signal.get('signaux_identifies', []))}
Source     : {signal.get('source', 'N/A')}
Info brute : {signal.get('raw_text', '')[:400]}

Structure du mémo (max 250 mots) :
1. SITUATION — contexte de l'entreprise
2. SIGNAL — ce qui a été détecté
3. THÈSE D'OPÉRATION — type de deal probable et logique
4. PROCHAINE ÉTAPE — action concrète cette semaine

Ton professionnel, direct, actionnable."""

        memo = appeler_gemini(prompt, max_tokens=500)
        return memo or f"Mémo non disponible — Signal détecté via {signal.get('source','N/A')}"
