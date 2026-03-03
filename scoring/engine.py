"""
M&A Radar Maroc — Scoring Engine (Gemini API - Batch Mode)
Au lieu d'appeler Gemini 43 fois, on envoie tout en 1 seul appel.
43 appels → 1 appel = quota quasi illimité.
"""

import os
import json
import requests
from loguru import logger

GEMINI_KEY = os.getenv("GEMINI_API_KEY", "COLLE-TA-CLÉ-ICI")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

# ─── FILTRE RAPIDE (avant IA) ─────────────────────────────────────────────
SIGNAL_FORT = [
    "fusion", "acquisition", "rachat", "cession", "apport",
    "augmentation de capital", "levée de fonds", "prise de participation",
    "transmission", "succession", "dissolution", "liquidation",
    "concentration", "offre publique", "introduction en bourse",
    "scission", "absorption", "restructuration", "désinvestissement",
    "partenariat stratégique", "entrée au capital", "cède", "vend",
]

BRUIT = [
    "football", "sport", "match", "joueur", "équipe", "tournoi",
    "champion", "goal", "ligue", "coupe", "mondial", "transfert sportif",
    "météo", "musique", "cinéma", "people", "célébrité",
    "élection", "parti", "discours", "recette", "cuisine", "voyage",
    "retraite sportive", "retraite internationale",
]

CONTEXTE_MAROC = [
    "maroc", "marocain", "casablanca", "rabat", "tanger", "fès",
    "agadir", "marrakech", "sa", "sarl", "mad", "dirham",
    "ompic", "ammc", "bulletin officiel", "conseil de la concurrence",
]


def filtrer_signal(texte: str) -> bool:
    """Filtre rapide — élimine le bruit évident avant d'appeler l'IA."""
    if not texte or len(texte) < 20:
        return False
    t = texte.lower()
    # Exclure bruit
    if any(b in t for b in BRUIT):
        return False
    # Garder si signal M&A OU contexte marocain
    has_signal = any(s in t for s in SIGNAL_FORT)
    has_maroc  = any(m in t for m in CONTEXTE_MAROC)
    return has_signal or has_maroc


def appeler_gemini(prompt: str, max_tokens: int = 4000) -> str:
    """Appelle Gemini 2.0 Flash."""
    try:
        response = requests.post(
            f"{GEMINI_URL}?key={GEMINI_KEY}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": max_tokens,
                    "temperature": 0.2,
                }
            },
            timeout=60
        )
        if response.status_code != 200:
            logger.error(f"Gemini error {response.status_code}: {response.text[:300]}")
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
        """
        Score TOUS les signaux en UN SEUL appel Gemini.
        43 signaux = 1 appel au lieu de 43.
        """
        if not signaux:
            return []

        # Étape 1 — Filtre rapide
        filtres_avant = []
        for s in signaux:
            texte = f"{s.get('titre','')} {s.get('raw_text','')}"
            if filtrer_signal(texte):
                filtres_avant.append(s)
            else:
                logger.debug(f"   ⛔ Filtré: {texte[:60]}")

        logger.info(f"   🔍 {len(signaux)} signaux → {len(filtres_avant)} après filtre rapide")

        if not filtres_avant:
            logger.info("   ℹ️ Aucun signal pertinent après filtrage")
            return []

        # Étape 2 — Batch scoring en 1 seul appel Gemini
        resultats = self._batch_score(filtres_avant)

        # Étape 3 — Stats
        critiques  = sum(1 for r in resultats if r.get("niveau_alerte") == "CRITIQUE")
        vigilances = sum(1 for r in resultats if r.get("niveau_alerte") == "VIGILANCE")
        radar      = sum(1 for r in resultats if r.get("niveau_alerte") == "RADAR")
        logger.info(f"   ✅ Scoring terminé — 🔴 {critiques} critiques | 🟠 {vigilances} vigilances | 🟡 {radar} radar")

        return resultats

    def _batch_score(self, signaux: list) -> list:
        """Envoie tous les signaux en un seul prompt Gemini."""

        # Construire la liste numérotée pour Gemini
        liste_signaux = ""
        for i, s in enumerate(signaux):
            texte = f"{s.get('titre', '')} {s.get('raw_text', '')}".strip()[:300]
            liste_signaux += f"\n[{i}] Source:{s.get('source','N/A')} | {texte}"

        prompt = f"""Tu es un banquier M&A senior spécialisé sur le marché marocain (PME, family businesses).

Analyse ces {len(signaux)} signaux et retourne UNIQUEMENT un tableau JSON valide.
Pas de texte avant, pas de texte après, pas de backticks.

SIGNAUX À ANALYSER:
{liste_signaux}

Retourne ce tableau JSON avec un objet par signal:
[
  {{
    "index": 0,
    "pertinent_ma": true,
    "score_final": 75,
    "niveau_alerte": "CRITIQUE",
    "type_deal_probable": "acquisition",
    "entreprise": "Nom SA",
    "secteur": "Distribution",
    "signaux_identifies": ["transmission_succession", "besoin_cash_bfr"],
    "recommandation": "Contacter le fondateur cette semaine."
  }},
  ...
]

Règles de scoring:
- 75-100 = CRITIQUE : deal imminent, action urgente
- 50-74  = VIGILANCE : signal fort, surveiller
- 30-49  = RADAR : signal modéré
- 0-29   = FAIBLE : bruit, pas pertinent

Types de signaux: transmission_succession, acquereur_actif_secteur, desinvestissement_activite, besoin_cash_bfr, gearing_eleve, changement_direction, expansion_geographique, consolidation_sectorielle, investissements_recents, recrutement_profil_ma

Si un signal n'est pas lié au M&A marocain → pertinent_ma: false, score_final: 0, niveau_alerte: "FAIBLE"
"""

        reponse = appeler_gemini(prompt, max_tokens=4000)

        if not reponse:
            logger.warning("   ⚠️ Gemini n'a pas répondu — signaux non scorés")
            return []

        try:
            # Nettoyer le JSON
            clean = reponse.strip()
            if "```" in clean:
                parts = clean.split("```")
                for p in parts:
                    if "[" in p and "{" in p:
                        clean = p.replace("json", "").strip()
                        break

            scores_ia = json.loads(clean)

            # Fusionner avec les données originales
            resultats = []
            for score in scores_ia:
                idx = score.get("index", -1)
                if idx < 0 or idx >= len(signaux):
                    continue

                if not score.get("pertinent_ma", True):
                    continue

                niveau = score.get("niveau_alerte", "FAIBLE")
                if niveau == "FAIBLE" or score.get("score_final", 0) < self.SEUIL_RADAR:
                    continue

                signal_original = signaux[idx]
                resultats.append({
                    **signal_original,
                    "score_final":        score.get("score_final", 0),
                    "score_ia":           score.get("score_final", 0),
                    "niveau_alerte":      niveau,
                    "type_deal_probable": score.get("type_deal_probable", "inconnu"),
                    "entreprise":         score.get("entreprise") or signal_original.get("entreprise"),
                    "secteur":            score.get("secteur", signal_original.get("secteur", "N/A")),
                    "signaux_identifies": score.get("signaux_identifies", []),
                    "recommandation":     score.get("recommandation", ""),
                })

            logger.info(f"   📊 {len(resultats)}/{len(signaux)} signaux retenus après scoring IA")
            return resultats

        except json.JSONDecodeError as e:
            logger.error(f"   ❌ JSON invalide de Gemini: {e}")
            logger.debug(f"   Réponse brute: {reponse[:300]}")
            return []

    def generer_memo(self, signal: dict) -> str:
        """Génère un mémo d'origination — 1 seul appel Gemini."""
        prompt = f"""Tu es un banquier M&A senior. Rédige un mémo d'origination professionnel et concis.

Entreprise : {signal.get('entreprise', 'N/A')}
Secteur    : {signal.get('secteur', 'N/A')}
Score M&A  : {signal.get('score_final', 0)}/100
Signaux    : {', '.join(signal.get('signaux_identifies', []))}
Source     : {signal.get('source', 'N/A')}
Info brute : {signal.get('raw_text', '')[:400]}

Structure (max 200 mots):
1. SITUATION — contexte de l'entreprise
2. SIGNAL DÉTECTÉ — ce qui a été observé
3. THÈSE D'OPÉRATION — type de deal probable et logique stratégique
4. PROCHAINE ÉTAPE — action concrète cette semaine

Ton: professionnel, direct, actionnable."""

        memo = appeler_gemini(prompt, max_tokens=600)
        return memo or f"Mémo non disponible — Signal via {signal.get('source','N/A')}"
