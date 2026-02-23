"""
M&A Radar Maroc — Moteur de Scoring IA
Utilise Claude (Anthropic) pour analyser chaque signal et :
  1. Extraire l'entreprise concernée
  2. Classifier le type de deal probable
  3. Calculer un score M&A de 0 à 100
  4. Générer une recommandation d'action
"""

import anthropic
import json
from loguru import logger
from config import ANTHROPIC_API_KEY, SCORING_WEIGHTS, SEUIL_CRITIQUE, SEUIL_VIGILANCE


class ScoringEngine:
    """
    Moteur de scoring IA basé sur Claude.

    Utilisation :
        engine = ScoringEngine()
        resultat = engine.analyser(signal)
    """

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        self.model  = "claude-sonnet-4-20250514"

    def analyser(self, signal: dict) -> dict:
        """
        Analyse un signal et retourne un scoring complet.
        
        Args:
            signal: Dict contenant source, texte, type_signal, etc.
        
        Returns:
            Dict enrichi avec score, entreprise, type_deal, recommandation
        """
        logger.info(f"   🤖 Analyse IA — {signal.get('source', 'N/A')} — {signal.get('titre', signal.get('entreprise', 'N/A'))[:50]}")

        try:
            prompt = self._construire_prompt(signal)
            
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )

            texte_reponse = response.content[0].text
            resultat = self._parser_reponse(texte_reponse)
            
            # Enrichir le signal original avec l'analyse IA
            signal_enrichi = {**signal, **resultat}
            signal_enrichi["score_final"] = self._calculer_score_final(signal_enrichi)
            signal_enrichi["niveau_alerte"] = self._determiner_niveau(signal_enrichi["score_final"])
            
            logger.success(f"   ✅ Score : {signal_enrichi['score_final']}/100 — {signal_enrichi['niveau_alerte']}")
            return signal_enrichi

        except Exception as e:
            logger.error(f"   ❌ Erreur scoring IA : {e}")
            return {**signal, "score_final": 0, "niveau_alerte": "ERREUR", "erreur": str(e)}

    def analyser_batch(self, signaux: list) -> list:
        """
        Analyse une liste de signaux en batch.
        Retourne la liste enrichie et triée par score décroissant.
        """
        logger.info(f"🤖 Scoring IA — Analyse de {len(signaux)} signaux...")
        
        resultats = []
        for signal in signaux:
            resultat = self.analyser(signal)
            resultats.append(resultat)

        # Trier par score décroissant
        resultats.sort(key=lambda x: x.get("score_final", 0), reverse=True)
        
        # Stats
        critiques  = [r for r in resultats if r.get("niveau_alerte") == "CRITIQUE"]
        vigilances = [r for r in resultats if r.get("niveau_alerte") == "VIGILANCE"]
        logger.success(f"✅ Scoring terminé — {len(critiques)} critiques, {len(vigilances)} vigilances")
        
        return resultats

    # ─── PROMPT ENGINEERING ──────────────────────────────────────────────────

    def _construire_prompt(self, signal: dict) -> str:
        """
        Construit le prompt d'analyse pour Claude.
        C'est ici que ta thèse d'origination est encodée.
        """
        
        poids_str = "\n".join([
            f"  - {k}: {v} points" 
            for k, v in SCORING_WEIGHTS.items()
        ])

        return f"""Tu es un expert en M&A et origination de deals pour le marché marocain, 
spécialisé dans les PME et family businesses. Tu travailles pour une boutique M&A marocaine.

Ta mission : analyser ce signal de marché et évaluer son potentiel M&A.

═══ SIGNAL DÉTECTÉ ═══
Source : {signal.get('source', 'N/A')}
Date : {signal.get('date', 'N/A')}
Entreprise : {signal.get('entreprise', 'Non identifiée')}
Titre/Description : {signal.get('titre', signal.get('raw_text', 'N/A'))}
Type de signal détecté : {signal.get('signal_type', 'N/A')}

═══ GRILLE DE SCORING (ta thèse d'origination) ═══
{poids_str}

═══ PROFIL DE LA CIBLE IDÉALE ═══
- Société en pleine croissance, rentable
- Besoin de financement BFR ou vision stratégique à consolider
- PME ou family business
- Opérations cibles : Pre-IPO, ouverture de capital, partenaire stratégique, acquisition majoritaire
- Secteurs prioritaires : Distribution, Industrie, BTP (opportuniste sur tous secteurs)
- Couverture : tout le Maroc

═══ SIGNAUX D'URGENCE (action immédiate) ═══
1. Problème de transmission/succession familiale
2. Concurrent avec stratégie de croissance externe active dans le secteur
3. Désengagement d'activité non-core

═══ TA MISSION ═══
Analyse ce signal et réponds UNIQUEMENT avec un JSON valide (sans markdown, sans explication) :

{{
  "entreprise": "Nom exact de l'entreprise concernée ou null",
  "secteur": "Secteur d'activité estimé",
  "type_deal_probable": "acquisition | cession | levee_fonds | pre_ipo | restructuring | transmission | inconnu",
  "signaux_identifies": ["liste", "des", "signaux", "présents"],
  "score_ma": <nombre entre 0 et 100>,
  "urgence": "critique | fort | modere | faible",
  "fenetre_action": "Description de la fenêtre d'opportunité temporelle",
  "recommandation": "Action concrète recommandée en 1-2 phrases",
  "pertinent": true | false,
  "raison_non_pertinent": "Si pertinent=false, expliquer pourquoi"
}}"""

    def _parser_reponse(self, texte: str) -> dict:
        """Parse la réponse JSON de Claude."""
        try:
            # Nettoyer la réponse (supprimer les éventuels backticks)
            texte_clean = texte.strip()
            if texte_clean.startswith("```"):
                texte_clean = texte_clean.split("```")[1]
                if texte_clean.startswith("json"):
                    texte_clean = texte_clean[4:]
            
            data = json.loads(texte_clean)
            return {
                "entreprise":        data.get("entreprise"),
                "secteur":           data.get("secteur", "N/A"),
                "type_deal_probable":data.get("type_deal_probable", "inconnu"),
                "signaux_identifies":data.get("signaux_identifies", []),
                "score_ia":          data.get("score_ma", 0),
                "urgence_ia":        data.get("urgence", "faible"),
                "fenetre_action":    data.get("fenetre_action", "N/A"),
                "recommandation":    data.get("recommandation", ""),
                "pertinent":         data.get("pertinent", True),
                "raison_non_pertinent": data.get("raison_non_pertinent", ""),
            }
        except json.JSONDecodeError as e:
            logger.warning(f"   ⚠️ Parsing JSON échoué : {e}")
            return {
                "score_ia": 0,
                "pertinent": False,
                "recommandation": "Erreur de parsing — analyse manuelle requise",
                "raw_ia_response": texte[:200]
            }

    # ─── SCORING FINAL ────────────────────────────────────────────────────────

    def _calculer_score_final(self, signal: dict) -> int:
        """
        Calcule le score final en combinant :
        - Le score IA de Claude (70% du poids)
        - Les poids de la grille de signaux (30% du poids)
        
        Résultat : score entre 0 et 100
        """
        score_ia = signal.get("score_ia", 0)
        
        # Bonus basé sur les signaux identifiés dans la grille
        bonus_grille = 0
        signaux_ids = signal.get("signaux_identifies", [])
        
        for signal_id in signaux_ids:
            # Chercher une correspondance dans les poids
            for cle, poids in SCORING_WEIGHTS.items():
                if cle.replace("_", " ") in " ".join(signaux_ids).lower():
                    bonus_grille += poids
                    break
        
        # Normaliser le bonus (max 30 points)
        bonus_normalise = min(30, bonus_grille // 3)
        
        # Score final = 70% IA + 30% grille
        score_final = int(score_ia * 0.7 + bonus_normalise)
        
        # Bonus urgence
        urgence = signal.get("urgence_ia", "faible")
        if urgence == "critique":
            score_final = min(100, score_final + 10)
        elif urgence == "fort":
            score_final = min(100, score_final + 5)
        
        return min(100, max(0, score_final))

    def _determiner_niveau(self, score: int) -> str:
        """Détermine le niveau d'alerte selon le score."""
        if score >= SEUIL_CRITIQUE:
            return "CRITIQUE"
        elif score >= SEUIL_VIGILANCE:
            return "VIGILANCE"
        elif score >= 40:
            return "RADAR"
        else:
            return "FAIBLE"

    def generer_memo(self, signal: dict) -> str:
        """
        Génère un mémo d'origination complet pour un signal critique.
        Appelé automatiquement quand score >= SEUIL_CRITIQUE.
        """
        logger.info(f"📝 Génération mémo — {signal.get('entreprise', 'N/A')}")

        prompt = f"""Tu es un banquier d'affaires M&A senior au Maroc.
Génère un mémo d'origination professionnel et concis pour cette opportunité.

Signal détecté :
- Entreprise : {signal.get('entreprise', 'N/A')}
- Secteur : {signal.get('secteur', 'N/A')}
- Type de deal : {signal.get('type_deal_probable', 'N/A')}
- Score M&A : {signal.get('score_final', 0)}/100
- Signaux : {', '.join(signal.get('signaux_identifies', []))}
- Source : {signal.get('source', 'N/A')}
- Description : {signal.get('titre', signal.get('raw_text', 'N/A'))[:300]}

Rédige un mémo d'origination structuré avec :
1. Situation actuelle (2-3 phrases)
2. Thèse de deal (2-3 scénarios possibles)
3. Acquéreurs/investisseurs potentiels (3-4 noms)
4. Recommandation d'action (1 action concrète, délai précis)

Ton style : direct, factuel, orienté action. C'est pour usage interne d'un banquier d'affaires."""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"❌ Erreur génération mémo : {e}")
            return f"Erreur génération mémo : {e}"
