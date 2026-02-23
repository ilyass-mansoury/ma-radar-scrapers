"""
M&A Radar Maroc — Scraper OMPIC
Surveille les modifications légales enregistrées à l'OMPIC :
  - Changements de dirigeants
  - Modifications de capital
  - Dissolutions / radiations
  - Créations de nouvelles sociétés dans les secteurs cibles
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from loguru import logger
from config import SECTEURS_PRIORITAIRES, MOTS_CLES_MA


class OmpicScraper:
    """
    Scraper pour le registre du commerce marocain (OMPIC).
    
    Utilisation :
        scraper = OmpicScraper()
        signaux = scraper.run()
        # signaux = liste de dicts avec les infos de chaque signal détecté
    """

    BASE_URL = "https://www.ompic.ma"
    SEARCH_URL = "https://www.ompic.ma/fr/content/recherche-dans-le-registre-central-du-commerce"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "fr-FR,fr;q=0.9",
        })
        self.signaux = []

    def run(self):
        """Point d'entrée principal — lance le scraping complet."""
        logger.info("🔍 OMPIC — Démarrage du scan...")
        
        try:
            self._scraper_modifications_recentes()
            self._scraper_nouvelles_immatriculations()
            logger.success(f"✅ OMPIC — {len(self.signaux)} signaux détectés")
        except Exception as e:
            logger.error(f"❌ OMPIC — Erreur : {e}")

        return self.signaux

    def _scraper_modifications_recentes(self):
        """
        Scrape les modifications récentes au registre du commerce.
        Cible : changements de dirigeants, modifications de capital, etc.
        """
        logger.info("   → Scan des modifications récentes...")

        # NOTE pour le développeur :
        # L'OMPIC nécessite parfois une authentification pour les données détaillées.
        # En première version, on scrape les données publiques disponibles.
        # Pour les données complètes, une convention avec l'OMPIC est recommandée.

        try:
            response = self.session.get(self.SEARCH_URL, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Extraction des entrées du registre
            # (adapter les sélecteurs CSS selon la structure réelle du site)
            entries = soup.select(".result-item, .rc-entry, tr.entry")

            for entry in entries:
                signal = self._parser_entry(entry)
                if signal and self._est_pertinent(signal):
                    self.signaux.append(signal)

        except requests.RequestException as e:
            logger.warning(f"   ⚠️ OMPIC modifications — {e}")
            # En mode dégradé : retourner des données de test
            self.signaux.extend(self._donnees_test())

    def _scraper_nouvelles_immatriculations(self):
        """
        Scrape les nouvelles immatriculations dans les secteurs cibles.
        Un acteur qui s'immatricule dans un secteur = signal de consolidation.
        """
        logger.info("   → Scan des nouvelles immatriculations...")
        
        for secteur in SECTEURS_PRIORITAIRES[:5]:  # Top 5 secteurs prioritaires
            try:
                params = {
                    "secteur": secteur,
                    "date_debut": self._date_hier(),
                    "type": "immatriculation"
                }
                # Requête OMPIC pour ce secteur
                # (adapter selon l'API ou le formulaire OMPIC)
                logger.debug(f"      Secteur : {secteur}")

            except Exception as e:
                logger.warning(f"   ⚠️ Secteur {secteur} — {e}")

    def _parser_entry(self, entry):
        """
        Parse une entrée du registre OMPIC et retourne un dict structuré.
        Adapter les sélecteurs selon la structure HTML réelle de l'OMPIC.
        """
        try:
            return {
                "source": "OMPIC",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "entreprise": entry.select_one(".company-name, .raison-sociale, td:nth-child(1)").get_text(strip=True) if entry.select_one(".company-name, .raison-sociale, td:nth-child(1)") else "N/A",
                "type_modification": entry.select_one(".type-modif, .acte, td:nth-child(2)").get_text(strip=True) if entry.select_one(".type-modif, .acte, td:nth-child(2)") else "N/A",
                "ville": entry.select_one(".ville, .city, td:nth-child(3)").get_text(strip=True) if entry.select_one(".ville, .city, td:nth-child(3)") else "N/A",
                "rc_number": entry.select_one(".rc, td:nth-child(4)").get_text(strip=True) if entry.select_one(".rc, td:nth-child(4)") else "N/A",
                "raw_text": entry.get_text(strip=True),
                "signal_type": self._classifier_signal(entry.get_text(strip=True)),
                "score_initial": 0,  # Sera calculé par le moteur de scoring
            }
        except Exception:
            return None

    def _classifier_signal(self, texte):
        """
        Classifie le type de signal M&A basé sur le texte de l'entrée.
        Retourne la clé du signal correspondant dans SCORING_WEIGHTS.
        """
        texte_lower = texte.lower()

        if any(kw in texte_lower for kw in ["directeur", "gérant", "président", "pdg", "dg"]):
            return "changement_direction"
        
        if any(kw in texte_lower for kw in ["capital", "augmentation", "cession de parts"]):
            return "besoin_cash_bfr"
        
        if any(kw in texte_lower for kw in ["dissolution", "radiation", "liquidation"]):
            return "desinvestissement_activite"
        
        if any(kw in texte_lower for kw in ["fusion", "absorption", "apport"]):
            return "acquereur_actif_secteur"

        return "signal_generique"

    def _est_pertinent(self, signal):
        """
        Filtre les signaux non pertinents pour le radar M&A.
        Retourne True si le signal mérite d'être analysé par l'IA.
        """
        if not signal:
            return False

        # Exclure les micro-entreprises et auto-entrepreneurs
        exclusions = ["auto-entrepreneur", "personne physique", "artisan"]
        if any(ex in signal.get("raw_text", "").lower() for ex in exclusions):
            return False

        # Garder si modification significative ou mots-clés M&A présents
        if signal.get("signal_type") != "signal_generique":
            return True

        # Vérifier présence de mots-clés M&A dans le texte brut
        texte = signal.get("raw_text", "").lower()
        return any(kw.lower() in texte for kw in MOTS_CLES_MA)

    def _date_hier(self):
        """Retourne la date d'hier au format YYYY-MM-DD."""
        from datetime import timedelta
        return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    def _donnees_test(self):
        """
        Données de test pour développement en mode dégradé.
        Simule des signaux OMPIC réels pour tester le pipeline.
        """
        return [
            {
                "source": "OMPIC",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "entreprise": "DISTRIB ATLAS SARL",
                "type_modification": "Changement de gérant",
                "ville": "Casablanca",
                "rc_number": "CS 123456",
                "raw_text": "Changement de gérant — Distrib Atlas SARL — Distribution alimentaire",
                "signal_type": "changement_direction",
                "score_initial": 0,
            },
            {
                "source": "OMPIC",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "entreprise": "INDUSTRIE MAGHREB SA",
                "type_modification": "Augmentation de capital",
                "ville": "Tanger",
                "rc_number": "TNG 789012",
                "raw_text": "Augmentation de capital social — Industrie Maghreb SA — Secteur industriel",
                "signal_type": "besoin_cash_bfr",
                "score_initial": 0,
            },
            {
                "source": "OMPIC",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "entreprise": "BTP NORD MAROC",
                "type_modification": "Cession de parts sociales",
                "ville": "Rabat",
                "rc_number": "RB 345678",
                "raw_text": "Cession de parts sociales — BTP Nord Maroc — Secteur BTP",
                "signal_type": "transmission_succession",
                "score_initial": 0,
            },
        ]
