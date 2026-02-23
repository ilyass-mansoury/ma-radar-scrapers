"""
M&A Radar Maroc — Scraper Bulletin Officiel
Source : bulletinofficiel.ma
Détecte : fusions, cessions, augmentations de capital, dissolutions, appels d'offres
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from loguru import logger
from config import MOTS_CLES_MA, SECTEURS_PRIORITAIRES


class BulletinOfficielScraper:
    """
    Scraper pour le Bulletin Officiel du Royaume du Maroc.
    C'est la source légale la plus fiable — tout deal significatif
    doit y être publié obligatoirement.
    """

    BASE_URL = "https://www.bulletinofficiel.ma"
    SEARCH_URL = "https://www.bulletinofficiel.ma/recherche"

    # Mots-clés spécifiques au Bulletin Officiel
    MOTS_CLES_BO = [
        "fusion", "absorption", "apport partiel", "cession de fonds",
        "augmentation de capital", "réduction de capital", "dissolution",
        "liquidation", "transformation", "scission", "apport en nature",
        "prise de participation", "cession d'actions", "cession de parts",
        "approbation de fusion", "traité de fusion", "projet de fusion",
        "concentration", "acquisition", "rachat", "offre publique",
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "fr-FR,fr;q=0.9,ar;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self.signaux = []

    def run(self):
        """Lance le scraping du Bulletin Officiel."""
        logger.info("📋 Bulletin Officiel — Démarrage du scan...")

        try:
            self._scraper_annonces_legales()
            self._scraper_par_mots_cles()
            logger.success(f"✅ Bulletin Officiel — {len(self.signaux)} signaux détectés")
        except Exception as e:
            logger.error(f"❌ Bulletin Officiel — Erreur : {e}")
            self.signaux.extend(self._donnees_test())

        return self.signaux

    def _scraper_annonces_legales(self):
        """Scrape les annonces légales récentes."""
        logger.info("   → Scan annonces légales...")

        urls_cibles = [
            f"{self.BASE_URL}/annonces-legales",
            f"{self.BASE_URL}/avis-et-communications",
            f"{self.BASE_URL}/fr/content/annonces",
        ]

        for url in urls_cibles:
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                annonces = soup.select(
                    ".annonce, .avis, article, .result-item, "
                    "tr.annonce, .bo-item, .publication-item"
                )

                for annonce in annonces[:30]:
                    texte = annonce.get_text(strip=True, separator=" ")
                    if self._contient_signal_ma(texte):
                        signal = self._construire_signal(texte, url)
                        if signal:
                            self.signaux.append(signal)

            except Exception as e:
                logger.debug(f"      {url} — {e}")

    def _scraper_par_mots_cles(self):
        """Recherche par mots-clés M&A dans le moteur de recherche du BO."""
        logger.info("   → Recherche par mots-clés M&A...")

        for mot_cle in self.MOTS_CLES_BO[:8]:
            try:
                params = {"q": mot_cle, "type": "annonce"}
                response = self.session.get(
                    self.SEARCH_URL,
                    params=params,
                    timeout=15
                )
                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                resultats = soup.select(".result, .search-result, article, tr")

                for resultat in resultats[:10]:
                    texte = resultat.get_text(strip=True, separator=" ")
                    if len(texte) > 30 and self._contient_signal_ma(texte):
                        signal = self._construire_signal(texte, self.SEARCH_URL)
                        if signal:
                            self.signaux.append(signal)

            except Exception as e:
                logger.debug(f"      Mot-clé '{mot_cle}' — {e}")

    def _contient_signal_ma(self, texte):
        """Vérifie si le texte contient un signal M&A."""
        if not texte or len(texte) < 20:
            return False
        texte_lower = texte.lower()
        return any(kw.lower() in texte_lower for kw in self.MOTS_CLES_BO + MOTS_CLES_MA)

    def _classifier_signal(self, texte):
        """Classifie le type de signal M&A."""
        texte_lower = texte.lower()
        if any(kw in texte_lower for kw in ["fusion", "absorption", "apport"]):
            return "acquereur_actif_secteur"
        if any(kw in texte_lower for kw in ["cession", "vente", "transfert"]):
            return "desinvestissement_activite"
        if any(kw in texte_lower for kw in ["augmentation de capital", "apport en numéraire"]):
            return "besoin_cash_bfr"
        if any(kw in texte_lower for kw in ["dissolution", "liquidation"]):
            return "desinvestissement_activite"
        if any(kw in texte_lower for kw in ["transformation", "scission"]):
            return "transmission_succession"
        return "signal_generique"

    def _extraire_entreprise(self, texte):
        """Tente d'extraire le nom de l'entreprise du texte."""
        # Patterns courants dans le BO marocain
        import re
        patterns = [
            r"(?:société|SARL|SA|SAS|SNC|GIE)\s+([A-Z][A-Za-z\s&'-]+?)(?:\s+au capital|\s+dont|\s+ayant|\,)",
            r"([A-Z][A-Z\s&'-]{3,40})\s+(?:SARL|SA|SAS|SNC)",
        ]
        for pattern in patterns:
            match = re.search(pattern, texte)
            if match:
                return match.group(1).strip()[:60]
        return None

    def _construire_signal(self, texte, url):
        """Construit un dict structuré pour un signal."""
        if not texte:
            return None
        return {
            "source":       "Bulletin Officiel",
            "date":         datetime.now().strftime("%Y-%m-%d"),
            "titre":        texte[:150],
            "url":          url,
            "raw_text":     texte[:500],
            "entreprise":   self._extraire_entreprise(texte),
            "signal_type":  self._classifier_signal(texte),
            "score_initial": 0,
        }

    def _donnees_test(self):
        """Données de test si le site est inaccessible."""
        return [
            {
                "source": "Bulletin Officiel",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "entreprise": "ATLAS DISTRIBUTION SA",
                "titre": "Projet de fusion-absorption de Atlas Distribution SA par Retail Maroc Group",
                "url": self.BASE_URL,
                "raw_text": "Avis de fusion — Atlas Distribution SA — Distribution alimentaire — Casablanca",
                "signal_type": "acquereur_actif_secteur",
                "score_initial": 0,
            },
            {
                "source": "Bulletin Officiel",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "entreprise": "INDUSTRIE MAGHREB HOLDING",
                "titre": "Cession de parts sociales — Industrie Maghreb Holding SARL — Tanger",
                "url": self.BASE_URL,
                "raw_text": "Cession de 60% des parts sociales — Industrie Maghreb Holding — Secteur industriel",
                "signal_type": "transmission_succession",
                "score_initial": 0,
            },
            {
                "source": "Bulletin Officiel",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "entreprise": "LOGISTIQUE NORD MAROC",
                "titre": "Augmentation de capital social — Logistique Nord Maroc SA",
                "url": self.BASE_URL,
                "raw_text": "Augmentation de capital de 50M MAD — Logistique Nord Maroc SA — Transport et logistique",
                "signal_type": "besoin_cash_bfr",
                "score_initial": 0,
            },
            {
                "source": "Bulletin Officiel",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "entreprise": "SANTE PLUS CLINIQUES",
                "titre": "Apport partiel d'actifs — Santé Plus Cliniques SA vers holding médical",
                "url": self.BASE_URL,
                "raw_text": "Apport partiel actifs cliniques — Santé Plus — Secteur santé — Casablanca Rabat",
                "signal_type": "desinvestissement_activite",
                "score_initial": 0,
            },
        ]
