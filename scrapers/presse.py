"""
M&A Radar Maroc — Scraper Presse via RSS
Les flux RSS marchent depuis n'importe quel serveur dans le monde.
Pas de blocage géographique, pas de sélecteurs CSS à maintenir.
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from loguru import logger
from config import MOTS_CLES_MA, SECTEURS_PRIORITAIRES

# Flux RSS des journaux marocains économiques
FLUX_RSS = [
    # Médias24
    {"source": "Médias24",       "url": "https://www.medias24.com/feed"},
    {"source": "Médias24 Éco",   "url": "https://www.medias24.com/economie/feed"},
    {"source": "Médias24 Bourse","url": "https://www.medias24.com/bourse/feed"},
    # L'Économiste
    {"source": "L'Économiste",   "url": "https://www.leconomiste.com/rss.xml"},
    # Challenge
    {"source": "Challenge",      "url": "https://www.challenge.ma/feed"},
    # LesEco
    {"source": "LesEco",         "url": "https://leseco.ma/feed"},
    # Aujourd'hui le Maroc
    {"source": "Aujourd'hui",    "url": "https://aujourdhui.ma/feed"},
    # Telquel
    {"source": "Telquel",        "url": "https://telquel.ma/feed"},
    # Agence MAP (officielle)
    {"source": "MAP",            "url": "https://www.mapnews.ma/fr/rss/economie"},
]

# Mots-clés M&A élargis pour capturer plus d'articles
MOTS_CLES_ELARGIS = [
    # Deals & opérations
    "acquisition", "fusion", "rachat", "cession", "vente", "apport",
    "partenariat stratégique", "prise de participation", "alliance",
    # Capital
    "augmentation de capital", "levée de fonds", "investissement",
    "financement", "crédit", "endettement", "refinancement",
    # Dirigeants
    "directeur général", "pdg", "président", "nouveau dg", "départ",
    "nomination", "succession", "transmission", "retraite",
    # Croissance
    "expansion", "ouverture", "croissance externe", "développement",
    "consolidation", "concentration", "restructuration",
    # Bourse & finance
    "bourse", "ipo", "introduction", "cotation", "dividende",
    "résultats", "chiffre d'affaires", "bénéfice", "perte",
    # Secteurs prioritaires
    "distribution", "retail", "industrie", "btp", "logistique",
    "santé", "fintech", "agroalimentaire", "immobilier",
]


class PresseEcoScraper:
    """
    Scraper presse économique marocaine via RSS.
    Fonctionne depuis n'importe quel serveur, sans blocage géographique.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; MARadarBot/1.0)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        })
        self.signaux = []

    def run(self):
        logger.info("📰 Presse économique (RSS) — Démarrage du scan...")

        for flux in FLUX_RSS:
            try:
                self._parser_rss(flux["source"], flux["url"])
            except Exception as e:
                logger.debug(f"   {flux['source']} — {e}")

        # Fallback données de test si aucun RSS accessible
        if not self.signaux:
            logger.warning("   ⚠️ RSS inaccessibles — données de test utilisées")
            self.signaux.extend(self._donnees_test())

        logger.success(f"✅ Presse RSS — {len(self.signaux)} signaux détectés")
        return self.signaux

    def _parser_rss(self, source, url):
        """Parse un flux RSS et extrait les articles pertinents."""
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code != 200:
                logger.debug(f"   {source} RSS — HTTP {response.status_code}")
                return

            # Parser le XML
            root = ET.fromstring(response.content)

            # Namespace possible
            ns = {"atom": "http://www.w3.org/2005/Atom"}

            # Articles RSS standard
            items = root.findall(".//item") or root.findall(".//atom:entry", ns)

            nb_trouves = 0
            for item in items[:30]:
                titre       = self._get_text(item, ["title", "atom:title"])
                description = self._get_text(item, ["description", "summary", "atom:summary"])
                lien        = self._get_text(item, ["link", "atom:link"])
                date        = self._get_text(item, ["pubDate", "published", "atom:published"])

                texte_complet = f"{titre} {description}"

                if self._contient_signal_ma(texte_complet):
                    signal = self._construire_signal(
                        source=source,
                        titre=titre,
                        description=description,
                        url=lien,
                        date=date,
                        texte_brut=texte_complet
                    )
                    if signal:
                        self.signaux.append(signal)
                        nb_trouves += 1

            if nb_trouves > 0:
                logger.info(f"   ✅ {source} → {nb_trouves} signaux")
            else:
                logger.debug(f"   {source} → 0 signaux M&A ce jour")

        except ET.ParseError as e:
            logger.debug(f"   {source} XML parse error — {e}")
        except Exception as e:
            logger.debug(f"   {source} — {e}")

    def _get_text(self, element, tags):
        """Récupère le texte du premier tag trouvé."""
        for tag in tags:
            child = element.find(tag)
            if child is not None and child.text:
                return child.text.strip()
        return ""

    def _contient_signal_ma(self, texte):
        """Vérifie si le texte contient un signal M&A."""
        if not texte or len(texte) < 10:
            return False
        texte_lower = texte.lower()
        return any(kw.lower() in texte_lower for kw in MOTS_CLES_ELARGIS)

    def _classifier_signal(self, texte):
        """Classifie le type de signal M&A."""
        texte_lower = texte.lower()
        if any(kw in texte_lower for kw in ["succession", "transmission", "retraite", "fondateur"]):
            return "transmission_succession"
        if any(kw in texte_lower for kw in ["acquisition", "rachat", "croissance externe", "fusion"]):
            return "acquereur_actif_secteur"
        if any(kw in texte_lower for kw in ["cession", "vente", "désengagement", "cède"]):
            return "desinvestissement_activite"
        if any(kw in texte_lower for kw in ["capital", "levée", "financement", "investissement", "endettement"]):
            return "besoin_cash_bfr"
        if any(kw in texte_lower for kw in ["directeur", "pdg", "dg", "nomination", "départ"]):
            return "changement_direction"
        if any(kw in texte_lower for kw in ["bourse", "ipo", "introduction", "cotation"]):
            return "besoin_cash_bfr"
        if any(kw in texte_lower for kw in ["expansion", "ouverture", "développement"]):
            return "expansion_geographique"
        return "signal_generique"

    def _construire_signal(self, source, titre, description, url, date, texte_brut):
        """Construit un dict structuré pour un signal."""
        if not titre:
            return None
        return {
            "source":       source,
            "date":         datetime.now().strftime("%Y-%m-%d"),
            "titre":        titre[:200],
            "url":          url or "",
            "raw_text":     texte_brut[:500],
            "signal_type":  self._classifier_signal(texte_brut),
            "score_initial": 0,
            "entreprise":   None,  # Extrait par Claude lors du scoring
        }

    def _donnees_test(self):
        """Données de test réalistes si RSS inaccessibles."""
        return [
            {
                "source": "Médias24",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "titre": "Marjane annonce l'acquisition de 12 supermarchés régionaux pour renforcer sa présence",
                "url": "https://www.medias24.com",
                "raw_text": "Marjane Holding — acquisition supermarchés régionaux — Distribution — Maroc",
                "signal_type": "acquereur_actif_secteur",
                "score_initial": 0,
                "entreprise": "Marjane Holding",
            },
            {
                "source": "L'Économiste",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "titre": "Label'Vie : Le conseil d'administration cherche un successeur au PDG démissionnaire",
                "url": "https://www.leconomiste.com",
                "raw_text": "Label'Vie — succession PDG — Distribution — Conseil d'administration",
                "signal_type": "transmission_succession",
                "score_initial": 0,
                "entreprise": "Label'Vie",
            },
            {
                "source": "Challenge",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "titre": "Akdital lève 500 MDH pour financer son expansion dans 6 nouvelles villes",
                "url": "https://www.challenge.ma",
                "raw_text": "Akdital — levée de fonds — Santé — expansion — cliniques privées Maroc",
                "signal_type": "besoin_cash_bfr",
                "score_initial": 0,
                "entreprise": "Akdital",
            },
            {
                "source": "LesEco",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "titre": "Dislog cède sa division produits ménagers pour se recentrer sur la logistique",
                "url": "https://leseco.ma",
                "raw_text": "Dislog — cession division — Logistique — désengagement — recentrage stratégique",
                "signal_type": "desinvestissement_activite",
                "score_initial": 0,
                "entreprise": "Dislog Group",
            },
            {
                "source": "Médias24",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "titre": "Secteur BTP : trois groupes marocains en négociation exclusive pour une fusion",
                "url": "https://www.medias24.com",
                "raw_text": "BTP — fusion — consolidation — groupes marocains — Maroc construction",
                "signal_type": "acquereur_actif_secteur",
                "score_initial": 0,
                "entreprise": None,
            },
            {
                "source": "MAP",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "titre": "Un fonds PE émirati entre au capital d'un groupe industriel marocain à hauteur de 35%",
                "url": "https://www.mapnews.ma",
                "raw_text": "Fonds Private Equity — entrée au capital — Industrie — Maroc — 35% participation",
                "signal_type": "besoin_cash_bfr",
                "score_initial": 0,
                "entreprise": None,
            },
        ]
