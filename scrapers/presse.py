"""
M&A Radar Maroc — Scraper Presse Économique
Surveille en temps réel :
  - L'Économiste, Médias24, Challenge, Aujourd'hui le Maroc, LesEco
  - Détecte les articles contenant des signaux M&A
  - Extrait le nom de l'entreprise, le type de signal, et le texte brut
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from loguru import logger
from config import SOURCES_PRESSE, MOTS_CLES_MA, SECTEURS_PRIORITAIRES


class PresseEcoScraper:
    """
    Scraper multi-sources pour la presse économique marocaine.

    Utilisation :
        scraper = PresseEcoScraper()
        signaux = scraper.run()
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "fr-FR,fr;q=0.9",
        })
        self.signaux = []

    def run(self):
        """Lance le scraping de toutes les sources de presse."""
        logger.info("📰 Presse économique — Démarrage du scan...")

        # Chaque source a ses propres sélecteurs CSS
        sources_config = {
            "https://www.medias24.com":      self._scraper_medias24,
            "https://www.leconomiste.com":   self._scraper_leconomiste,
            "https://www.challenge.ma":      self._scraper_challenge,
            "https://www.leseco.ma":         self._scraper_leseco,
        }

        for url, scraper_fn in sources_config.items():
            try:
                scraper_fn(url)
            except Exception as e:
                logger.warning(f"   ⚠️ {url} — {e}")

        logger.success(f"✅ Presse — {len(self.signaux)} signaux détectés")
        return self.signaux

    # ─── SCRAPERS PAR SOURCE ──────────────────────────────────────────────────

    def _scraper_medias24(self, base_url):
        """Scraper spécifique pour Médias24."""
        logger.info("   → Scan Médias24...")
        
        sections_cibles = [
            f"{base_url}/economie",
            f"{base_url}/bourse",
            f"{base_url}/societe",
        ]

        for url in sections_cibles:
            try:
                soup = self._fetch(url)
                if not soup:
                    continue

                articles = soup.select("article, .article-item, .news-item, h2 a, h3 a")
                
                for article in articles[:20]:  # Top 20 articles récents
                    titre = article.get_text(strip=True) if article else ""
                    lien  = article.get("href", "") if article.name == "a" else ""
                    
                    if self._contient_signal_ma(titre):
                        signal = self._construire_signal(
                            source="Médias24",
                            titre=titre,
                            url=lien if lien.startswith("http") else base_url + lien,
                            texte_brut=titre
                        )
                        if signal:
                            self.signaux.append(signal)

            except Exception as e:
                logger.debug(f"      Section {url} — {e}")

    def _scraper_leconomiste(self, base_url):
        """Scraper spécifique pour L'Économiste."""
        logger.info("   → Scan L'Économiste...")
        
        try:
            soup = self._fetch(f"{base_url}/categorie/economie")
            if not soup:
                return

            articles = soup.select(".article-title, .titre, h2, h3")
            
            for article in articles[:20]:
                titre = article.get_text(strip=True)
                lien  = article.find("a")
                lien_href = lien.get("href", "") if lien else ""

                if self._contient_signal_ma(titre):
                    signal = self._construire_signal(
                        source="L'Économiste",
                        titre=titre,
                        url=lien_href if lien_href.startswith("http") else base_url + lien_href,
                        texte_brut=titre
                    )
                    if signal:
                        self.signaux.append(signal)

        except Exception as e:
            logger.debug(f"      L'Économiste — {e}")

    def _scraper_challenge(self, base_url):
        """Scraper spécifique pour Challenge."""
        logger.info("   → Scan Challenge...")
        
        try:
            soup = self._fetch(base_url)
            if not soup:
                return

            articles = soup.select("h2 a, h3 a, .post-title a, .entry-title a")
            
            for article in articles[:15]:
                titre = article.get_text(strip=True)
                lien  = article.get("href", "")

                if self._contient_signal_ma(titre):
                    signal = self._construire_signal(
                        source="Challenge",
                        titre=titre,
                        url=lien,
                        texte_brut=titre
                    )
                    if signal:
                        self.signaux.append(signal)

        except Exception as e:
            logger.debug(f"      Challenge — {e}")

    def _scraper_leseco(self, base_url):
        """Scraper spécifique pour LesEco."""
        logger.info("   → Scan LesEco...")
        
        try:
            soup = self._fetch(base_url)
            if not soup:
                return

            articles = soup.select("h2 a, h3 a, .article-link, .news-title a")
            
            for article in articles[:15]:
                titre = article.get_text(strip=True)
                lien  = article.get("href", "")

                if self._contient_signal_ma(titre):
                    signal = self._construire_signal(
                        source="LesEco",
                        titre=titre,
                        url=lien,
                        texte_brut=titre
                    )
                    if signal:
                        self.signaux.append(signal)

        except Exception as e:
            logger.debug(f"      LesEco — {e}")

    # ─── HELPERS ─────────────────────────────────────────────────────────────

    def _fetch(self, url, timeout=15):
        """Fetch une URL et retourne un objet BeautifulSoup."""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.debug(f"      Fetch failed {url}: {e}")
            return None

    def _contient_signal_ma(self, texte):
        """
        Vérifie si un titre d'article contient un signal M&A pertinent.
        Retourne True si au moins un mot-clé M&A est présent.
        """
        if not texte or len(texte) < 10:
            return False
        
        texte_lower = texte.lower()
        
        # Vérifier mots-clés M&A
        if any(kw.lower() in texte_lower for kw in MOTS_CLES_MA):
            return True
        
        # Vérifier secteurs prioritaires (signal sectoriel)
        if any(s.lower() in texte_lower for s in SECTEURS_PRIORITAIRES):
            # Présence secteur + certains indicateurs = signal
            indicateurs = ["croissance", "investissement", "expansion", 
                          "consolidation", "rachat", "partenariat", "capital"]
            return any(ind in texte_lower for ind in indicateurs)
        
        return False

    def _classifier_signal(self, texte):
        """Classifie le type de signal M&A."""
        texte_lower = texte.lower()
        
        if any(kw in texte_lower for kw in ["succession", "transmission", "héritier", "retraite"]):
            return "transmission_succession"
        if any(kw in texte_lower for kw in ["acquisition", "rachat", "fusion", "croissance externe"]):
            return "acquereur_actif_secteur"
        if any(kw in texte_lower for kw in ["cession", "vente", "désengagement", "cède"]):
            return "desinvestissement_activite"
        if any(kw in texte_lower for kw in ["capital", "levée", "financement", "investissement"]):
            return "besoin_cash_bfr"
        if any(kw in texte_lower for kw in ["directeur", "pdg", "dg", "dirigeant", "départ"]):
            return "changement_direction"
        if any(kw in texte_lower for kw in ["bourse", "ipo", "introduction", "cotation"]):
            return "besoin_cash_bfr"
        
        return "signal_generique"

    def _construire_signal(self, source, titre, url, texte_brut):
        """Construit un dict structuré pour un signal détecté."""
        if not titre:
            return None
        
        return {
            "source":       source,
            "date":         datetime.now().strftime("%Y-%m-%d"),
            "titre":        titre[:200],
            "url":          url,
            "raw_text":     texte_brut[:500],
            "signal_type":  self._classifier_signal(texte_brut),
            "score_initial": 0,  # Calculé par le moteur de scoring IA
            "entreprise":   None,  # Extrait par Claude dans l'étape suivante
        }
