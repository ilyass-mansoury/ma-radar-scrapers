"""
M&A Radar Maroc — Pipeline Principal
Sources : OMPIC + Presse + Bulletin Officiel + Conseil de la Concurrence
"""

import sys
import os
import schedule
import time
from datetime import datetime
from loguru import logger

from scrapers.ompic                import OmpicScraper
from scrapers.presse               import PresseEcoScraper
from scrapers.bulletin_officiel    import BulletinOfficielScraper
from scrapers.conseil_concurrence  import ConseilConcurrenceScraper
from scoring.engine                import ScoringEngine
from config                        import HEURE_SCAN_QUOTIDIEN
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | {message}", level="INFO")


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("⚠️ Variables Supabase manquantes")
        return None
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def sauvegarder_opportunite(supabase, signal):
    if not supabase:
        return
    try:
        data = {
            "entreprise":       signal.get("entreprise") or signal.get("titre", "N/A")[:50],
            "secteur":          signal.get("secteur", "N/A"),
            "score_final":      signal.get("score_final", 0),
            "niveau_alerte":    signal.get("niveau_alerte", "RADAR"),
            "type_deal":        signal.get("type_deal_probable", "inconnu"),
            "source":           signal.get("source", "N/A"),
            "signaux":          signal.get("signaux_identifies", []),
            "recommandation":   signal.get("recommandation", ""),
            "memo_origination": signal.get("memo_origination", ""),
            "statut":           "nouveau",
        }
        supabase.table("opportunites").upsert(data, on_conflict="entreprise").execute()
        logger.success(f"   💾 {data['entreprise']} → score {data['score_final']} ({data['niveau_alerte']})")
    except Exception as e:
        logger.error(f"   ❌ Supabase : {e}")


def sauvegarder_signal(supabase, signal):
    if not supabase:
        return
    try:
        data = {
            "source":      signal.get("source", "N/A"),
            "titre":       signal.get("titre", signal.get("raw_text", "N/A"))[:200],
            "entreprise":  signal.get("entreprise"),
            "signal_type": signal.get("signal_type", "N/A"),
            "score_ia":    signal.get("score_ia", 0),
            "url":         signal.get("url", ""),
            "raw_text":    signal.get("raw_text", "")[:500],
        }
        supabase.table("signaux").insert(data).execute()
    except Exception as e:
        logger.debug(f"Signal save error: {e}")


def run_pipeline():
    debut    = datetime.now()
    supabase = get_supabase()

    logger.info("=" * 60)
    logger.info(f"🚀 M&A RADAR MAROC — Scan du {debut.strftime('%d/%m/%Y à %H:%M')}")
    logger.info("=" * 60)

    logger.info("\n📡 PHASE 1 — Collecte (4 sources)\n")
    tous_signaux = []

    sources = [
        ("OMPIC",                     OmpicScraper),
        ("Presse Économique",         PresseEcoScraper),
        ("Bulletin Officiel",         BulletinOfficielScraper),
        ("Conseil de la Concurrence", ConseilConcurrenceScraper),
    ]

    for nom, ScraperClass in sources:
        try:
            scraper = ScraperClass()
            signaux = scraper.run()
            tous_signaux.extend(signaux)
            logger.info(f"   {nom:<32} → {len(signaux)} signaux")
        except Exception as e:
            logger.error(f"   {nom:<32} → ERREUR : {e}")

    if not tous_signaux:
        logger.warning("⚠️ Aucun signal collecté")
        return

    # Déduplication
    vus, signaux_uniques = set(), []
    for s in tous_signaux:
        cle = (s.get("entreprise", "") or s.get("titre", "")[:50]).lower().strip()
        if cle and cle not in vus:
            vus.add(cle)
            signaux_uniques.append(s)

    logger.info(f"\n   TOTAL : {len(tous_signaux)} bruts → {len(signaux_uniques)} uniques")

    # Scoring IA
    logger.info(f"\n🤖 PHASE 2 — Scoring IA\n")
    engine = ScoringEngine()
    signaux_scores = engine.analyser_batch(signaux_uniques)

    # Sauvegarde
    logger.info(f"\n💾 PHASE 3 — Sauvegarde Supabase\n")
    critiques, vigilances, radar = [], [], []

    for signal in signaux_scores:
        sauvegarder_signal(supabase, signal)
        niveau = signal.get("niveau_alerte", "FAIBLE")
        if niveau == "CRITIQUE":
            signal["memo_origination"] = engine.generer_memo(signal)
            sauvegarder_opportunite(supabase, signal)
            critiques.append(signal)
        elif niveau in ("VIGILANCE", "RADAR"):
            sauvegarder_opportunite(supabase, signal)
            if niveau == "VIGILANCE":
                vigilances.append(signal)
            else:
                radar.append(signal)

    duree = (datetime.now() - debut).seconds
    logger.info(f"""
╔══════════════════════════════════════════╗
║       RAPPORT M&A RADAR MAROC           ║
╠══════════════════════════════════════════╣
║  🔴 CRITIQUES   : {len(critiques):>3}                     ║
║  🟠 VIGILANCES  : {len(vigilances):>3}                     ║
║  🟡 RADAR       : {len(radar):>3}                     ║
║  ⏱️  Durée       : {duree}s                       ║
╚══════════════════════════════════════════╝
""")


if __name__ == "__main__":
    if "--schedule" in sys.argv:
        logger.info(f"⏰ Scan quotidien à {HEURE_SCAN_QUOTIDIEN}")
        schedule.every().day.at(HEURE_SCAN_QUOTIDIEN).do(run_pipeline)
        run_pipeline()
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_pipeline()
