"""
M&A Radar Maroc — Point d'entrée principal
Lance le pipeline complet : collecte → scoring IA → sauvegarde Supabase
"""

import sys
import os
import schedule
import time
from datetime import datetime
from loguru import logger

from scrapers.ompic   import OmpicScraper
from scrapers.presse  import PresseEcoScraper
from scoring.engine   import ScoringEngine
from config           import SEUIL_CRITIQUE, SEUIL_VIGILANCE, HEURE_SCAN_QUOTIDIEN

# Supabase
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ─── LOGS ────────────────────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | {message}", level="INFO")


def get_supabase():
    """Retourne le client Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("⚠️ Variables Supabase manquantes — mode local uniquement")
        return None
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def sauvegarder_opportunite(supabase, signal: dict):
    """Sauvegarde une opportunité dans Supabase."""
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
        logger.success(f"   💾 Sauvegardé → {data['entreprise']} (score {data['score_final']})")

    except Exception as e:
        logger.error(f"   ❌ Erreur Supabase : {e}")


def sauvegarder_signal(supabase, signal: dict):
    """Sauvegarde un signal brut dans Supabase."""
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
        logger.error(f"   ❌ Erreur signal Supabase : {e}")


def run_pipeline():
    """Pipeline complet du M&A Radar Maroc."""

    debut = datetime.now()
    supabase = get_supabase()

    logger.info("=" * 60)
    logger.info(f"🚀 M&A RADAR MAROC — Scan du {debut.strftime('%d/%m/%Y à %H:%M')}")
    logger.info("=" * 60)

    # ── COLLECTE ──────────────────────────────────────────────────────────────
    logger.info("\n📡 PHASE 1 — Collecte des signaux\n")

    tous_signaux = []

    try:
        ompic = OmpicScraper()
        signaux_ompic = ompic.run()
        tous_signaux.extend(signaux_ompic)
        logger.info(f"   OMPIC        → {len(signaux_ompic)} signaux")
    except Exception as e:
        logger.error(f"   OMPIC        → ERREUR : {e}")

    try:
        presse = PresseEcoScraper()
        signaux_presse = presse.run()
        tous_signaux.extend(signaux_presse)
        logger.info(f"   Presse éco   → {len(signaux_presse)} signaux")
    except Exception as e:
        logger.error(f"   Presse éco   → ERREUR : {e}")

    if not tous_signaux:
        logger.warning("⚠️ Aucun signal collecté")
        return

    logger.info(f"\n   TOTAL        → {len(tous_signaux)} signaux bruts")

    # ── DÉDUPLICATION ─────────────────────────────────────────────────────────
    vus = set()
    signaux_uniques = []
    for s in tous_signaux:
        cle = (s.get("entreprise", "") or s.get("titre", "")[:50]).lower().strip()
        if cle and cle not in vus:
            vus.add(cle)
            signaux_uniques.append(s)

    logger.info(f"   Après dédup  → {len(signaux_uniques)} signaux uniques")

    # ── SCORING IA ────────────────────────────────────────────────────────────
    logger.info(f"\n🤖 PHASE 2 — Scoring IA\n")
    engine = ScoringEngine()
    signaux_scores = engine.analyser_batch(signaux_uniques)

    # ── SAUVEGARDE SUPABASE ───────────────────────────────────────────────────
    logger.info(f"\n💾 PHASE 3 — Sauvegarde Supabase\n")

    critiques  = []
    vigilances = []

    for signal in signaux_scores:
        # Sauvegarder le signal brut
        sauvegarder_signal(supabase, signal)

        # Sauvegarder comme opportunité si score suffisant
        niveau = signal.get("niveau_alerte", "FAIBLE")

        if niveau == "CRITIQUE":
            # Générer mémo automatiquement
            memo = engine.generer_memo(signal)
            signal["memo_origination"] = memo
            sauvegarder_opportunite(supabase, signal)
            critiques.append(signal)

        elif niveau == "VIGILANCE":
            sauvegarder_opportunite(supabase, signal)
            vigilances.append(signal)

        elif niveau == "RADAR":
            sauvegarder_opportunite(supabase, signal)

    # ── RAPPORT ───────────────────────────────────────────────────────────────
    duree = (datetime.now() - debut).seconds
    logger.info(f"""
╔══════════════════════════════════════════╗
║      RAPPORT M&A RADAR MAROC            ║
║      {debut.strftime('%d/%m/%Y — %H:%M')}                   ║
╠══════════════════════════════════════════╣
║  🔴 CRITIQUES  : {len(critiques):>3}                      ║
║  🟠 VIGILANCES : {len(vigilances):>3}                      ║
║  ⏱️  Durée      : {duree}s                        ║
╚══════════════════════════════════════════╝
""")


if __name__ == "__main__":

    if "--schedule" in sys.argv:
        logger.info(f"⏰ Mode planifié — Scan quotidien à {HEURE_SCAN_QUOTIDIEN}")
        schedule.every().day.at(HEURE_SCAN_QUOTIDIEN).do(run_pipeline)
        run_pipeline()  # Premier scan immédiat
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_pipeline()
