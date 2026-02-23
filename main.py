"""
M&A Radar Maroc — Point d'entrée principal
Lance le pipeline complet : collecte → scoring IA → alertes

Usage :
    python main.py              # Lance un scan immédiat
    python main.py --schedule   # Lance en mode planifié (chaque jour à 07h00)
"""

import sys
import schedule
import time
from datetime import datetime
from loguru import logger

from scrapers.ompic   import OmpicScraper
from scrapers.presse  import PresseEcoScraper
from scoring.engine   import ScoringEngine
from config           import SEUIL_CRITIQUE, SEUIL_VIGILANCE, HEURE_SCAN_QUOTIDIEN


# ─── CONFIGURATION LOGS ──────────────────────────────────────────────────────
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | {message}", level="INFO")
logger.add("logs/radar_{time:YYYY-MM-DD}.log", rotation="1 day", retention="30 days")


def run_pipeline():
    """
    Pipeline complet du M&A Radar Maroc.
    
    Étapes :
    1. Collecte des signaux (OMPIC + presse)
    2. Déduplication
    3. Scoring IA (Claude)
    4. Filtrage des pertinents
    5. Génération des alertes et mémos
    6. Sauvegarde en base de données
    """
    
    debut = datetime.now()
    logger.info("=" * 60)
    logger.info(f"🚀 M&A RADAR MAROC — Scan du {debut.strftime('%d/%m/%Y à %H:%M')}")
    logger.info("=" * 60)

    # ── ÉTAPE 1 : COLLECTE ────────────────────────────────────────────────────
    logger.info("\n📡 PHASE 1 — Collecte des signaux\n")
    
    tous_signaux = []

    # OMPIC
    try:
        ompic = OmpicScraper()
        signaux_ompic = ompic.run()
        tous_signaux.extend(signaux_ompic)
        logger.info(f"   OMPIC        → {len(signaux_ompic)} signaux")
    except Exception as e:
        logger.error(f"   OMPIC        → ERREUR : {e}")

    # Presse économique
    try:
        presse = PresseEcoScraper()
        signaux_presse = presse.run()
        tous_signaux.extend(signaux_presse)
        logger.info(f"   Presse éco   → {len(signaux_presse)} signaux")
    except Exception as e:
        logger.error(f"   Presse éco   → ERREUR : {e}")

    if not tous_signaux:
        logger.warning("⚠️ Aucun signal collecté — fin du pipeline")
        return

    logger.info(f"\n   TOTAL        → {len(tous_signaux)} signaux bruts collectés")

    # ── ÉTAPE 2 : DÉDUPLICATION ────────────────────────────────────────────────
    logger.info("\n🧹 PHASE 2 — Déduplication\n")
    signaux_uniques = _dedupliquer(tous_signaux)
    logger.info(f"   {len(tous_signaux)} → {len(signaux_uniques)} signaux uniques")

    # ── ÉTAPE 3 : SCORING IA ──────────────────────────────────────────────────
    logger.info(f"\n🤖 PHASE 3 — Scoring IA ({len(signaux_uniques)} signaux)\n")
    engine = ScoringEngine()
    signaux_scores = engine.analyser_batch(signaux_uniques)

    # ── ÉTAPE 4 : FILTRAGE ────────────────────────────────────────────────────
    logger.info("\n🎯 PHASE 4 — Filtrage et priorisation\n")
    
    critiques  = [s for s in signaux_scores if s.get("niveau_alerte") == "CRITIQUE"]
    vigilances = [s for s in signaux_scores if s.get("niveau_alerte") == "VIGILANCE"]
    radar      = [s for s in signaux_scores if s.get("niveau_alerte") == "RADAR"]

    logger.info(f"   🔴 CRITIQUE  → {len(critiques)} opportunités")
    logger.info(f"   🟠 VIGILANCE → {len(vigilances)} opportunités")
    logger.info(f"   🟡 RADAR     → {len(radar)} opportunités")

    # ── ÉTAPE 5 : ALERTES & MÉMOS ─────────────────────────────────────────────
    logger.info("\n📝 PHASE 5 — Génération des alertes et mémos\n")
    
    if critiques:
        logger.info(f"   🔴 {len(critiques)} mémos d'origination à générer :")
        for signal in critiques:
            entreprise = signal.get("entreprise") or signal.get("titre", "N/A")[:40]
            score      = signal.get("score_final", 0)
            deal       = signal.get("type_deal_probable", "N/A")
            logger.info(f"      → {entreprise} | Score {score}/100 | {deal}")
            
            # Générer le mémo automatiquement
            memo = engine.generer_memo(signal)
            signal["memo_origination"] = memo
            
            # Sauvegarder le mémo
            _sauvegarder_memo(signal)

    # ── ÉTAPE 6 : RAPPORT QUOTIDIEN ───────────────────────────────────────────
    logger.info("\n📊 PHASE 6 — Rapport quotidien\n")
    _generer_rapport(critiques, vigilances, radar, debut)

    # FIN
    duree = (datetime.now() - debut).seconds
    logger.info("=" * 60)
    logger.info(f"✅ Pipeline terminé en {duree}s — {len(critiques)} opportunités critiques")
    logger.info("=" * 60)


def _dedupliquer(signaux: list) -> list:
    """Supprime les doublons basés sur le titre/nom d'entreprise."""
    vus = set()
    uniques = []
    
    for signal in signaux:
        cle = (
            signal.get("entreprise", "") or 
            signal.get("titre", "")[:50]
        ).lower().strip()
        
        if cle and cle not in vus:
            vus.add(cle)
            uniques.append(signal)
    
    return uniques


def _sauvegarder_memo(signal: dict):
    """
    Sauvegarde le mémo en fichier texte.
    En production : sauvegarder dans Supabase et envoyer par email.
    """
    import os
    os.makedirs("output/memos", exist_ok=True)
    
    entreprise = (signal.get("entreprise") or "inconnu").replace(" ", "_").replace("/", "-")
    date       = datetime.now().strftime("%Y%m%d")
    filename   = f"output/memos/memo_{entreprise}_{date}.txt"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"M&A RADAR MAROC — MÉMO D'ORIGINATION\n")
        f.write(f"{'='*50}\n")
        f.write(f"Entreprise  : {signal.get('entreprise', 'N/A')}\n")
        f.write(f"Secteur     : {signal.get('secteur', 'N/A')}\n")
        f.write(f"Score M&A   : {signal.get('score_final', 0)}/100\n")
        f.write(f"Type deal   : {signal.get('type_deal_probable', 'N/A')}\n")
        f.write(f"Source      : {signal.get('source', 'N/A')}\n")
        f.write(f"Date        : {signal.get('date', 'N/A')}\n")
        f.write(f"{'='*50}\n\n")
        f.write(signal.get("memo_origination", ""))
    
    logger.info(f"   💾 Mémo sauvegardé → {filename}")


def _generer_rapport(critiques, vigilances, radar, debut):
    """Génère et affiche le rapport quotidien du radar."""
    logger.info(f"""
┌─────────────────────────────────────────┐
│        RAPPORT QUOTIDIEN M&A RADAR      │
│        {debut.strftime('%d/%m/%Y — %H:%M')}                  │
├─────────────────────────────────────────┤
│  🔴 Opportunités CRITIQUES  : {len(critiques):>3}         │
│  🟠 Opportunités VIGILANCE  : {len(vigilances):>3}         │
│  🟡 Opportunités RADAR      : {len(radar):>3}         │
├─────────────────────────────────────────┤""")
    
    if critiques:
        logger.info("│  TOP OPPORTUNITÉS :                     │")
        for s in critiques[:3]:
            nom = (s.get("entreprise") or s.get("titre", "N/A"))[:28]
            score = s.get("score_final", 0)
            logger.info(f"│  → {nom:<28} {score:>3}/100  │")
    
    logger.info("└─────────────────────────────────────────┘")


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    
    if "--schedule" in sys.argv:
        # Mode planifié — tourne chaque jour à l'heure configurée
        logger.info(f"⏰ Mode planifié — Scan quotidien à {HEURE_SCAN_QUOTIDIEN}")
        schedule.every().day.at(HEURE_SCAN_QUOTIDIEN).do(run_pipeline)
        
        # Lancer immédiatement le premier scan
        run_pipeline()
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        # Mode immédiat — un seul scan et on s'arrête
        run_pipeline()
