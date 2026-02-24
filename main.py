import sys, os, schedule, time
from datetime import datetime
from loguru import logger
from scrapers.ompic import OmpicScraper
from scrapers.presse import PresseEcoScraper
from scrapers.bulletin_officiel import BulletinOfficielScraper
from scrapers.conseil_concurrence import ConseilConcurrenceScraper
from scoring.engine import ScoringEngine
from config import HEURE_SCAN_QUOTIDIEN
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | {message}", level="INFO")

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def save_opp(sb, s):
    if not sb: return
    try:
        sb.table("opportunites").upsert({
            "entreprise": s.get("entreprise") or s.get("titre","N/A")[:50],
            "secteur": s.get("secteur","N/A"),
            "score_final": s.get("score_final",0),
            "niveau_alerte": s.get("niveau_alerte","RADAR"),
            "type_deal": s.get("type_deal_probable","inconnu"),
            "source": s.get("source","N/A"),
            "signaux": s.get("signaux_identifies",[]),
            "recommandation": s.get("recommandation",""),
            "memo_origination": s.get("memo_origination",""),
            "statut": "nouveau",
        }, on_conflict="entreprise").execute()
        logger.success(f"   💾 {s.get('entreprise','?')} → {s.get('score_final',0)}/100")
    except Exception as e:
        logger.error(f"   ❌ {e}")

def save_signal(sb, s):
    if not sb: return
    try:
        sb.table("signaux").insert({
            "source": s.get("source","N/A"),
            "titre": s.get("titre",s.get("raw_text","N/A"))[:200],
            "entreprise": s.get("entreprise"),
            "signal_type": s.get("signal_type","N/A"),
            "score_ia": s.get("score_ia",0),
            "url": s.get("url",""),
            "raw_text": s.get("raw_text","")[:500],
        }).execute()
    except: pass

def run_pipeline():
    debut = datetime.now()
    sb = get_supabase()
    logger.info("="*60)
    logger.info(f"🚀 M&A RADAR MAROC — {debut.strftime('%d/%m/%Y à %H:%M')}")
    logger.info("="*60)

    tous = []
    for nom, Cls in [("OMPIC",OmpicScraper),("Presse RSS",PresseEcoScraper),("Bulletin Officiel",BulletinOfficielScraper),("Conseil Concurrence",ConseilConcurrenceScraper)]:
        try:
            sigs = Cls().run()
            tous.extend(sigs)
            logger.info(f"   {nom:<28} → {len(sigs)} signaux")
        except Exception as e:
            logger.error(f"   {nom} → ERREUR: {e}")

    vus, uniques = set(), []
    for s in tous:
        k = (s.get("entreprise","") or s.get("titre","")[:50]).lower().strip()
        if k and k not in vus:
            vus.add(k); uniques.append(s)

    logger.info(f"\n   TOTAL: {len(tous)} → {len(uniques)} uniques")
    engine = ScoringEngine()
    scores = engine.analyser_batch(uniques)

    crit, vig, rad = [], [], []
    for s in scores:
        save_signal(sb, s)
        n = s.get("niveau_alerte","FAIBLE")
        if n == "CRITIQUE":
            s["memo_origination"] = engine.generer_memo(s)
            save_opp(sb, s); crit.append(s)
        elif n == "VIGILANCE":
            save_opp(sb, s); vig.append(s)
        elif n == "RADAR":
            save_opp(sb, s); rad.append(s)

    logger.info(f"\n🔴 CRITIQUES: {len(crit)} | 🟠 VIGILANCES: {len(vig)} | 🟡 RADAR: {len(rad)}")

if __name__ == "__main__":
    if "--schedule" in sys.argv:
        schedule.every().day.at(HEURE_SCAN_QUOTIDIEN).do(run_pipeline)
        run_pipeline()
        while True:
            schedule.run_pending(); time.sleep(60)
    else:
        run_pipeline()
