"""
M&A Radar Maroc — Configuration Centrale
Modifie ce fichier selon tes besoins.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── API KEYS ────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")   # Ta clé Claude API
SUPABASE_URL      = os.getenv("SUPABASE_URL")          # URL de ta base Supabase
SUPABASE_KEY      = os.getenv("SUPABASE_KEY")          # Clé Supabase


# ─── THÈSE D'ORIGINATION ─────────────────────────────────────────────────────
# Ces poids définissent l'importance de chaque signal dans le scoring final.
# Tu peux les ajuster à tout moment selon ton expérience terrain.

SCORING_WEIGHTS = {
    "transmission_succession":      25,  # Problème de succession / fondateur âgé
    "acquereur_actif_secteur":      22,  # Concurrent avec stratégie croissance externe
    "desinvestissement_activite":   20,  # Cession filiale / désengagement non-core
    "besoin_cash_bfr":              18,  # Trésorerie négative / BFR contraint
    "gearing_eleve":                16,  # Dette élevée / endettement à maturité
    "investissements_recents":      14,  # Capex important récent
    "changement_direction":         12,  # Nouveau DG / changement de management
    "recrutement_profil_ma":        10,  # Recrutement CFO / DAF / DG adjoint
    "expansion_geographique":        8,  # Ouverture nouveaux sites / régions
    "consolidation_sectorielle":     6,  # Tendance M&A dans le secteur
}

# Seuils d'alerte
SEUIL_CRITIQUE  = 80   # Mémo auto-généré + alerte immédiate
SEUIL_VIGILANCE = 60   # Ajouté en liste de surveillance active
SEUIL_RADAR     = 40   # Surveillance passive

# ─── SECTEURS PRIORITAIRES ───────────────────────────────────────────────────
SECTEURS_PRIORITAIRES = [
    "distribution",
    "retail",
    "industrie",
    "manufacturing",
    "btp",
    "materiaux construction",
    "logistique",
    "agroalimentaire",
    "sante",
    "fintech",
    "education",
]

# ─── MOTS-CLÉS M&A ───────────────────────────────────────────────────────────
# Mots-clés qui déclenchent une analyse approfondie par l'IA
MOTS_CLES_MA = [
    # Transmission
    "succession", "transmission", "héritier", "cession", "retraite fondateur",
    "deuxième génération", "passage de flambeau",
    # Financement
    "augmentation de capital", "levée de fonds", "ouverture capital",
    "endettement", "refinancement", "restructuration dette",
    # Opérations
    "acquisition", "fusion", "rapprochement", "partenariat stratégique",
    "prise de participation", "cession filiale", "désengagement",
    # Signaux de direction
    "nouveau directeur général", "nouveau PDG", "départ dirigeant",
    "recrutement CFO", "recrutement DAF", "directeur financier",
    # Pre-IPO
    "introduction en bourse", "pré-IPO", "cotation", "appel public épargne",
]

# ─── SOURCES ─────────────────────────────────────────────────────────────────
SOURCES_PRESSE = [
    "https://www.medias24.com",
    "https://www.leconomiste.com",
    "https://www.challenge.ma",
    "https://www.aujourdhui.ma",
    "https://www.leseco.ma",
]

# ─── PLANNING ────────────────────────────────────────────────────────────────
# Heure de lancement du scan quotidien (format HH:MM)
HEURE_SCAN_QUOTIDIEN = "07:00"
