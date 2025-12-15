#!/usr/bin/env python3
"""
global_relevancy.py
- Multi-query processing system for relevancy search across the global catalog
- Handles complex queries with multiple products separated by commas, semicolons, or newlines
- Uses embedding similarity + token overlap + category-specific boosts
- Routes "Analyser" queries to analyser_relevancy, "Endo" to endo_relevancy (if present)
- CLI: python global_relevancy.py "query1, query2, query3"
"""
import os
import json
import numpy as np
import re
import math
import argparse
import unicodedata
from typing import List, Dict, Any, Tuple
from sentence_transformers import SentenceTransformer

# ------------------------------------------------------------------
#  SAFE IMPORTS FOR SPECIAL MODELS
# ------------------------------------------------------------------
HAS_ANALYSER_MODEL = False
HAS_ENDO_MODEL = False
analyser_predict = None
predict_endo = None

try:
    from analyser_relevancy import predict_relevancy as analyser_predict
    HAS_ANALYSER_MODEL = True
except Exception as e:
    print("Warning: analyser_relevancy not loaded:", e)

try:
    from endo_relevancy import predict_endo as predict_endo
    HAS_ENDO_MODEL = True
except Exception as e:
    print("Warning: endo_relevancy not loaded:", e)

# ------------------------------------------------------------------

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INDEX_PATH = os.path.join(ROOT, "data", "embeddings", "global_index.json")
EMB_PATH = os.path.join(ROOT, "data", "embeddings", "global_embeddings.npy")
MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"

# Tunable weights
EMB_WEIGHT = 1.0
TOKEN_WEIGHT = 0.35
TITLE_WEIGHT = 0.5
CATEGORY_BOOST = 0.25
TOP_K = 5

# keyword->category map (extendable)
CATEGORY_KEYWORDS = {
    "pipette": "Pipettes",
    "pipettes": "Pipettes",
    "fixed volume": "Pipettes",
    "variable": "Pipettes",
    "dengue": "Elisa",
    "ns1": "Elisa",
    "hiv": "Elisa",
    "hbsag": "Elisa",
    "crp": "Turbidimetry",
    "rf": "Nephelometry",
    "aso": "Nephelometry",
    "control": "Controls",
    "control kit": "Controls",
    "system pack": "System Packs",
    "albumin": "System Packs",
    "anti a": "BloodGroup",
    "anti b": "BloodGroup",
    "anti d": "BloodGroup",
    "anti ab": "BloodGroup",
    "blood grouping": "BloodGroup",
    "reagent": "Reagents",
    "reagents": "Reagents",
    "analyser": "Analyser",
    "analyzer": "Analyser",
    "hematology": "Analyser",
    "hb": "Analyser",
    "meriscreen": "Meriscreen",
    "rapid": "Rapids",
    "elisa": "Elisa",
    "nephelometry": "Nephelometry",
    "turbidimetry": "Turbidimetry",
    "5 part": "Analyser",
    "3 part": "Analyser",
    "cbc": "Analyser",
    "celquant": "Analyser",
    "autoloader": "Analyser",
    "5 part": "Analyser",
    "3 part": "Analyser",
    "6 part": "Analyser",
    "hematology": "Analyser",
    "cell counter": "Analyser",
    "automated analy": "Analyser",
    "cbc": "Analyser",
    "celquant": "Analyser",
    "autoloader": "Analyser",

    "biochemistry": "Analyser",
    "bio chemistry": "Analyser",
    "chemistry analy": "Analyser",
    "fully automatic biochemistry analyzer": "Analyser",
    "semi automatic bio chemistry analyser": "Analyser",
    "veterinary biochemistry analyzer": "Analyser",

    "elisa reader": "Analyser",
    "elisa washer": "Analyser",
    "elisa plate washer": "Analyser",
    "elisa test": "Analyser",
    "immunoassay analyzer": "Analyser",

    "coagulation": "Analyser",
    "coagulation analyzer": "Analyser",
    "semi automated coagulation analyser": "Analyser",

    "electrolyte": "Analyser",
    "electrolyte analy": "Analyser",
    "electrolyte analyzer": "Analyser",

    "pcr machine": "Analyser",
    "real time pcr": "Analyser",
    "rt-pcr": "Analyser",
    "rtpcr": "Analyser",
    "qpcr": "Analyser",
    "thermal cycler": "Analyser",

    "dna extraction system": "Analyser",
    "rna extraction": "Analyser",
    "dna extraction": "Analyser",

    "gel doc": "Analyser",
    "gel documentation system": "Analyser",

    "hplc analy": "Analyser",
    "hplc system": "Analyser",
    "liquid chromatograph": "Analyser",

    "immunoassay": "Analyser",
    "immunoassay analyzer reagents": "Analyser",
    "electrolyte analyzer reagents": "Analyser",
    "coagulation analyzer reagents": "Analyser",
    "biochemistry reagent kit": "Analyser",

    "poct": "Analyser",
    "point of care": "Analyser",
    "glucometer": "Analyser",
    # Endo
    "bonewax": "Endo",
    "bone wax": "Endo",
    "catgut": "Endo",
    "suture": "Endo",
    "sutures": "Endo",
    "endo": "Endo",
    "aspiron": "Endo",
    "Polyglactine": "Endo",
    "endo": "Endo",
    "endoscope": "Endo",
    "endoscopes": "Endo",
    "endoscopic": "Endo",
    "endoscopic equipment": "Endo",
    "endoscopic accessories": "Endo",

    "trocar": "Endo",
    "endocutter": "Endo",
    "endo cutter": "Endo",
    "reload linear cutter": "Endo",
    "circular stapler": "Endo",
    "hemorrhoid stapler": "Endo",
    "skin stapler": "Endo",
    "stapler": "Endo",

    "suture": "Endo",
    "suture item": "Endo",
    "ligation clip": "Endo",
    "fixation device": "Endo",
    "powered fixation": "Endo",

    "hernia": "Endo",
    "hernia mesh": "Endo",
    "herniamesh": "Endo",
    "anatomical mesh": "Endo",

    "haemostat": "Endo",
    "haemostatics": "Endo",
    "hemostat": "Endo",
    "gelatin sponge": "Endo",
    "oxidised cellulose": "Endo",
    "oxidised regenerated cellulose": "Endo",
    "bone wax": "Endo",
    "umbilical cotton tape": "Endo",

    "laparoscop": "Endo",
    "minimal invasive": "Endo",
    "ultrasonic surg": "Endo",
    "surgical system": "Endo",

    "diode laser": "Endo",
    "laser diode": "Endo",
    "laser fiber": "Endo",
    "fibre laser": "Endo",
    "fiber laser": "Endo",
    "laser ablation": "Endo",

    "robotics": "Endo",
    "robot machines": "Endo",
    "robot components": "Endo",
    "robotic assisted": "Endo",
    "robotic surg": "Endo",
    "surg robot": "Endo",
    "robot for surg": "Endo",
    "ras": "Endo",

    "joint replace": "Endo",
    "knee replace": "Endo",
    "tkr robot": "Endo",

    "endotracheal tubes": "Endo",
    "transducers": "Endo",
    "cartridges": "Endo",

    "disposable medical item": "Endo",
    "medical consumable": "Endo",
    "medical item": "Endo",

    "intra uterine": "Endo",
    "iud": "Endo",
    "iucd": "Endo",
    "hormonal intrauterine": "Endo",
    "anti contraceptive": "Endo",
    "contraceptive": "Endo",

    "skill lab": "Endo",
    "polyglactine": "Endo",
    "CHROMIC CATGUT": "Endo",
    "gt-bonewax" : "Endo",
    "gt bonewax" : "Endo",
    "gtbonewax" : "Endo",
    "gt_bonewax" : "Endo",
    "gt bone wax" : "Endo",
    "gt-bone-wax" : "Endo",
    "gt bone-wax" : "Endo",
    "gt-bone wax" : "Endo",
    "gt bonewax" : "Endo",
    "gt chromic catgut" : "Endo",
    "gt appl20844apgn202317 anyl203336appm611" : "Endo",
    "gt appl20844apgn202317 anyl203336appm715" : "Endo",
    "gt polyamide black" : "Endo",
    "ane polyamide black" : "Endo",
    "sp polyamide black" : "Endo",
    "gt polyglecaprone undyed" : "Endo",
    "ane polyglecaprone undyed" : "Endo",
    "gt polydioxanone violet" : "Endo",
    "sp polydioxanone violet" : "Endo",
    "gt polyglycolic acid violet" : "Endo",
    "gt polyglycolic acid violet" : "Endo",
    "gt polyglactin 910 fast undyed" : "Endo",
    "ane polyglactin 910 fast undyed" : "Endo",
    "gt polyglactin 910 violet" : "Endo",
    "ane polyglactin 910 violet" : "Endo",
    "sp polyglactin 910 violet" : "Endo",
    "gt polyglactin 910 violet" : "Endo",
    "gt polyglactin 910 undyed" : "Endo",
    "ane polyglactin 910 violet" : "Endo",
    "sp polyglactin 910 violet" : "Endo",
    "ane polyglactin 910 undyed" : "Endo",
    "sp polyglactin 910 undyed" : "Endo",
    "gt polyglactin 910 with triclosan violet" : "Endo",
    "ane polyglactin 910 with triclosan violet" : "Endo",
    "ane polyglactin 910 with triclosan undyed" : "Endo",
    "gt polyglactin 910 t plus violet" : "Endo",
    "gt polypropylene blue" : "Endo",
    "ane polypropylene blue" : "Endo",
    "sp polypropylene blue" : "Endo",
    "sp polypropylene blue" : "Endo",
    "gt polypropylene blue" : "Endo",
    "ane polypropylene blue" : "Endo",
    "polypropylene mesh" : "Endo",
    "gt polypropylene mesh" : "Endo",
    "gt polyester green" : "Endo",
    "gt polyester gr" : "Endo",
    "gt silk black" : "Endo",
    "ane silk black" : "Endo",
    "sp silk black" : "Endo",
    "avr kit" : "Endo",
    "topical skin adhesive glue" : "Endo",
    "bowel structures" : "Endo",
    "Silk Black" : "Endo",
    "BONEWAX X 2 GM" : "Endo",
    "CABG KIT FOR DR. BRIJ MOHAN SINGH" : "Endo",
    "CABG KIT FOR DR. MAHESH KEDAR" : "Endo",
    "CABG KIT-DR.VINAYAK KARMARKAR" : "Endo",
    "CABG KIT FOR DR. PRASHANT MISHRA" : "Endo",
    "CHROMIC CATGUT 1 X 50 LOOP" : "Endo",
    "PE+PU ANATOMICAL MESH LEFT (11CM*15CM)" : "Endo",
    "PE+PU ANATOMICAL MESH RIGHT (11CM*15CM)" : "Endo",
    "PE+PU ANATOMICAL MESH LEFT (12CM*16CM)" : "Endo",
    "PE+PU ANATOMICAL MESH RIGHT (12CM*16CM)" : "Endo",
    "PE+PU MESH (15CM*15CM)" : "Endo",
    "CLUTCH TOURNIQUET DEVICE-L" : "Endo",
    "CLUTCH TOURNIQUET DEVICE-M" : "Endo",
    "CLUTCH TOURNIQUET DEVICE-XL" : "Endo",
    "DISPOSABLE BLADELESS TROCAR - 05 MM" : "Endo",
    "DISPOSABLE BLADELESS TROCAR KIT 5-5 MM" : "Endo",
    "DISPOSABLE BLADELESS TROCAR - 10 MM" : "Endo",
    "DISPOSABLE BLADELESS TROCAR KIT 10-10 MM" : "Endo",
    "DISPOSABLE BLADELESS TROCAR - 12 MM" : "Endo",
    "DISPOSABLE BLADELESS TROCAR KIT 12-12 MM" : "Endo",
    "DISPOSABLE BLADELESS TROCAR - 15MM" : "Endo",
    "CU 375" : "Endo",
    "CU 375 SLEEK" : "Endo",
    "CU 250" : "Endo",
    "CU 250 SLEEK" : "Endo",
    "CU T 380A" : "Endo",
    "HORMONAL INTRAUTERINE SYSTEM" : "Endo",
    "LAPROSCOPIC APPLICATOR for Medium-Large Titanium clip" : "Endo",
    "LAPROSCOPIC APPLICATOR for Large Titanium clip" : "Endo",
    "LAPROSCOPIC APPLICATOR 300" : "Endo",
    "LAPROSCOPIC APPLICATOR 400" : "Endo",
    "Absorbable Gelatin Sponge 80 X 07MM ENDOSCOPIC TAMPON" : "Endo",
    "Absorbable Gelatin Sponge 80*30 TAMPON" : "Endo",
    "Absorbable Gelatin Sponge 80501 FILM" : "Endo",
    "Absorbable Gelatin Sponge 805010 NO TOUCH" : "Endo",
    "Absorbable Gelatin Sponge 805010 STANDARD" : "Endo",
    "Absorbable Gelatin POWDER" : "Endo",
    "OPEN APPLICATOR for Small Titanium clip 15CM" : "Endo",
    "OPEN APPLICATOR for Small Titanium clip 20CM" : "Endo",
    "OPEN APPLICATOR for Small Titanium clip 28CM" : "Endo",
    "OPEN APPLICATOR for Medium Titanium clip 15CM" : "Endo",
    "OPEN APPLICATOR for Medium Titanium clip 20CM" : "Endo",
    "OPEN APPLICATOR for Medium Titanium clip 28CM" : "Endo",
    "OPEN APPLICATOR for Medium-Large Titanium clip 15CM" : "Endo",
    "OPEN APPLICATOR for Medium-Large Titanium clip 20CM" : "Endo",
    "OPEN APPLICATOR for Medium-Large Titanium clip 28CM" : "Endo",
    "OPEN APPLICATOR for Large Titanium clip 15CM" : "Endo",
    "OPEN APPLICATOR for Large Titanium clip 20CM" : "Endo",
    "OPEN APPLICATOR for Large Titanium clip 28CM" : "Endo",
    "PGN01 2347DNL, NYL20 3336, PGN01 2347" : "Endo",
    "AUTO LINEAR STAPLER 30mm" : "Endo",
    "AUTO LINEAR STAPLER 45mm" : "Endo",
    "bonewax x 2 gm" : "Endo",
    "cabg kit for dr brij mohan singh" : "Endo",
    "cabg kit for dr mahesh kedar" : "Endo",
    "cabg kit dr vinayak karmarkar" : "Endo",
    "cabg kit for dr prashant mishra" : "Endo",
    "chromic catgut 1 x 50 loop" : "Endo",
    "pe pu anatomical mesh left" : "Endo",
    "pe pu anatomical mesh right" : "Endo",
    "pe pu anatomical mesh left" : "Endo",
    "pe pu anatomical mesh right" : "Endo",
    "pe pu mesh" : "Endo",
    "clutch tourniquet device l" : "Endo",
    "clutch tourniquet device m" : "Endo",
    "clutch tourniquet device xl" : "Endo",
    "disposable bladeless trocar 5 mm" : "Endo",
    "disposable bladeless trocar kit 5 5 mm" : "Endo",
    "disposable bladeless trocar 10 mm" : "Endo",
    "disposable bladeless trocar kit 10 10 mm" : "Endo",
    "disposable bladeless trocar 12 mm" : "Endo",
    "disposable bladeless trocar kit 12 12 mm" : "Endo",
    "disposable bladeless trocar 15 mm" : "Endo",
    "cu 375" : "Endo",
    "cu 375 sleek" : "Endo",
    "cu 250" : "Endo",
    "cu 250 sleek" : "Endo",
    "cu t 380a" : "Endo",
    "hormonal intrauterine system" : "Endo",
    "laparoscopic applicator medium large titanium clip" : "Endo",
    "laparoscopic applicator large titanium clip" : "Endo",
    "laparoscopic applicator 300" : "Endo",
    "laparoscopic applicator 400" : "Endo",
    "absorbable gelatin sponge 80 x 07mm endoscopic tampon" : "Endo",
    "absorbable gelatin sponge 80 30 tampon" : "Endo",
    "absorbable gelatin sponge 80 50 1 film" : "Endo",
    "absorbable gelatin sponge 80 50 10 no touch" : "Endo",
    "absorbable gelatin sponge 80 50 10 standard" : "Endo",
    "absorbable gelatin powder" : "Endo",
    "open applicator small titanium clip 15cm" : "Endo",
    "open applicator small titanium clip 20cm" : "Endo",
    "open applicator small titanium clip 28cm" : "Endo",
    "open applicator medium titanium clip 15cm" : "Endo",
    "open applicator medium titanium clip 20cm" : "Endo",
    "open applicator medium titanium clip 28cm" : "Endo",
    "open applicator medium large titanium clip 15cm" : "Endo",
    "open applicator medium large titanium clip 20cm" : "Endo",
    "open applicator medium large titanium clip 28cm" : "Endo",
    "open applicator large titanium clip 15cm" : "Endo",
    "open applicator large titanium clip 20cm" : "Endo",
    "open applicator large titanium clip 28cm" : "Endo",
    "pgn01 2347dnl nyl20 3336 pgn01 2347" : "Endo",
    "auto linear stapler 30mm" : "Endo",
    "auto linear stapler 45mm" : "Endo",
    "auto linear stapler" : "Endo",
    "ptfe pledget" : "Endo",
    "disposable circular stapler" : "Endo",
    "dial linear stapler" : "Endo",
    "polyester whgr" : "Endo",
    "polyester green" : "Endo",
    "chromic catgut" : "Endo",
    "chromic catgut" : "Endo",
    "plain catgut" : "Endo",
    "polyester green" : "Endo",
    "polyester gr" : "Endo",
    "polyester white" : "Endo",
    "polyester wh" : "Endo",
    "ne polyester white" : "Endo",
    "ne polyester green" : "Endo",
    "c1 polyester green" : "Endo",
    "c2 polyester green" : "Endo",
    "endoscopic linear cutter" : "Endo",
    "endoscopic linear cutter short" : "Endo",
    "endoscopic linear cutter medium" : "Endo",
    "endoscopic linear cutter long" : "Endo",
    "power endoscopic linear cutter" : "Endo",
    "power endocutter" : "Endo",
    "power endocutter reload" : "Endo",
    "power endoscopic linear cutter reload" : "Endo",
    "trio staple technology" : "Endo",
    "vascular reload" : "Endo",
    "vascular medium reload" : "Endo",
    "gynaecologist laser hand piece" : "Endo",
    "mesic gynaecologist laser hand piece" : "Endo",
    "powered hernia mesh fixation device" : "Endo",
    "mesh fixation device" : "Endo",
    "powered tacker" : "Endo",
    "polylactide co glycolide absorbable" : "Endo",
    "titanium non absorbable" : "Endo",
    "disposable circular stapler" : "Endo",
    "disposable linear cutter" : "Endo",
    "disposable linear cutter reload" : "Endo",
    "disposable liner stapler dial reload" : "Endo",
    "polylactide co glycolide absorbable mesh fixation device" : "Endo",
    "titanium non absorbable mesh fixation device" : "Endo",
    "disposable linear stapler reload" : "Endo",
    "disposable liner stapler dial reload" : "Endo",
    "liver kit" : "Endo",
    "liver transplant kit" : "Endo",
    "titanium ligating clip small" : "Endo",
    "titanium ligating clip medium" : "Endo",
    "titanium ligating clip medium large" : "Endo",
    "ligating clip" : "Endo",
    "titanium ligating clip large" : "Endo",
    "polyester white green" : "Endo",
    "pledgets" : "Endo",
    "polyester whgr" : "Endo",
    "polyester xl whgr" : "Endo",
    "disposable bladeless optical trocar" : "Endo",
    "disposable trocar" : "Endo",
    "trocar" : "Endo",
    "disposable bladeless optical long sleeve trocar" : "Endo",
    "powered hernia mesh fixation device absorbable" : "Endo",
    "disposable hemorrhoids stapler" : "Endo",
    "disposable hemorrhoid stapler" : "Endo",
    "long sleeve trocar" : "Endo",
    "disposable pph" : "Endo",
    "polyglycolic acid violet" : "Endo",
    "polyglycolic acid undyed" : "Endo",
    "polyglycolic acid violet heavy" : "Endo",
    "polyglycolic acid rpd undyed" : "Endo",
    "suture practice starter kit basic" : "Endo",
    "disposable skin stapler" : "Endo",
    "skin stapler" : "Endo",
    "skin stapler plus" : "Endo",
    "meril suture practice starter kit basic" : "Endo",
    "ultrasonic generator" : "Endo",
    "compact ultrasonic generator" : "Endo",
    "skin stapler hand piece transducer" : "Endo",
    "ultrasonic scalpel" : "Endo",
    "hand piece transducer" : "Endo",
    "ultrasonic scalpel with transducer" : "Endo",
    "mvr kit" : "Endo",
    "polyamide black" : "Endo",
    "polyamide black" : "Endo",
    "ne polyamide black" : "Endo",
    "loop polyamide black" : "Endo",
    "oxidized regenerated cellulose fibre" : "Endo",
    "oxidized regenerated cellulose standard" : "Endo",
    "oxidized regenerated cellulose woven" : "Endo",
    "polyglecaprone undyed" : "Endo",
    "polyglecaprone violet" : "Endo",
    "polyglecaprone" : "Endo",
    "polyglycaprone undyed" : "Endo",
    "polyglycaprone violet" : "Endo",
    "polypropylene polyglecaprone composite mesh" : "Endo",
    "pacing wire" : "Endo",
    "polydioxanone" : "Endo",
    "polydioxanone violet" : "Endo",
    "polyester 3d mesh" : "Endo",
    "polyglactin 910 fast undyed" : "Endo",
    "Round Body" : "Endo",
    "Taper Cut" : "Endo",
    "Tapercut" : "Endo",
    "Reverse Cutting" : "Endo",
    "Cutting" : "Endo",
    "Blunt Point" : "Endo",
    "SKKI Round Body" : "Endo",
    "SKKI Needle" : "Endo",
    "V-Black Needle" : "Endo",
    "Round Body Double Needle" : "Endo",
    "Taper Cut Double Needle" : "Endo",
    "Reverse Cutting Double Needle" : "Endo",
    "NE (non-eyed)" : "Endo",
    "PRC I" : "Endo",
    "HCRCRBDN" : "Endo",
    "C1" : "Endo",
    "TN" : "Endo",
    "TH" : "Endo",
    "VB (V-Black)" : "Endo",
    "BP (Blunt Point)" : "Endo",
    "Elixir Flash Point Needle" : "Endo",
    "Polyglactin 910 Violet 2 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 1 X 75 31MM J Tapercut" : "Endo",
    "Polyglactin 910 Violet 2 X 90 40MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Undyed 1 X 90 36MM 1/2 Circle Round Body" : "Endo",
    "NE Polyglactin 910 Violet 2 X 90 40MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 1 X 90 45MM 1/2 Circle BP" : "Endo",
    "Polyglactin 910 Violet 0 X 90 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 1 X 180 40MM 1/2 Circle Round Body Tapercut Double Needle" : "Endo",
    "Polyglactin 910 Violet 0 X 70 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 1 X 110 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 1 X 90 40MM 1/2 Circle Round Body H" : "Endo",
    "Polyglactin 910 Violet 0 X 140 40MM 1/2 Circle Round Body Taper Cut Double Needle" : "Endo",
    "Polyglactin 910 Violet 1 X 45 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 180 40MM 1/2 Circle Round Body Taper Cut Double Needle" : "Endo",
    "Polyglactin 910 Violet 1 X 120 36MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 110 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 1 X 90 40MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 0 X 45 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 1 X 180 50 40MM 1/2 Circle Round Body Tapercut Double Needle" : "Endo",
    "Polyglactin 910 Violet 0 X 90 36MM 1/2 Circle Taper Cut" : "Endo",
    "Polyglactin 910 Violet 1 X 35 23MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle Taper Cut" : "Endo",
    "Polyglactin 910 Violet 2 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 90 36MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 2 X 90 40MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 0 X 70 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 45 23MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 0 X 90 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 45 22MM SKKI Round Body V Black Needle" : "Endo",
    "Polyglactin 910 Violet 0 X 70 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle Blunt Point" : "Endo",
    "Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Undyed 0 X 90 36MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 140 40MM 1/2 Circle Round Body Tapercut Double Needle" : "Endo",
    "Polyglactin 910 Undyed 0 X 75 40MM 1/2 Circle Taper Cut" : "Endo",
    "Polyglactin 910 Violet 0 X 180 40MM 1/2 Circle Round Body Tapercut Double Needle" : "Endo",
    "NE Polyglactin 910 Violet 0 X 90 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 110 40MM 1/2 Circle Round Body" : "Endo",
    "NE Polyglactin 910 Violet 0 X 180 40MM 1/2 Circle Round Body Taper Cut Double Needle" : "Endo",
    "Polyglactin 910 Violet 0 X 45 40MM 1/2 Circle Round Body" : "Endo",
    "NE Polyglactin 910 Violet 0 X 110 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle Round Body C1" : "Endo",
    "NE Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 90 36MM 1/2 Circle Tapercut C1" : "Endo",
    "NE Polyglactin 910 Violet 0 X 90 36MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle Tapercut" : "Endo",
    "Polyglactin 910 Violet 2 0 X 70 26MM 1/2 Circle Round Body V Black Needle" : "Endo",
    "Polyglactin 910 Violet 0 X 90 36MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 30MM 3/8 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 0 X 70 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 45 23MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 2 0 X 140 30MM 1/2 Circle Round Body Taper Cut Double Needle" : "Endo",
    "Polyglactin 910 Violet 0 X 45 22MM SKKI Round Body VB" : "Endo",
    "Polyglactin 910 Violet 2 0 X 45 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle BP" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 30MM 1/2 Circle Round Body Double Needle" : "Endo",
    "Polyglactin 910 Undyed 0 X 90 36MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 70 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Undyed 0 X 75 40MM 1/2 Circle Tapercut" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 90 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 26MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 180 40MM 1/2 Circle Round Body Tapercut Double Needle" : "Endo",
    "Polyglactin 910 Violet 2 0 X 35 26MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 0 X 110 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 40MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 0 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Undyed 2 0 X 76 60MM Straight Cutting" : "Endo",
    "Polyglactin 910 Violet 0 X 90 36MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Undyed 2 0 X 90 26MM 3/8 Circle Cutting" : "Endo",
    "Polyglactin 910 Violet 2 0 X 70 26MM 1/2 Circle Round Body VB" : "Endo",
    "Polyglactin 910 Violet 2 0 X 45 19MM SKKI Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 30MM 3/8 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 2 0 X 70 36MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 70 26MM 5/8 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 140 30MM 1/2 Circle Round Body Taper Cut Double Needle" : "Endo",
    "Polyglactin 910 Violet 2 0 X 110 26MM 5/8 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 45 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 75 26MM 1/2 Circle Taper Cut" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 30MM 1/2 Circle Round Body Double Needle" : "Endo",
    "Polyglactin 910 Undyed 2 0 X 90 36MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 70 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 75 22MM 1/2 Circle Taper Cut" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "NE Polyglactin 910 Violet 2 0 X 90 30MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 26MM 1/2 Circle Round Body" : "Endo",
    "NE Polyglactin 910 Violet 2 0 X 90 40MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 35 26MM 1/2 Circle Round Body" : "Endo",
    "NE Polyglactin 910 Violet 2 0 X 90 40MM 1/2 Circle Reverse Cutting" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 40MM 1/2 Circle Reverse Cutting" : "Endo",
    "NE Polyglactin 910 Undyed 2 0 X 90 26MM 3/8 Circle Cutting" : "Endo",
    "Polyglactin 910 Undyed 2 0 X 76 60MM Straight Cutting" : "Endo",
    "Polyglactin 910 Undyed 2 0 X 75 26MM 3/8 Circle Cutting" : "Endo",
    "Polyglactin 910 Violet 2 0 X 90 20MM 1/2 Circle Round Body" : "Endo",
    "Polyglactin 910 Violet 2 0 X 45 19MM SKKI Round Body" : "Endo",
    "polyglactin 910 violet" : "Endo",
    "polyglactin 910 undyed" : "Endo",
    "ne polyglactin 910 undyed" : "Endo",
    "ne polyglactin 910 violet" : "Endo",
    "vb polyglactin 910 violet" : "Endo",
    "polyglactin 910 c plus violet" : "Endo",
    "polyglactin 910 c plus undyed" : "Endo",
    "ih polyglactin 910 with triclosan violet" : "Endo",
    "polyglactin 910 violet c plus" : "Endo",
    "polyglactin 910 c plus violet tn" : "Endo",
    "polyglactin 910 c plus violet th" : "Endo",
    "polyglactin 910 c plus violet ih" : "Endo",
    "polyglactin 910 c plus undyed tn" : "Endo",
    "polyglactin 910 c plus undyed th" : "Endo",
    "IH Polyglactin 910  With triclosan Undyed" : "Endo",
    "POLYGLACTIN 910  C PLUS UNDYED" : "Endo",
    "NE Polyglactin 910  With triclosan Violet" : "Endo",
    "POLYGLACTIN 910  WITH TRICLOSAN UNDYED" : "Endo",
    "BARB Polyglycolic Acid  Poly" : "Endo",
    "barb polyglycolic acid" : "Endo",
    "poly glycolide co caprolactone undyed" : "Endo",
    "Barbed sutures PDS violet" : "Endo",
    "Unidirectional barbed sutures" : "Endo",
    "Bidirectional barbed sutures" : "Endo",
    "1/2 circle round body needle" : "Endo",
    "3/8 circle cutting needle" : "Endo",
    "Reverse cutting needle" : "Endo",
    "Taper cut needle" : "Endo",
    "Double needle sutures" : "Endo",
    "Loop polypropylene sutures" : "Endo",
    "Polypropylene blue surgical suture" : "Endo",
    "HCRB, HCTC, TP double‑needle sutures" : "Endo",
    "Polymer surgical clips" : "Endo",
    "Medium / large / extra‑large polymer clips" : "Endo",
    "Polypropylene 3D hernia mesh" : "Endo",
    "Inguinal anatomical mesh" : "Endo",
    "EO sterilized mesh" : "Endo",
    "Large‑pore polypropylene mesh" : "Endo",
    "Polypropylene suture" : "Endo",
    "Polypropylene mesh" : "Endo",
    "Non-absorbable suture" : "Endo",
    "Surgical suture" : "Endo",
    "Double needle suture" : "Endo",
    "Round body needle" : "Endo",
    "Reverse cutting needle" : "Endo",
    "Taper cut needle" : "Endo",
    "Elixir needle" : "Endo",
    "V‑black needle" : "Endo",
    "HCRB needle" : "Endo",
    "CURB needle" : "Endo",
    "DVB needle" : "Endo",
    "Surgical mesh" : "Endo",
    "Polypropylene macroporous mesh" : "Endo",
    "Lightweight mesh" : "Endo",
    "Heavyweight mesh" : "Endo",
    "Laparoscopic clip applicator" : "Endo",
    "Polymer clip applicator" : "Endo",
    "Open clip applicator" : "Endo",
    "Polypropylene 5‑0 suture" : "Endo",
    "Polypropylene 6‑0 suture" : "Endo",
    "Polypropylene 7‑0 suture" : "Endo",
    "Polypropylene 8‑0 suture" : "Endo",
    "3/8 circle needle" : "Endo",
    "1/2 circle needle" : "Endo",
    "45mm – 90mm needle" : "Endo",
    "6mm – 17mm needle sizes" : "Endo",
    "Round body needle" : "Endo",
    "Reverse cutting needle" : "Endo",
    "Cutting needle" : "Endo",
    "Taper point needle" : "Endo",
    "Taper cut needle" : "Endo",
    "Easy glide needle" : "Endo",
    "Black needle" : "Endo",
    "V‑black needle" : "Endo",
    "Elixir needle" : "Endo",
    "Polypropylene mesh 10×15 cm" : "Endo",
    "Polypropylene mesh 12×15 cm" : "Endo",
    "Polypropylene mesh 12×18 cm" : "Endo",
    "Polypropylene mesh 15×15 cm" : "Endo",
    "Polypropylene mesh 15×20 cm" : "Endo",
    "Polypropylene mesh 15×30 cm" : "Endo",
    "Polypropylene mesh 30×30 cm" : "Endo",
    "Polypropylene mesh 50×50 cm" : "Endo",
    "Macroporous lightweight mesh" : "Endo",
    "Soft polypropylene mesh" : "Endo",
    "Polymer clip laparoscopic applicator" : "Endo",
    "Medium–large clip applicator" : "Endo",
    "Large clip applicator" : "Endo",
    "Extra-large clip applicator" : "Endo",
    "Open clip applicator 30°" : "Endo",
    "Laparoscopic surgical instruments" : "Endo",
    "Sterilized polypropylene" : "Endo",
    "Ethylene oxide sterilized" : "Endo",
    "Macroporous structure" : "Endo",
    "100 gsm mesh" : "Endo",
    "1.0 × 1.2 mm pore size" : "Endo",
    "0.48 mm thickness" : "Endo",
    "106.3 N/cm² burst strength" : "Endo",
    "polypropylene blue 6 0 x 70 13mm" : "Endo",
    "polypropylene bu 5 0 x 70 12mm" : "Endo",
    "polypropylene blue 6 0 x 60 13mm" : "Endo",
    "polypropylene bu 5 0 x 90 16mm hcrb" : "Endo",
    "polypropylene blue 6 0 x 60 10mm" : "Endo",
    "polypropylene bu 5 0 x 90 16mm hcrb" : "Endo",
    "polypropylene blue 6 0 x 70 15mm" : "Endo",
    "polypropylene bu 5 0 x 90 16mm hcrb" : "Endo",
    "polypropylene blue 6 0 x 70 13mm" : "Endo",
    "polypropylene bu 5 0 x 70 13mm" : "Endo",
    "Polypropylene mesh" : "Endo",
    "Synthetic Non-absorbable Polypropylene Macroporous Light Weight Mesh" : "Endo",
    "Polypropylene mesh soft" : "Endo",
    "POLYMER CLIP LAPROSCOPIC APLICATOR for medium large clips" : "Endo",
    "silk black" : "Endo",
    "plain catgut" : "Endo",
    "metal skin stapler" : "Endo",
    "v shape ligation clip" : "Endo",
    "Laser Ablation System Kit" : "Endo",

    
}

# ----------------- utility helpers -----------------
def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = re.sub(r"[\u200B-\u200F\u202A-\u202E\u00A0]", " ", s)
    s = s.replace("\n", " ").replace("\r", " ")
    s = s.strip()
    return s

def norm_token_list(s: str):
    s = normalize_text(s).lower()
    tokens = re.findall(r"[a-z0-9]+", s)
    return [t for t in tokens if len(t) > 2]

def token_set(s: str):
    return set(norm_token_list(s))

def token_overlap(query: str, target: str) -> float:
    q = token_set(query)
    t = token_set(target)
    if not q:
        return 0.0
    return len(q & t) / len(q)

def detect_category_from_query(q: str, index_items=None):
    ql = normalize_text(q).lower()

    hits = []
    for kw, cat in CATEGORY_KEYWORDS.items():
        if kw in ql:
            hits.append((len(kw), kw, cat))

    if hits:
        hits.sort(reverse=True)
        return hits[0][2]

    if index_items:
        q_tokens = token_set(q)
        best = None
        best_score = 0
        for it in index_items:
            combined = " ".join([
                it.get("title") or "",
                it.get("category") or "",
                it.get("type") or "",
                it.get("merged_text") or ""
            ])
            score = len(q_tokens & token_set(combined))
            if score > best_score:
                best_score = score
                best = it.get("category") or it.get("type")
        if best_score > 0:
            return best

    return None

def safe_product_code(item):
    candidates = []
    for k in ("product_code", "product code", "productcode", "code", "product"):
        v = item.get(k)
        if v:
            candidates.append(str(v).strip())

    v0 = str(item.get("product_code") or "").strip()
    if v0:
        candidates.insert(0, v0)

    for c in candidates:
        if re.search(r"[A-Za-z]", c) and re.search(r"[0-9]", c):
            return c

    for c in candidates:
        low = c.lower()
        if low in ("regular", "no slab") or low.startswith("slab"):
            continue
        if c:
            return c
    return ""

def sanitize_match(raw: dict) -> dict:
    """Ensure match dict has expected keys and Python-native numeric types."""
    if not raw:
        return {
            "index": None,
            "product_code": "",
            "title": "",
            "type": "",
            "category": "",
            "specification": "",
            "emb_score": 0.0,
            "token_score": 0.0,
            "title_overlap": 0.0,
            "raw_score": 0.0,
            "relevancy": 0.0
        }
    out = {}
    out["index"] = int(raw.get("index")) if raw.get("index") is not None else None
    out["product_code"] = str(raw.get("product_code") or "") 
    out["title"] = str(raw.get("title") or "")
    out["type"] = str(raw.get("type") or "")
    out["category"] = str(raw.get("category") or "")
    out["specification"] = str(raw.get("specification") or raw.get("spec") or raw.get("specification_text") or "")
    out["emb_score"] = float(raw.get("emb_score") or raw.get("emb") or 0.0)
    out["token_score"] = float(raw.get("token_score") or raw.get("token") or 0.0)
    out["title_overlap"] = float(raw.get("title_overlap") or raw.get("title_tok") or 0.0)
    out["raw_score"] = float(raw.get("raw_score") or 0.0)
    out["relevancy"] = float(raw.get("relevancy") or raw.get("relevancy_score") or raw.get("relevancy_local") or 0.0)
    return out

# ----------------- query splitting logic -----------------
def split_multi_query(query: str) -> List[str]:
    """
    Split a complex query into individual product queries.
    Handles various separators: commas, semicolons, newlines, 'and', numbered lists
    
    Examples:
    - "product1, product2, product3"
    - "1. product1 2. product2"
    - "supply of - product1, product2"
    """
    # Normalize the query first
    query = normalize_text(query)
    
    # Remove common prefixes like "supply of -", "requirement of", etc.
    prefixes_to_remove = [
        r"^supply\s+of\s*[-:]*\s*",
        r"^requirement\s+of\s*[-:]*\s*",
        r"^procurement\s+of\s*[-:]*\s*",
        r"^purchase\s+of\s*[-:]*\s*",
        r"^quotation\s+for\s*[-:]*\s*",
    ]
    
    for prefix_pattern in prefixes_to_remove:
        query = re.sub(prefix_pattern, "", query, flags=re.IGNORECASE)
    
    # Split by common delimiters: comma, semicolon, newline, pipe
    # Also split by numbered lists like "1.", "2)", etc.
    parts = re.split(r'[,;|\n]|\d+[\.)]\s*', query)
    
    # Clean up each part
    queries = []
    for part in parts:
        part = part.strip()
        
        # Skip empty parts or very short parts (likely noise)
        if len(part) < 3:
            continue
            
        # Remove leading/trailing dashes, colons, etc.
        part = re.sub(r'^[-:•\s]+|[-:•\s]+$', '', part)
        
        # Skip if still too short after cleaning
        if len(part) < 5:
            continue
        
        # Remove trailing location/identifier patterns like "- gmc jagdalpur equipments"
        part = re.sub(r'\s*[-–]\s*[a-z\s]+equipments?\s*$', '', part, flags=re.IGNORECASE)
        part = re.sub(r'\s*[-–]\s*[a-z\s]+hospital\s*$', '', part, flags=re.IGNORECASE)
        
        queries.append(part.strip())
    
    # If no queries were extracted (maybe single query), return original
    if not queries:
        return [query.strip()]
    
    return queries

# ----------------- load global index & embeddings -----------------
print("Loading index and embeddings...")
with open(INDEX_PATH, "r", encoding="utf-8") as f:
    INDEX_RAW = json.load(f)

INDEX = []
for it in INDEX_RAW:
    title = normalize_text(it.get("title") or it.get("Title") or "")
    prod = safe_product_code(it)
    spec = it.get("specification") or it.get("spec") or it.get("specification_text") or ""
    spec = normalize_text(spec)

    if "SLABS" in spec:
        if "kit_price" not in spec:
            spec += " kit_price: —"
        if "test_price" not in spec:
            spec += " test_price: —"

    merged = normalize_text(it.get("merged_text") or it.get("mergedText") or title or spec)

    item = {
        "index": int(it.get("index")) if it.get("index") not in (None, "") else None,
        "product_code": prod,
        "title": title,
        "type": normalize_text(it.get("type") or it.get("Type") or it.get("category") or ""),
        "category": normalize_text(it.get("category") or it.get("Category") or ""),
        "specification": spec,
        "merged_text": merged,
    }
    INDEX.append(item)

EMB = np.load(EMB_PATH)
MODEL = SentenceTransformer(MODEL_NAME)

# ------------------------------------------------------------------
#                  SINGLE QUERY PREDICT FUNCTION
# ------------------------------------------------------------------
def predict_single(query: str, top_k: int = TOP_K) -> Dict[str, Any]:
    """Process a single query and return results"""
    query = normalize_text(query)
    detected_category = detect_category_from_query(query, INDEX)

    # -------------- route to analyser model if detected ----------------
    if detected_category and detected_category.lower() == "analyser" and HAS_ANALYSER_MODEL:
        try:
            print(f"  → Routing to analyser_relevancy.py")
            r = analyser_predict(query, top_k=top_k)
            best = sanitize_match(r.get("best_match") if isinstance(r.get("best_match"), dict) else r.get("best_match") or {})
            top_matches = [sanitize_match(m) for m in (r.get("top_matches") or [])]
            return {
                "query": query,
                "detected_category": "Analyser",
                "relevancy_score": float(r.get("relevancy_score") or r.get("relevancy") or 0.0),
                "relevant": bool(r.get("relevant") or False),
                "best_match": best,
                "top_matches": top_matches,
                "model_used": "analyser_relevancy"
            }
        except Exception as e:
            print(f"  → Error running analyser_relevancy: {e}")

    # -------------- route to endo model if detected ----------------
    if detected_category and detected_category.lower() == "endo" and HAS_ENDO_MODEL:
        try:
            print(f"  → Routing to endo_relevancy.py")
            r = predict_endo(query, top_k=top_k)
            best = sanitize_match(r.get("best_match") or {})
            top_matches = [sanitize_match(m) for m in (r.get("top_matches") or [])]
            return {
                "query": query,
                "detected_category": "Endo",
                "relevancy_score": float(r.get("relevancy_score") or r.get("relevancy") or best.get("relevancy") or 0.0),
                "relevant": bool(r.get("relevant") or False),
                "best_match": best,
                "top_matches": top_matches,
                "model_used": "endo_relevancy"
            }
        except Exception as e:
            print(f"  → Error running endo_relevancy: {e}")

    # -------------------- global fallback ----------------------------
    q_emb = MODEL.encode([query], normalize_embeddings=True)[0]
    sims = np.dot(EMB, q_emb)

    results = []
    q_lower = query.lower()

    for i, item in enumerate(INDEX):
        emb_score = float(sims[i]) if i < len(sims) else 0.0
        tok = float(token_overlap(query, item.get("merged_text", "")))
        title_tok = float(token_overlap(query, item.get("title", "")))

        raw = EMB_WEIGHT * emb_score + TOKEN_WEIGHT * tok + TITLE_WEIGHT * title_tok

        if detected_category:
            item_cat = (item.get("category") or "").lower()
            item_type = (item.get("type") or "").lower()
            if detected_category.lower() in item_cat or detected_category.lower() in item_type:
                raw += CATEGORY_BOOST

        pc = (item.get("product_code") or "").lower()
        if pc and re.search(r"\b" + re.escape(pc) + r"\b", q_lower):
            raw += 0.5

        match = {
            "index": int(item.get("index")) if item.get("index") is not None else int(i),
            "product_code": item.get("product_code") or "",
            "title": item.get("title"),
            "type": item.get("type"),
            "category": item.get("category"),
            "specification": item.get("specification"),
            "emb_score": float(emb_score),
            "token_score": float(tok),
            "title_overlap": float(title_tok),
            "raw_score": float(raw),
            "relevancy": float(1.0 / (1.0 + math.exp(-raw)))
        }
        results.append(match)

    results.sort(key=lambda x: x["raw_score"], reverse=True)

    top = results[:top_k]
    best = top[0] if top else None
    final_score = float(best["relevancy"]) if best else 0.0

    return {
        "query": query,
        "detected_category": detected_category,
        "relevancy_score": final_score,
        "relevant": bool(final_score >= 0.5),
        "best_match": best or sanitize_match({}),
        "top_matches": top,
        "model_used": "global_index"
    }

# ------------------------------------------------------------------
#                  MULTI-QUERY PREDICT FUNCTION
# ------------------------------------------------------------------
def predict(query: str, top_k: int = TOP_K, return_individual: bool = True) -> Dict[str, Any]:
    """
    Main prediction function that handles both single and multi-query inputs.
    
    Args:
        query: Input query string (can contain multiple queries separated by delimiters)
        top_k: Number of top results to return per query
        return_individual: If True, returns detailed results for each query separately
    
    Returns:
        Dictionary containing:
        - is_multi_query: Whether multiple queries were detected
        - query_count: Number of individual queries found
        - results: List of results (one per query if multi-query)
        - summary: Overall summary statistics
    """
    # Split the query into individual queries
    individual_queries = split_multi_query(query)
    
    is_multi = len(individual_queries) > 1
    
    print(f"\n{'='*70}")
    print(f"Processing {'MULTI-QUERY' if is_multi else 'SINGLE QUERY'} input")
    print(f"Found {len(individual_queries)} individual {'queries' if is_multi else 'query'}")
    print(f"{'='*70}\n")
    
    all_results = []
    
    for idx, single_query in enumerate(individual_queries, 1):
        print(f"[Query {idx}/{len(individual_queries)}]: {single_query}")
        
        result = predict_single(single_query, top_k=top_k)
        result["query_number"] = idx
        all_results.append(result)
        
        # Print quick summary
        if result.get("best_match"):
            best = result["best_match"]
            print(f"  ✓ Best: {best.get('title', 'N/A')} (relevancy: {result['relevancy_score']:.3f})")
        else:
            print(f"  ✗ No match found")
        print()
    
    # Compute summary statistics
    relevant_count = sum(1 for r in all_results if r.get("relevant"))
    avg_relevancy = sum(r.get("relevancy_score", 0) for r in all_results) / len(all_results) if all_results else 0
    
    summary = {
        "total_queries": len(individual_queries),
        "relevant_matches": relevant_count,
        "irrelevant_matches": len(all_results) - relevant_count,
        "average_relevancy": float(avg_relevancy),
        "success_rate": float(relevant_count / len(all_results)) if all_results else 0.0
    }
    
    response = {
        "is_multi_query": is_multi,
        "original_query": query,
        "query_count": len(individual_queries),
        "individual_queries": individual_queries,
        "results": all_results,
        "summary": summary
    }
    
    return response

# ------------------------------------------------------------------
#                  BATCH PROCESSING FUNCTION
# ------------------------------------------------------------------
def predict_batch(queries: List[str], top_k: int = TOP_K) -> List[Dict[str, Any]]:
    """
    Process multiple queries in batch mode.
    Each query can itself contain multiple sub-queries.
    
    Args:
        queries: List of query strings
        top_k: Number of top results per query
    
    Returns:
        List of prediction results
    """
    results = []
    for query in queries:
        result = predict(query, top_k=top_k)
        results.append(result)
    return results

# ------------------------------------------------------------------
#                  OUTPUT FORMATTING
# ------------------------------------------------------------------
def format_output(result: Dict[str, Any], verbose: bool = False) -> str:
    """Format the result for console output"""
    lines = []
    lines.append("\n" + "="*70)
    lines.append("MULTI-QUERY RELEVANCY SEARCH RESULTS")
    lines.append("="*70)
    
    if result.get("is_multi_query"):
        lines.append(f"\n📋 Original Query: {result['original_query'][:100]}...")
        lines.append(f"🔍 Detected {result['query_count']} individual queries\n")
        
        for idx, query_result in enumerate(result["results"], 1):
            lines.append(f"\n{'─'*70}")
            lines.append(f"Query {idx}: {query_result['query']}")
            lines.append(f"{'─'*70}")
            
            if query_result.get("detected_category"):
                lines.append(f"Category: {query_result['detected_category']}")
            
            best = query_result.get("best_match")
            if best and best.get("title"):
                lines.append(f"\n✓ BEST MATCH:")
                lines.append(f"  Product Code: {best.get('product_code', 'N/A')}")
                lines.append(f"  Title: {best['title']}")
                lines.append(f"  Category: {best.get('category', 'N/A')}")
                lines.append(f"  Type: {best.get('type', 'N/A')}")
                lines.append(f"  Relevancy: {query_result['relevancy_score']:.3f}")
                
                if verbose and best.get("specification"):
                    lines.append(f"  Specification: {best['specification'][:200]}...")
            else:
                lines.append(f"\n✗ NO MATCH FOUND")
            
            if verbose and query_result.get("top_matches"):
                lines.append(f"\n  Other top matches:")
                for i, match in enumerate(query_result["top_matches"][1:4], 2):
                    lines.append(f"    {i}. {match.get('title', 'N/A')} (rel: {match.get('relevancy', 0):.3f})")
        
        lines.append(f"\n{'='*70}")
        lines.append("SUMMARY")
        lines.append(f"{'='*70}")
        summary = result["summary"]
        lines.append(f"Total Queries: {summary['total_queries']}")
        lines.append(f"Relevant Matches: {summary['relevant_matches']}")
        lines.append(f"Success Rate: {summary['success_rate']*100:.1f}%")
        lines.append(f"Average Relevancy: {summary['average_relevancy']:.3f}")
        
    else:
        # Single query output
        query_result = result["results"][0]
        lines.append(f"\nQuery: {query_result['query']}")
        
        if query_result.get("detected_category"):
            lines.append(f"Category: {query_result['detected_category']}")
        
        best = query_result.get("best_match")
        if best and best.get("title"):
            lines.append(f"\n✓ BEST MATCH:")
            lines.append(f"  Product Code: {best.get('product_code', 'N/A')}")
            lines.append(f"  Title: {best['title']}")
            lines.append(f"  Category: {best.get('category', 'N/A')}")
            lines.append(f"  Relevancy: {query_result['relevancy_score']:.3f}")
        else:
            lines.append(f"\n✗ NO MATCH FOUND")
    
    lines.append("="*70 + "\n")
    return "\n".join(lines)

# -------------------- CLI --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Global relevancy search with multi-query support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Single query:
    python global_relevancy.py "5 part hematology analyser"
  
  Multi-query (comma-separated):
    python global_relevancy.py "5 part analyser, laparoscope, microscope"
  
  Complex multi-query:
    python global_relevancy.py "supply of - 5 part analyser, laparoscope, microscope - hospital"
  
  With options:
    python global_relevancy.py "analyser, microscope" --top 3 --verbose
    python global_relevancy.py "query" --json
        """
    )
    parser.add_argument("query", nargs="*", help="Query text (supports multiple queries)")
    parser.add_argument("--top", type=int, default=5, help="Top K results per query")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--batch", type=str, help="Path to file with queries (one per line)")
    
    args = parser.parse_args()
    
    if args.batch:
        # Batch mode from file
        with open(args.batch, 'r', encoding='utf-8') as f:
            queries = [line.strip() for line in f if line.strip()]
        
        results = predict_batch(queries, top_k=args.top)
        
        if args.json:
            print(json.dumps(results, indent=2, ensure_ascii=False))
        else:
            for result in results:
                print(format_output(result, verbose=args.verbose))
    else:
        # Single/multi query mode
        q = " ".join(args.query) if args.query else "5 Part Automated Hematology Analyser (V2) (Q2) , Operating Laparoscope with instrumentation , HD Endoscopy system , Diagnostic clinical Audiometer , Eclampsia Cot , Fully Automatic Rotary Microtome , Binocular Microscope , Monocular Version Compound Microscope, Polyglactine 910 Polyglycolic Acid"
        
        res = predict(q, top_k=args.top)
        
        if args.json:
            print(json.dumps(res, indent=2, ensure_ascii=False))
        else:
            print(format_output(res, verbose=args.verbose))