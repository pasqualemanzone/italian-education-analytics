"""
ETL Pipeline - Italian Education System Database
================================================
Script per caricare dati CSV in SQL Server

Autore: Pasquale Manzone
Data: Febbraio 2025
Server: PAKO-PC\SQLEXPRESS
Database: ItalianEducationDB
"""

import pandas as pd
import pyodbc
import os
from pathlib import Path

# ============================================
# CONFIGURAZIONE
# ============================================

SERVER = r'PAKO-PC\SQLEXPRESS'
DATABASE = 'ItalianEducationDB'
CSV_PATH = r'C:\Users\pasqu\Desktop\Data Analyst\Progetto scuole\CODE\csv'

# Connection string per Windows Authentication
CONNECTION_STRING = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'

print("="*80)
print("ETL PIPELINE - ITALIAN EDUCATION SYSTEM DATABASE")
print("="*80)
print(f"Server: {SERVER}")
print(f"Database: {DATABASE}")
print(f"CSV Path: {CSV_PATH}")
print("="*80)
print()

# ============================================
# FUNZIONI UTILITY
# ============================================

def get_area_geografica(regione):
    """Mappa regione → area geografica"""
    if pd.isna(regione):
        return 'Non specificato'
    
    nord = ['PIEMONTE', 'VALLE D\'AOSTA', 'VALLE DAOSTA', 'LOMBARDIA', 
            'TRENTINO-ALTO ADIGE', 'TRENTINO ALTO ADIGE', 'VENETO', 
            'FRIULI-VENEZIA GIULIA', 'FRIULI VENEZIA GIULIA', 'LIGURIA', 
            'EMILIA-ROMAGNA', 'EMILIA ROMAGNA']
    centro = ['TOSCANA', 'UMBRIA', 'MARCHE', 'LAZIO']
    sud = ['ABRUZZO', 'MOLISE', 'CAMPANIA', 'PUGLIA', 'BASILICATA', 'CALABRIA']
    isole = ['SICILIA', 'SARDEGNA']
    
    regione_upper = str(regione).upper().strip()
    
    if regione_upper in nord:
        return 'Nord'
    elif regione_upper in centro:
        return 'Centro'
    elif regione_upper in sud:
        return 'Sud'
    elif regione_upper in isole:
        return 'Isole'
    else:
        return 'Non specificato'

# ============================================
# STEP 1: CONNESSIONE DATABASE
# ============================================

print("\n[STEP 1] Connessione a SQL Server...")
try:
    conn = pyodbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    print("✓ Connessione stabilita con successo")
except Exception as e:
    print(f"✗ ERRORE connessione: {e}")
    print("\nVerifica:")
    print("1. SQL Server sia avviato")
    print("2. Database 'ItalianEducationDB' esista")
    print("3. Schema SQL sia stato eseguito")
    exit(1)

# ============================================
# STEP 2: VERIFICA FILE CSV
# ============================================

print("\n[STEP 2] Verifica file CSV...")

required_files = [
    'personale_cleaned.csv',
    'alunni_cleaned.csv',
    'anagrafica_scuole_completa.csv'
]

for file in required_files:
    file_path = os.path.join(CSV_PATH, file)
    if os.path.exists(file_path):
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        print(f"  ✓ {file} ({size_mb:.2f} MB)")
    else:
        print(f"  ✗ {file} NON TROVATO!")
        print(f"     Percorso cercato: {file_path}")
        exit(1)

print("✓ Tutti i file CSV trovati")

# ============================================
# STEP 3: CARICAMENTO CSV
# ============================================

print("\n[STEP 3] Caricamento CSV in memoria...")

# 3.1 Personale
print("  - Caricamento personale_cleaned.csv...")
df_personale = pd.read_csv(os.path.join(CSV_PATH, 'personale_cleaned.csv'))
print(f"    → {len(df_personale):,} righe")

# 3.2 Alunni
print("  - Caricamento alunni_cleaned.csv...")
df_alunni = pd.read_csv(os.path.join(CSV_PATH, 'alunni_cleaned.csv'))
print(f"    → {len(df_alunni):,} righe")

# 3.3 Anagrafica Scuole (completa con dummy)
print("  - Caricamento anagrafica_scuole_completa.csv...")
df_scuole = pd.read_csv(os.path.join(CSV_PATH, 'anagrafica_scuole_completa.csv'))
print(f"    → {len(df_scuole):,} righe")

print("✓ Tutti i CSV caricati in memoria")

# ============================================
# STEP 4: POPOLAMENTO dim_tempo
# ============================================

print("\n[STEP 4] Popolamento dim_tempo...")

# Estrai anni unici da Personale e Alunni
anni_personale = df_personale['ANNOSCOLASTICO'].unique() if 'ANNOSCOLASTICO' in df_personale.columns else []
anni_alunni = df_alunni['ANNOSCOLASTICO'].unique() if 'ANNOSCOLASTICO' in df_alunni.columns else []
anni_unici = sorted(set(list(anni_personale) + list(anni_alunni)))

count_tempo = 0
for anno in anni_unici:
    anno_int = int(anno)
    anno_inizio = int(str(anno)[:4])
    anno_fine = anno_inizio + 1
    
    try:
        # Check se esiste già
        cursor.execute("SELECT COUNT(*) FROM dim_tempo WHERE anno_scolastico = ?", anno_int)
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO dim_tempo (anno_scolastico, anno_inizio, anno_fine)
                VALUES (?, ?, ?)
            """, anno_int, anno_inizio, anno_fine)
            count_tempo += 1
    except Exception as e:
        print(f"    Errore anno {anno}: {e}")

conn.commit()
print(f"  ✓ {count_tempo} anni inseriti in dim_tempo")

# ============================================
# STEP 5: POPOLAMENTO dim_province
# ============================================

print("\n[STEP 5] Popolamento dim_province...")

# Estrai province uniche da Personale
province_personale = df_personale[['PROVINCIA']].drop_duplicates() if 'PROVINCIA' in df_personale.columns else pd.DataFrame()

# Mappa provincia → regione da Anagrafica Scuole
provincia_regione = df_scuole[['PROVINCIA', 'REGIONE']].drop_duplicates() if 'PROVINCIA' in df_scuole.columns and 'REGIONE' in df_scuole.columns else pd.DataFrame()

# Merge per avere provincia + regione
if not province_personale.empty and not provincia_regione.empty:
    province_complete = province_personale.merge(provincia_regione, on='PROVINCIA', how='left')
else:
    province_complete = province_personale

count_province = 0
for _, row in province_complete.iterrows():
    prov = row['PROVINCIA']
    reg = row.get('REGIONE', 'Non specificata') if pd.notna(row.get('REGIONE')) else 'Non specificata'
    area = get_area_geografica(reg)
    
    try:
        # Check se esiste già
        cursor.execute("SELECT COUNT(*) FROM dim_province WHERE provincia = ?", prov)
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO dim_province (provincia, regione, area_geografica)
                VALUES (?, ?, ?)
            """, prov, reg, area)
            count_province += 1
    except Exception as e:
        print(f"    Errore provincia {prov}: {e}")

conn.commit()
print(f"  ✓ {count_province} province inserite in dim_province")

# ============================================
# STEP 6: POPOLAMENTO dim_scuole
# ============================================

print("\n[STEP 6] Popolamento dim_scuole...")
print("  (Questa operazione richiederà qualche minuto per ~53K scuole...)")

# Prepara colonne per insert
colonne_scuole = [
    'CODICESCUOLA', 'DENOMINAZIONESCUOLA', 'CODICEISTITUTORIFERIMENTO',
    'DENOMINAZIONEISTITUTORIFERIMENTO', 'INDIRIZZOSCUOLA', 'CAPSCUOLA',
    'DESCRIZIONECOMUNE', 'PROVINCIA', 'REGIONE', 'AREAGEOGRAFICA',
    'INDIRIZZOEMAILSCUOLA', 'INDIRIZZOPECSCUOLA', 'SITOWEBSCUOLA',
    'DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA', 'DESCRIZIONECARATTERISTICASCUOLA',
    'INDICAZIONESEDEDIRETTIVO', 'INDICAZIONESEDEOMNICOMPRENSIVO', 'SEDESCOLASTICA'
]

# Verifica quali colonne esistono realmente nel CSV
colonne_disponibili = [col for col in colonne_scuole if col in df_scuole.columns]

# Aggiungi area geografica se non presente
if 'AREAGEOGRAFICA' not in df_scuole.columns and 'REGIONE' in df_scuole.columns:
    df_scuole['AREAGEOGRAFICA'] = df_scuole['REGIONE'].apply(get_area_geografica)
    colonne_disponibili.append('AREAGEOGRAFICA')

# Flag scuola attiva (se presente nel CSV)
if 'SCUOLA_ATTIVA_2025' in df_scuole.columns:
    colonne_disponibili.append('SCUOLA_ATTIVA_2025')

# Rimuovi duplicati per codice scuola
df_scuole_insert = df_scuole[colonne_disponibili].drop_duplicates(subset=['CODICESCUOLA'])

count_scuole = 0
batch_size = 1000
total_scuole = len(df_scuole_insert)

for i in range(0, total_scuole, batch_size):
    batch = df_scuole_insert.iloc[i:i+batch_size]
    
    for _, row in batch.iterrows():
        try:
            # Check se esiste già
            cursor.execute("SELECT COUNT(*) FROM dim_scuole WHERE codice_scuola = ?", row['CODICESCUOLA'])
            if cursor.fetchone()[0] == 0:
                # Prepara valori (None per colonne mancanti)
                valori = []
                for col in ['CODICESCUOLA', 'DENOMINAZIONESCUOLA', 'CODICEISTITUTORIFERIMENTO',
                           'DENOMINAZIONEISTITUTORIFERIMENTO', 'INDIRIZZOSCUOLA', 'CAPSCUOLA',
                           'DESCRIZIONECOMUNE', 'PROVINCIA', 'REGIONE', 'AREAGEOGRAFICA',
                           'INDIRIZZOEMAILSCUOLA', 'INDIRIZZOPECSCUOLA', 'SITOWEBSCUOLA',
                           'DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA', 'DESCRIZIONECARATTERISTICASCUOLA',
                           'INDICAZIONESEDEDIRETTIVO', 'INDICAZIONESEDEOMNICOMPRENSIVO', 'SEDESCOLASTICA']:
                    valori.append(row.get(col) if pd.notna(row.get(col)) else None)
                
                # Flag scuola attiva
                flag_attiva = row.get('SCUOLA_ATTIVA_2025', 'SI') if pd.notna(row.get('SCUOLA_ATTIVA_2025')) else 'SI'
                valori.append(flag_attiva)
                
                cursor.execute("""
                    INSERT INTO dim_scuole (
                        codice_scuola, denominazione_scuola, codice_istituto_riferimento,
                        denominazione_istituto, indirizzo, cap, comune, provincia, regione,
                        area_geografica, email, pec, sito_web, tipologia_grado,
                        caratteristica_scuola, sede_direttivo, sede_omnicomprensivo, 
                        sede_scolastica, scuola_attiva_2025
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, *valori)
                count_scuole += 1
        except Exception as e:
            print(f"    Errore scuola {row['CODICESCUOLA']}: {e}")
            continue
    
    conn.commit()
    print(f"  → {min(i+batch_size, total_scuole):,}/{total_scuole:,} scuole processate...")

print(f"  ✓ {count_scuole:,} scuole inserite in dim_scuole")

# ============================================
# STEP 7: POPOLAMENTO fact_personale
# ============================================

print("\n[STEP 7] Popolamento fact_personale...")

count_personale = 0
for _, row in df_personale.iterrows():
    try:
        cursor.execute("""
            INSERT INTO fact_personale (
                anno_scolastico, provincia, ordine_scuola, tipo_posto,
                fascia_eta, docenti_maschi, docenti_femmine
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        int(row['ANNOSCOLASTICO']),
        row['PROVINCIA'],
        row['ORDINESCUOLA'],
        row['TIPOPOSTO'],
        row['FASCIAETA'],
        int(row['DOCENTITITOLARIMASCHI']),
        int(row['DOCENTITITOLARIFEMMINE'])
        )
        count_personale += 1
    except Exception as e:
        print(f"    Errore riga personale: {e}")
        continue

conn.commit()
print(f"  ✓ {count_personale:,} righe inserite in fact_personale")

# ============================================
# STEP 8: POPOLAMENTO fact_alunni
# ============================================

print("\n[STEP 8] Popolamento fact_alunni...")
print("  (Questa operazione richiederà qualche minuto per ~305K righe...)")

count_alunni = 0
batch_size = 5000
total_alunni = len(df_alunni)

for i in range(0, total_alunni, batch_size):
    batch = df_alunni.iloc[i:i+batch_size]
    
    for _, row in batch.iterrows():
        try:
            # Verifica che la scuola esista in dim_scuole
            cursor.execute("SELECT COUNT(*) FROM dim_scuole WHERE codice_scuola = ?", row['CODICESCUOLA'])
            if cursor.fetchone()[0] > 0:
                cursor.execute("""
                    INSERT INTO fact_alunni (
                        anno_scolastico, codice_scuola, ordine_scuola,
                        anno_corso, fascia_eta, numero_alunni
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                int(row['ANNOSCOLASTICO']),
                row['CODICESCUOLA'],
                row['ORDINESCUOLA'],
                int(row['ANNOCORSO']) if pd.notna(row['ANNOCORSO']) else None,
                row['FASCIAETA'] if pd.notna(row['FASCIAETA']) else None,
                int(row['ALUNNI'])
                )
                count_alunni += 1
        except Exception as e:
            print(f"    Errore riga alunni: {e}")
            continue
    
    conn.commit()
    print(f"  → {min(i+batch_size, total_alunni):,}/{total_alunni:,} alunni processati...")

print(f"  ✓ {count_alunni:,} righe inserite in fact_alunni")

# ============================================
# STEP 9: VERIFICA FINALE
# ============================================

print("\n[STEP 9] Verifica caricamento dati...")
print()

tables = [
    ('dim_tempo', 'anni scolastici'),
    ('dim_province', 'province'),
    ('dim_scuole', 'scuole'),
    ('fact_personale', 'righe personale docente'),
    ('fact_alunni', 'righe studenti')
]

print("RIEPILOGO DATABASE:")
print("-" * 60)
for table, descrizione in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  {table:20s} : {count:>8,} {descrizione}")
print("-" * 60)

# ============================================
# STEP 10: QUERY TEST
# ============================================

print("\n[STEP 10] Query di test...")
print()

# Test 1: Totale docenti per area geografica
print("TEST 1: Totale docenti per area geografica")
cursor.execute("""
    SELECT TOP 5
        p.area_geografica,
        SUM(fp.totale_docenti) as totale_docenti
    FROM fact_personale fp
    JOIN dim_province p ON fp.provincia = p.provincia
    GROUP BY p.area_geografica
    ORDER BY totale_docenti DESC
""")
for row in cursor.fetchall():
    print(f"  {row[0]:15s} : {row[1]:>8,} docenti")

print()

# Test 2: Totale studenti per ordine scuola
print("TEST 2: Totale studenti per ordine scuola")
cursor.execute("""
    SELECT TOP 5
        ordine_scuola,
        SUM(numero_alunni) as totale_studenti
    FROM fact_alunni
    GROUP BY ordine_scuola
    ORDER BY totale_studenti DESC
""")
for row in cursor.fetchall():
    print(f"  {row[0]:30s} : {row[1]:>8,} studenti")

print()

# Test 3: View test
print("TEST 3: View docenti per provincia (top 5)")
cursor.execute("""
    SELECT TOP 5
        provincia,
        totale_docenti,
        media_perc_femmine
    FROM view_docenti_per_provincia
    ORDER BY totale_docenti DESC
""")
for row in cursor.fetchall():
    print(f"  {row[0]:20s} : {row[1]:>6,} docenti ({row[2]:.1f}% femmine)")

# ============================================
# FINE
# ============================================

conn.close()

print()
print("="*80)
print("ETL COMPLETATO CON SUCCESSO! ✓")
print("="*80)
print()
print("PROSSIMO STEP:")
print("  → Apri Power BI Desktop")
print("  → Get Data → SQL Server")
print("  → Server: PAKO-PC\\SQLEXPRESS")
print("  → Database: ItalianEducationDB")
print("  → Importa tabelle e crea dashboard!")
print()
print("="*80)
