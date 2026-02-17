-- ============================================
-- ITALIAN EDUCATION SYSTEM DATABASE SCHEMA
-- SQL Server / T-SQL Version
-- Database: ItalianEducationDB
-- ============================================

USE ItalianEducationDB;
GO

-- DROP TABLES se esistono (per reset pulito)
IF OBJECT_ID('fact_importi_fse', 'U') IS NOT NULL DROP TABLE fact_importi_fse;
IF OBJECT_ID('fact_importi_fesr', 'U') IS NOT NULL DROP TABLE fact_importi_fesr;
IF OBJECT_ID('fact_progetti_eu', 'U') IS NOT NULL DROP TABLE fact_progetti_eu;
IF OBJECT_ID('fact_alunni', 'U') IS NOT NULL DROP TABLE fact_alunni;
IF OBJECT_ID('fact_personale', 'U') IS NOT NULL DROP TABLE fact_personale;
IF OBJECT_ID('dim_scuole', 'U') IS NOT NULL DROP TABLE dim_scuole;
IF OBJECT_ID('dim_tempo', 'U') IS NOT NULL DROP TABLE dim_tempo;
IF OBJECT_ID('dim_province', 'U') IS NOT NULL DROP TABLE dim_province;
GO

-- ============================================
-- DIMENSION TABLES (Tabelle di lookup)
-- ============================================

-- Tabella Province (dimensione geografica)
CREATE TABLE dim_province (
    id_provincia INT IDENTITY(1,1) PRIMARY KEY,
    provincia NVARCHAR(100) NOT NULL UNIQUE,
    regione NVARCHAR(100),
    area_geografica NVARCHAR(50), -- Nord, Centro, Sud, Isole
    codice_istat_provincia NVARCHAR(10)
);
GO

-- Tabella Scuole (dimensione scuole)
CREATE TABLE dim_scuole (
    id_scuola INT IDENTITY(1,1) PRIMARY KEY,
    codice_scuola NVARCHAR(20) NOT NULL UNIQUE,
    denominazione_scuola NVARCHAR(500),
    codice_istituto_riferimento NVARCHAR(20),
    denominazione_istituto NVARCHAR(500),
    indirizzo NVARCHAR(255),
    cap NVARCHAR(10),
    comune NVARCHAR(100),
    provincia NVARCHAR(100),
    regione NVARCHAR(100),
    area_geografica NVARCHAR(50),
    email NVARCHAR(255),
    pec NVARCHAR(255),
    sito_web NVARCHAR(255),
    tipologia_grado NVARCHAR(100),
    caratteristica_scuola NVARCHAR(255),
    sede_direttivo NVARCHAR(5),
    sede_omnicomprensivo NVARCHAR(5),
    sede_scolastica NVARCHAR(5)
);
GO

-- Tabella Tempo (dimensione temporale)
CREATE TABLE dim_tempo (
    id_tempo INT IDENTITY(1,1) PRIMARY KEY,
    anno_scolastico INT NOT NULL UNIQUE,
    anno_inizio INT,
    anno_fine INT
);
GO

-- ============================================
-- FACT TABLES (Tabelle dei fatti/metriche)
-- ============================================

-- Fatto: Personale Docente
CREATE TABLE fact_personale (
    id_personale INT IDENTITY(1,1) PRIMARY KEY,
    anno_scolastico INT NOT NULL,
    provincia NVARCHAR(100) NOT NULL,
    ordine_scuola NVARCHAR(100) NOT NULL,
    tipo_posto NVARCHAR(50) NOT NULL,
    fascia_eta NVARCHAR(50) NOT NULL,
    docenti_maschi INT DEFAULT 0,
    docenti_femmine INT DEFAULT 0,
    totale_docenti AS (docenti_maschi + docenti_femmine) PERSISTED,
    perc_femmine AS (
        CASE 
            WHEN (docenti_maschi + docenti_femmine) > 0 
            THEN ROUND((CAST(docenti_femmine AS FLOAT) / (docenti_maschi + docenti_femmine)) * 100, 2)
            ELSE 0 
        END
    ) PERSISTED,
    FOREIGN KEY (anno_scolastico) REFERENCES dim_tempo(anno_scolastico),
    FOREIGN KEY (provincia) REFERENCES dim_province(provincia)
);
GO

-- Fatto: Alunni
CREATE TABLE fact_alunni (
    id_alunni INT IDENTITY(1,1) PRIMARY KEY,
    anno_scolastico INT NOT NULL,
    codice_scuola NVARCHAR(20) NOT NULL,
    ordine_scuola NVARCHAR(100) NOT NULL,
    anno_corso INT,
    fascia_eta NVARCHAR(50),
    numero_alunni INT DEFAULT 0,
    FOREIGN KEY (anno_scolastico) REFERENCES dim_tempo(anno_scolastico),
    FOREIGN KEY (codice_scuola) REFERENCES dim_scuole(codice_scuola)
);
GO

-- Fatto: Progetti EU (FESR/FSE)
CREATE TABLE fact_progetti_eu (
    id_progetto INT IDENTITY(1,1) PRIMARY KEY,
    anno_avviso INT NOT NULL,
    codice_avviso INT,
    tipo_fondo NVARCHAR(20),
    codice_programma NVARCHAR(50),
    denominazione_programma NVARCHAR(500),
    codice_istituto_originario NVARCHAR(20),
    codice_istituto_attualizzato NVARCHAR(20),
    codice_candidatura INT,
    codice_progetto NVARCHAR(100),
    stato_candidatura NVARCHAR(100),
    importo_autorizzato DECIMAL(15, 2),
    importo_erogato DECIMAL(15, 2),
    importo_certificato DECIMAL(15, 2),
    importo_rendicontato_ue DECIMAL(15, 2),
    codice_area_territoriale NVARCHAR(20),
    descrizione_area_territoriale NVARCHAR(100),
    codice_istat_comune INT,
    perc_erogato AS (
        CASE 
            WHEN importo_autorizzato > 0 
            THEN ROUND((importo_erogato / importo_autorizzato) * 100, 2)
            ELSE 0 
        END
    ) PERSISTED,
    perc_certificato AS (
        CASE 
            WHEN importo_autorizzato > 0 
            THEN ROUND((importo_certificato / importo_autorizzato) * 100, 2)
            ELSE 0 
        END
    ) PERSISTED,
    FOREIGN KEY (codice_istituto_attualizzato) REFERENCES dim_scuole(codice_scuola)
);
GO

-- Fatto: Importi Certificati FESR
CREATE TABLE fact_importi_fesr (
    id_importo_fesr INT IDENTITY(1,1) PRIMARY KEY,
    codice_asse NVARCHAR(50),
    tipo_fondo NVARCHAR(20),
    codice_programma NVARCHAR(50),
    denominazione_programma NVARCHAR(500),
    codice_avviso INT,
    anno_avviso INT,
    codice_istituto NVARCHAR(20),
    codice_candidatura INT,
    descrizione_ciclo NVARCHAR(500),
    codice_area_territoriale NVARCHAR(20),
    descrizione_area_territoriale NVARCHAR(100),
    codice_istat_comune INT,
    codice_voce_costo NVARCHAR(50),
    descrizione_voce_costo NVARCHAR(500),
    importo_autorizzato DECIMAL(15, 2),
    importo_certificato DECIMAL(15, 2),
    importo_rimodulato DECIMAL(15, 2),
    FOREIGN KEY (codice_istituto) REFERENCES dim_scuole(codice_scuola)
);
GO

-- Fatto: Importi Certificati FSE
CREATE TABLE fact_importi_fse (
    id_importo_fse INT IDENTITY(1,1) PRIMARY KEY,
    codice_asse NVARCHAR(50),
    tipo_fondo NVARCHAR(20),
    codice_programma NVARCHAR(50),
    denominazione_programma NVARCHAR(500),
    codice_avviso INT,
    anno_avviso INT,
    codice_istituto NVARCHAR(20),
    codice_candidatura INT,
    descrizione_ciclo NVARCHAR(500),
    codice_area_territoriale NVARCHAR(20),
    descrizione_area_territoriale NVARCHAR(100),
    codice_istat_comune INT,
    codice_voce_costo NVARCHAR(50),
    descrizione_voce_costo NVARCHAR(500),
    importo_autorizzato DECIMAL(15, 2),
    importo_certificato DECIMAL(15, 2),
    importo_rimodulato DECIMAL(15, 2),
    FOREIGN KEY (codice_istituto) REFERENCES dim_scuole(codice_scuola)
);
GO

-- ============================================
-- INDEXES per performance
-- ============================================

-- Indici per fact_personale
CREATE INDEX idx_personale_provincia ON fact_personale(provincia);
CREATE INDEX idx_personale_anno ON fact_personale(anno_scolastico);
CREATE INDEX idx_personale_ordine ON fact_personale(ordine_scuola);
GO

-- Indici per fact_alunni
CREATE INDEX idx_alunni_scuola ON fact_alunni(codice_scuola);
CREATE INDEX idx_alunni_anno ON fact_alunni(anno_scolastico);
GO

-- Indici per fact_progetti_eu
CREATE INDEX idx_progetti_istituto ON fact_progetti_eu(codice_istituto_attualizzato);
CREATE INDEX idx_progetti_anno ON fact_progetti_eu(anno_avviso);
CREATE INDEX idx_progetti_stato ON fact_progetti_eu(stato_candidatura);
CREATE INDEX idx_progetti_tipo ON fact_progetti_eu(tipo_fondo);
GO

-- Indici per dim_scuole
CREATE INDEX idx_scuole_provincia ON dim_scuole(provincia);
CREATE INDEX idx_scuole_regione ON dim_scuole(regione);
CREATE INDEX idx_scuole_codice_istituto ON dim_scuole(codice_istituto_riferimento);
GO

-- ============================================
-- VIEWS per analisi comuni
-- ============================================

-- View: Riepilogo Docenti per Provincia
IF OBJECT_ID('view_docenti_per_provincia', 'V') IS NOT NULL DROP VIEW view_docenti_per_provincia;
GO

CREATE VIEW view_docenti_per_provincia AS
SELECT 
    p.provincia,
    p.regione,
    p.area_geografica,
    fp.anno_scolastico,
    SUM(fp.totale_docenti) as totale_docenti,
    SUM(fp.docenti_femmine) as totale_femmine,
    SUM(fp.docenti_maschi) as totale_maschi,
    ROUND(AVG(fp.perc_femmine), 2) as media_perc_femmine
FROM fact_personale fp
LEFT JOIN dim_province p ON fp.provincia = p.provincia
GROUP BY p.provincia, p.regione, p.area_geografica, fp.anno_scolastico;
GO

-- View: Riepilogo Alunni per Scuola
IF OBJECT_ID('view_alunni_per_scuola', 'V') IS NOT NULL DROP VIEW view_alunni_per_scuola;
GO

CREATE VIEW view_alunni_per_scuola AS
SELECT 
    s.codice_scuola,
    s.denominazione_scuola,
    s.provincia,
    s.regione,
    s.area_geografica,
    s.tipologia_grado,
    a.anno_scolastico,
    SUM(a.numero_alunni) as totale_alunni
FROM fact_alunni a
JOIN dim_scuole s ON a.codice_scuola = s.codice_scuola
GROUP BY s.codice_scuola, s.denominazione_scuola, s.provincia, s.regione, 
         s.area_geografica, s.tipologia_grado, a.anno_scolastico;
GO

-- View: Fondi EU per Area Geografica
IF OBJECT_ID('view_fondi_eu_per_area', 'V') IS NOT NULL DROP VIEW view_fondi_eu_per_area;
GO

CREATE VIEW view_fondi_eu_per_area AS
SELECT 
    p.tipo_fondo,
    p.descrizione_area_territoriale,
    p.anno_avviso,
    COUNT(DISTINCT p.codice_progetto) as numero_progetti,
    COUNT(DISTINCT p.codice_istituto_attualizzato) as numero_scuole,
    SUM(p.importo_autorizzato) as totale_autorizzato,
    SUM(p.importo_erogato) as totale_erogato,
    SUM(p.importo_certificato) as totale_certificato,
    ROUND(AVG(p.perc_erogato), 2) as media_perc_erogato,
    ROUND(AVG(p.perc_certificato), 2) as media_perc_certificato
FROM fact_progetti_eu p
GROUP BY p.tipo_fondo, p.descrizione_area_territoriale, p.anno_avviso;
GO

-- View: Ratio Studenti/Docenti per Provincia
IF OBJECT_ID('view_ratio_studenti_docenti', 'V') IS NOT NULL DROP VIEW view_ratio_studenti_docenti;
GO

CREATE VIEW view_ratio_studenti_docenti AS
SELECT 
    s.provincia,
    s.regione,
    s.area_geografica,
    a.anno_scolastico,
    SUM(a.numero_alunni) as totale_studenti,
    (SELECT SUM(fp.totale_docenti) 
     FROM fact_personale fp 
     WHERE fp.provincia = s.provincia 
     AND fp.anno_scolastico = a.anno_scolastico) as totale_docenti,
    CASE 
        WHEN (SELECT SUM(fp.totale_docenti) 
              FROM fact_personale fp 
              WHERE fp.provincia = s.provincia 
              AND fp.anno_scolastico = a.anno_scolastico) > 0
        THEN ROUND(CAST(SUM(a.numero_alunni) AS FLOAT) / 
             (SELECT SUM(fp.totale_docenti) 
              FROM fact_personale fp 
              WHERE fp.provincia = s.provincia 
              AND fp.anno_scolastico = a.anno_scolastico), 2)
        ELSE NULL
    END as ratio_studenti_docenti
FROM fact_alunni a
JOIN dim_scuole s ON a.codice_scuola = s.codice_scuola
GROUP BY s.provincia, s.regione, s.area_geografica, a.anno_scolastico;
GO

PRINT 'Schema database creato con successo!';
PRINT 'Database: ItalianEducationDB';
PRINT 'Tabelle: 8 (3 dimension + 5 fact)';
PRINT 'Views: 4';
GO
