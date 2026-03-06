-- ============================================================
-- NPI Database Schema – First Normal Form (1NF)
-- Target:  MariaDB (latest)
-- Source:  NPPES Data Dissemination (npidata_pfile_20050523-20260208)
--          + companion files: othername, pl, endpoint
-- Focus:   NPI ↔ Taxonomy relationships
--
-- 1NF transformations applied:
--   1. Removed repeating group Healthcare Provider Taxonomy Code_1..15
--      (+ license number, state code, primary switch, taxonomy group)
--      → provider_taxonomy table (one row per NPI-taxonomy pair)
--
--   2. Removed repeating group Other Provider Identifier_1..50
--      (+ type code, state, issuer)
--      → other_provider_identifier table (one row per identifier)
--
--   3. Each column holds a single, atomic value.
--   4. Every table has a well-defined primary key.
--
-- Column dropped (entirely blank across all 9.3 M rows):
--   NPI Deactivation Reason Code
-- ============================================================

CREATE DATABASE IF NOT EXISTS npi_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE npi_db;

-- ============================================================
-- Table 1: provider
-- One row per NPI.  All non-repeating scalar attributes.
-- Entity Type Code: 1 = Individual, 2 = Organization
-- ============================================================

CREATE TABLE IF NOT EXISTS provider (
    npi                             CHAR(10)        NOT NULL,
    entity_type_code                TINYINT UNSIGNED NOT NULL   COMMENT '1=Individual 2=Organization',
    replacement_npi                 CHAR(10)        NULL,

    -- Organization identity (entity_type_code = 2)
    org_name                        VARCHAR(300)    NULL        COMMENT 'Legal Business Name',
    ein                             VARCHAR(20)     NULL        COMMENT 'Employer Identification Number',

    -- Individual identity (entity_type_code = 1)
    last_name                       VARCHAR(100)    NULL,
    first_name                      VARCHAR(100)    NULL,
    middle_name                     VARCHAR(100)    NULL,
    name_prefix                     VARCHAR(10)     NULL,
    name_suffix                     VARCHAR(10)     NULL,
    credential                      VARCHAR(50)     NULL,
    sex_code                        CHAR(1)         NULL        COMMENT 'M / F',

    -- Alternate / "other" name (single entry in main file;
    --   additional entries live in other_name table from companion file)
    other_org_name                  VARCHAR(300)    NULL,
    other_org_name_type_code        VARCHAR(5)      NULL,
    other_last_name                 VARCHAR(100)    NULL,
    other_first_name                VARCHAR(100)    NULL,
    other_middle_name               VARCHAR(100)    NULL,
    other_name_prefix               VARCHAR(10)     NULL,
    other_name_suffix               VARCHAR(10)     NULL,
    other_credential                VARCHAR(50)     NULL,
    other_last_name_type_code       VARCHAR(5)      NULL,

    -- Primary mailing address
    mailing_address_line1           VARCHAR(200)    NULL,
    mailing_address_line2           VARCHAR(200)    NULL,
    mailing_city                    VARCHAR(100)    NULL,
    mailing_state                   VARCHAR(40)     NULL,
    mailing_postal_code             VARCHAR(20)     NULL,
    mailing_country_code            VARCHAR(5)      NULL,
    mailing_telephone               VARCHAR(20)     NULL,
    mailing_fax                     VARCHAR(20)     NULL,

    -- Primary practice location address
    practice_address_line1          VARCHAR(200)    NULL,
    practice_address_line2          VARCHAR(200)    NULL,
    practice_city                   VARCHAR(100)    NULL,
    practice_state                  VARCHAR(40)     NULL,
    practice_postal_code            VARCHAR(20)     NULL,
    practice_country_code           VARCHAR(5)      NULL,
    practice_telephone              VARCHAR(20)     NULL,
    practice_fax                    VARCHAR(20)     NULL,

    -- Lifecycle dates (NPI Deactivation Reason Code omitted — entirely blank)
    enumeration_date                DATE            NULL,
    last_update_date                DATE            NULL,
    npi_deactivation_date           DATE            NULL,
    npi_reactivation_date           DATE            NULL,

    -- Authorized official (organizations only)
    auth_last_name                  VARCHAR(100)    NULL,
    auth_first_name                 VARCHAR(100)    NULL,
    auth_middle_name                VARCHAR(100)    NULL,
    auth_name_prefix                VARCHAR(10)     NULL,
    auth_name_suffix                VARCHAR(10)     NULL,
    auth_credential                 VARCHAR(50)     NULL,
    auth_title_or_position          VARCHAR(100)    NULL,
    auth_telephone                  VARCHAR(20)     NULL,

    -- Organizational hierarchy
    is_sole_proprietor              CHAR(1)         NULL        COMMENT 'Y / N / X',
    is_organization_subpart         CHAR(1)         NULL        COMMENT 'Y / N / X',
    parent_org_lbn                  VARCHAR(300)    NULL        COMMENT 'Parent Organization Legal Business Name',
    parent_org_tin                  VARCHAR(20)     NULL        COMMENT 'Parent Organization Tax Identification Number',

    certification_date              DATE            NULL,

    PRIMARY KEY (npi),
    INDEX idx_entity_type           (entity_type_code),
    INDEX idx_last_name             (last_name),
    INDEX idx_org_name              (org_name(100)),
    INDEX idx_practice_state        (practice_state),
    INDEX idx_enumeration_date      (enumeration_date)
) ENGINE=InnoDB
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;


-- ============================================================
-- Table 2: taxonomy
-- Reference / lookup table for NUCC Healthcare Provider
-- Taxonomy codes encountered in the dataset.
-- ============================================================

CREATE TABLE IF NOT EXISTS taxonomy (
    taxonomy_code   VARCHAR(10)     NOT NULL,
    PRIMARY KEY (taxonomy_code)
) ENGINE=InnoDB
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;


-- ============================================================
-- Table 3: provider_taxonomy
-- 1NF FIX: Resolves the 15-slot repeating group
--   (Healthcare Provider Taxonomy Code_1 .. _15,
--    Provider License Number_1 .. _15,
--    Provider License Number State Code_1 .. _15,
--    Healthcare Provider Primary Taxonomy Switch_1 .. _15,
--    Healthcare Provider Taxonomy Group_1 .. _15)
--
-- One row per (NPI, slot).  Central table for NPI↔Taxonomy queries.
-- ============================================================

CREATE TABLE IF NOT EXISTS provider_taxonomy (
    id                  INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    npi                 CHAR(10)        NOT NULL,
    taxonomy_code       VARCHAR(10)     NOT NULL,
    license_number      VARCHAR(50)     NULL,
    license_state_code  VARCHAR(5)      NULL,
    is_primary          CHAR(1)         NULL    COMMENT 'Y = this is the provider''s primary taxonomy',
    taxonomy_group      VARCHAR(100)    NULL,
    slot_order          TINYINT UNSIGNED NOT NULL COMMENT 'Original CSV slot (1..15)',

    PRIMARY KEY (id),
    UNIQUE  KEY uq_npi_slot         (npi, slot_order),
    INDEX       idx_npi             (npi),
    INDEX       idx_taxonomy_code   (taxonomy_code),
    INDEX       idx_is_primary      (is_primary),
    INDEX       idx_license_state   (license_state_code),

    CONSTRAINT fk_pt_npi
        FOREIGN KEY (npi)           REFERENCES provider(npi)   ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_pt_taxonomy
        FOREIGN KEY (taxonomy_code) REFERENCES taxonomy(taxonomy_code) ON UPDATE CASCADE
) ENGINE=InnoDB
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;


-- ============================================================
-- Table 4: other_provider_identifier
-- 1NF FIX: Resolves the 50-slot repeating group
--   (Other Provider Identifier_1 .. _50,
--    Other Provider Identifier Type Code_1 .. _50,
--    Other Provider Identifier State_1 .. _50,
--    Other Provider Identifier Issuer_1 .. _50)
--
-- One row per (NPI, slot).
-- ============================================================

CREATE TABLE IF NOT EXISTS other_provider_identifier (
    id              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    npi             CHAR(10)        NOT NULL,
    identifier      VARCHAR(100)    NOT NULL,
    type_code       VARCHAR(10)     NULL,
    state           VARCHAR(5)      NULL,
    issuer          VARCHAR(100)    NULL,
    slot_order      TINYINT UNSIGNED NOT NULL COMMENT 'Original CSV slot (1..50)',

    PRIMARY KEY (id),
    UNIQUE  KEY uq_npi_slot     (npi, slot_order),
    INDEX       idx_npi         (npi),
    INDEX       idx_identifier  (identifier),
    INDEX       idx_type_code   (type_code),

    CONSTRAINT fk_opi_npi
        FOREIGN KEY (npi) REFERENCES provider(npi) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;


-- ============================================================
-- Table 5: other_name
-- From companion file: othername_pfile_20050523-20260208.csv
-- Stores additional organization names beyond the single
-- "other name" slot already captured in provider.
-- ============================================================

CREATE TABLE IF NOT EXISTS other_name (
    id                          INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    npi                         CHAR(10)        NOT NULL,
    other_org_name              VARCHAR(300)    NOT NULL,
    other_org_name_type_code    VARCHAR(5)      NULL,

    PRIMARY KEY (id),
    INDEX idx_npi           (npi),
    INDEX idx_org_name      (other_org_name(100)),

    CONSTRAINT fk_on_npi
        FOREIGN KEY (npi) REFERENCES provider(npi) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;


-- ============================================================
-- Table 6: secondary_practice_location
-- From companion file: pl_pfile_20050523-20260208.csv
-- Stores additional practice location addresses (beyond the
-- primary practice address stored in provider).
-- ============================================================

CREATE TABLE IF NOT EXISTS secondary_practice_location (
    id              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    npi             CHAR(10)        NOT NULL,
    address_line1   VARCHAR(200)    NULL,
    address_line2   VARCHAR(200)    NULL,
    city            VARCHAR(100)    NULL,
    state           VARCHAR(40)     NULL,
    postal_code     VARCHAR(20)     NULL,
    country_code    VARCHAR(5)      NULL,
    telephone       VARCHAR(20)     NULL,
    telephone_ext   VARCHAR(10)     NULL,
    fax             VARCHAR(20)     NULL,

    PRIMARY KEY (id),
    INDEX idx_npi   (npi),
    INDEX idx_state (state),

    CONSTRAINT fk_spl_npi
        FOREIGN KEY (npi) REFERENCES provider(npi) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;


-- ============================================================
-- Table 7: endpoint
-- From companion file: endpoint_pfile_20050523-20260208.csv
-- ============================================================

CREATE TABLE IF NOT EXISTS endpoint (
    id                              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    npi                             CHAR(10)        NOT NULL,
    endpoint_type                   VARCHAR(20)     NULL,
    endpoint_type_description       VARCHAR(100)    NULL,
    endpoint_value                  VARCHAR(300)    NULL,
    affiliation                     CHAR(1)         NULL,
    endpoint_description            VARCHAR(300)    NULL,
    affiliation_lbn                 VARCHAR(300)    NULL,
    use_code                        VARCHAR(10)     NULL,
    use_description                 VARCHAR(100)    NULL,
    other_use_description           VARCHAR(200)    NULL,
    content_type                    VARCHAR(10)     NULL,
    content_description             VARCHAR(100)    NULL,
    other_content_description       VARCHAR(200)    NULL,
    affiliation_address_line1       VARCHAR(200)    NULL,
    affiliation_address_line2       VARCHAR(200)    NULL,
    affiliation_address_city        VARCHAR(100)    NULL,
    affiliation_address_state       VARCHAR(40)     NULL,
    affiliation_address_country     VARCHAR(5)      NULL,
    affiliation_address_postal      VARCHAR(20)     NULL,

    PRIMARY KEY (id),
    INDEX idx_npi               (npi),
    INDEX idx_endpoint_type     (endpoint_type),

    CONSTRAINT fk_ep_npi
        FOREIGN KEY (npi) REFERENCES provider(npi) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;


-- ============================================================
-- Useful views for NPI ↔ Taxonomy queries
-- ============================================================

-- All taxonomy codes associated with each NPI (primary only)
CREATE OR REPLACE VIEW v_npi_primary_taxonomy AS
SELECT
    p.npi,
    p.entity_type_code,
    COALESCE(p.org_name, CONCAT_WS(' ', p.first_name, p.last_name)) AS provider_name,
    pt.taxonomy_code,
    pt.license_number,
    pt.license_state_code,
    pt.taxonomy_group,
    p.practice_state,
    p.practice_city
FROM provider p
JOIN provider_taxonomy pt ON pt.npi = p.npi
WHERE pt.is_primary = 'Y';

-- Count of NPIs per taxonomy code (useful for analytics)
CREATE OR REPLACE VIEW v_taxonomy_npi_count AS
SELECT
    taxonomy_code,
    COUNT(DISTINCT npi)     AS total_npis,
    SUM(is_primary = 'Y')   AS primary_count
FROM provider_taxonomy
GROUP BY taxonomy_code;
