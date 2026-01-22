*&---------------------------------------------------------------------*
*& Table Definition: ZSCM_RFQ_JSONMAP
*&---------------------------------------------------------------------*
*& Purpose: Maps JSON field names to GTY_UPLOAD field names per scenario
*&---------------------------------------------------------------------*
*& Description:
*& This table stores the mapping between JSON field names from the
*& external API (Sourcing_Data_Transfer_API_Import_v1.1) and
*& GTY_UPLOAD structure fields (a, b, c, ... jz) for different scenarios.
*&
*& Key Features:
*& - Scenario-based mappings (E, O, P, F, G, M, D, etc.)
*& - Maps JSON field names to GTY_UPLOAD column names
*& - References SAP field names for clarity
*& - Supports data conversion rules
*&---------------------------------------------------------------------*
*& Table Structure (to be created in SE11):
*&---------------------------------------------------------------------*
*& Field Name     | Type      | Length | Description
*&----------------|-----------|--------|----------------------------------
*& ZSCENR         | CHAR      | 1      | Scenario code (E, O, P, F, G, etc.)
*& JSON_FIELD     | CHAR      | 40     | JSON field name (e.g., 'vendor', 'rate')
*& UPLOAD_FIELD   | CHAR      | 3      | GTY_UPLOAD field name (e.g., 'a', 'b', 'k')
*& SAP_FIELD      | FIELDNAME | 30     | SAP field name (e.g., 'TDLNR', 'KBETR')
*& CONVERSION     | CHAR      | 10     | Conversion rule (e.g., 'ALPHA', 'DATE', 'NUMBER')
*& MANDATORY      | CHAR      | 1      | Mandatory flag (X = Mandatory)
*& ACTIVE         | CHAR      | 1      | Active flag (X = Active)
*& CREATED_BY     | SYUNAME   | 12     | Created by user
*& CREATED_DATE   | DATS      | 8      | Creation date
*& CREATED_TIME   | TIMS      | 6      | Creation time
*& CHANGED_BY     | SYUNAME   | 12     | Changed by user
*& CHANGED_DATE   | DATS      | 8      | Change date
*& CHANGED_TIME   | TIMS      | 6      | Change time
*&---------------------------------------------------------------------*
*& Primary Key: ZSCENR, JSON_FIELD
*&---------------------------------------------------------------------*
*& Technical Settings:
*& - Data Class: APPL0
*& - Size Category: 0
*& - Buffering: Not buffered
*&---------------------------------------------------------------------*
*& Maintenance:
*& - Create transaction for maintenance (e.g., ZSCM_RFQ_JSONMAP)
*& - Or use SM30 for table maintenance
*&---------------------------------------------------------------------*
*& Example Mappings for Scenario 'E':
*&---------------------------------------------------------------------*
*& ZSCENR | JSON_FIELD      | UPLOAD_FIELD | SAP_FIELD | CONVERSION | MANDATORY
*&--------|-----------------|--------------|-----------|------------|----------
*& E      | vendor          | a            | TDLNR     | ALPHA      | X
*& E      | charge_code     | b            | KSCHL     | NONE       | X
*& E      | origin          | c            | (varies)  | NONE       | 
*& E      | destination     | d            | (varies)  | NONE       | 
*& E      | scale_id        | k            | SCAID     | ALPHA      | 
*& E      | rate            | (via COLM)   | KBETR     | NUMBER     | X
*& E      | currency        | (via COLM)   | KONWA     | NONE       | X
*& E      | validity_from   | (via COLM)   | DATAB     | DATE       | X
*& E      | validity_to     | (via COLM)   | DATBI     | DATE       | X
*&---------------------------------------------------------------------*
*& Notes:
*& - For fields that map via ZLOG_COLM_MAP (like DATAB, DATBI, KBETR),
*&   the UPLOAD_FIELD will be determined dynamically from ZLOG_COLM_MAP
*& - Charge code (charge_code) should be mapped to KSCHL via ZSCM_RFQ_CHRGMAP
*&   before being assigned to field 'b'
*&---------------------------------------------------------------------*

" This file is a reference document for table creation
" Actual table must be created in SE11 (Transaction Code)
"
" Steps to create:
" 1. Go to SE11
" 2. Enter table name: ZSCM_RFQ_JSONMAP
" 3. Create fields as specified above
" 4. Set primary key: ZSCENR, JSON_FIELD
" 5. Activate table
" 6. Create maintenance view if needed
" 7. Populate mappings for each scenario

