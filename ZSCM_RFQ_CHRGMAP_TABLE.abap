*&---------------------------------------------------------------------*
*& Table Definition: ZSCM_RFQ_CHRGMAP
*&---------------------------------------------------------------------*
*& Purpose: Maps charge codes from API input to SAP condition types
*&---------------------------------------------------------------------*
*& Description:
*& This table stores the mapping between charge codes received from
*& the external API (Sourcing_Data_Transfer_API_Import_v1.1) and
*& SAP condition types (KSCHL) used in LE pricing.
*&
*& Key Features:
*& - Supports service-type specific mappings (FF/CHA)
*& - Validity period support
*& - Active/Inactive flag for easy maintenance
*&---------------------------------------------------------------------*
*& Table Structure (to be created in SE11):
*&---------------------------------------------------------------------*
*& Field Name     | Type      | Length | Description
*&----------------|-----------|--------|----------------------------------
*& CHARGE_CODE    | CHAR      | 20     | Charge code from API input
*& SERVICE_TYPE   | CHAR      | 10     | Service type (FF/CHA) - Optional
*& KSCHL          | KSCHL     | 4      | SAP condition type
*& VALID_FROM     | DATS      | 8      | Validity start date
*& VALID_TO       | DATS      | 8      | Validity end date
*& ACTIVE         | CHAR      | 1      | Active flag (X = Active)
*& CREATED_BY     | SYUNAME   | 12     | Created by user
*& CREATED_DATE   | DATS      | 8      | Creation date
*& CREATED_TIME   | TIMS      | 6      | Creation time
*& CHANGED_BY     | SYUNAME   | 12     | Changed by user
*& CHANGED_DATE   | DATS      | 8      | Change date
*& CHANGED_TIME   | TIMS      | 6      | Change time
*&---------------------------------------------------------------------*
*& Primary Key: CHARGE_CODE, SERVICE_TYPE
*&---------------------------------------------------------------------*
*& Technical Settings:
*& - Data Class: APPL0
*& - Size Category: 0
*& - Buffering: Not buffered
*&---------------------------------------------------------------------*
*& Maintenance:
*& - Create transaction for maintenance (e.g., ZSCM_RFQ_CHRGMAP)
*& - Or use SM30 for table maintenance
*&---------------------------------------------------------------------*

" This file is a reference document for table creation
" Actual table must be created in SE11 (Transaction Code)
"
" Steps to create:
" 1. Go to SE11
" 2. Enter table name: ZSCM_RFQ_CHRGMAP
" 3. Create fields as specified above
" 4. Set primary key: CHARGE_CODE, SERVICE_TYPE
" 5. Activate table
" 6. Create maintenance view if needed

