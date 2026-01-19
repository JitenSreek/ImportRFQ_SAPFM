*&---------------------------------------------------------------------*
*& Function Module  Z_SCM_RATE_UPLOAD_IMPFF_WR
*&---------------------------------------------------------------------*
*& Purpose: Convert JSON payload from API to SAP rate upload format
*&---------------------------------------------------------------------*
*& Author: Generated
*& Creation Date: [Date]
*&---------------------------------------------------------------------*
*& Description:
*& This function module accepts JSON payload from Sourcing_Data_Transfer_API
*& Import v1.1, maps charge codes to SAP condition types, converts the
*& JSON structure to GTY_UPLOAD format, and calls Z_SCM_RATE_UPLOAD_IMPFF
*& to upload rates into SAP LE pricing.
*&
*& Key Features:
*& - JSON payload parsing using ABAP JSON classes
*& - Charge code to condition type mapping via ZSCM_RFQ_CHRGMAP
*& - Data conversion from JSON to GTY_UPLOAD structure
*& - Integration with existing Z_SCM_RATE_UPLOAD_IMPFF
*&---------------------------------------------------------------------*
*& CHANGE HISTORY
*&---------------------------------------------------------------------*
*& Date       User ID  Description                          Change Label
*&---------------------------------------------------------------------*
*& [Date]     [User]   Initial creation                     [Label]
*&---------------------------------------------------------------------*

" BEGIN: Cursor Generated Code
FUNCTION z_scm_rate_upload_impff_wr.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(IV_JSON_PAYLOAD) TYPE STRING
*"     VALUE(IV_CHNG) TYPE C OPTIONAL
*"     VALUE(IV_BATCH_SIZE) TYPE I OPTIONAL
*"     VALUE(IV_TEST_MODE) TYPE C OPTIONAL
*"  EXPORTING
*"     VALUE(EV_TOTAL_RECORDS) TYPE I
*"     VALUE(EV_SUCCESS_COUNT) TYPE I
*"     VALUE(EV_ERROR_COUNT) TYPE I
*"  TABLES
*"     ET_RESULT TYPE ZLOG_RATE_UPLOAD_RESULT
*"  EXCEPTIONS
*"     INVALID_JSON
*"     MAPPING_ERROR
*"     CONVERSION_ERROR
*"     PROCESSING_ERROR
*"----------------------------------------------------------------------

  " Local type definitions
  TYPES: BEGIN OF lty_charge_code_map,
           charge_code TYPE char20,
           service_type TYPE char10,
           kschl TYPE kschl,
           valid_from TYPE dats,
           valid_to TYPE dats,
           active TYPE c,
         END OF lty_charge_code_map.

  TYPES: BEGIN OF lty_json_header,
           charge_code TYPE string,
           rate TYPE string,
           currency TYPE string,
           validity_from TYPE string,
           validity_to TYPE string,
           scale_id TYPE string,
           vendor TYPE string,
           account_id TYPE string,
           " Additional fields as per API structure
           origin TYPE string,
           destination TYPE string,
           service_type TYPE string,
           uom TYPE string,
           min_value TYPE string,
           max_value TYPE string,
           scale_value TYPE string,
         END OF lty_json_header.

  TYPES: BEGIN OF lty_json_payload,
           rfq_no TYPE string,
           rfq_date TYPE string,
           accountid TYPE string,
           service_product TYPE string,
           contract_type TYPE string,
           header_data TYPE STANDARD TABLE OF lty_json_header WITH DEFAULT KEY,
         END OF lty_json_payload.

  TYPES: BEGIN OF lty_colm_map,
           zscenr TYPE char1,
           zcolno TYPE char2,
           zfield TYPE fieldname,
           zdeser TYPE char40,
         END OF lty_colm_map.

  TYPES: lty_upload TYPE gty_upload.
  TYPES: lty_xldata TYPE gty_xldata.

  " Local data declarations
  DATA: lv_json_payload TYPE string,
        lv_total_records TYPE i,
        lv_success_count TYPE i,
        lv_error_count TYPE i,
        lv_record_num TYPE i,
        lv_batch_size TYPE i,
        lv_test_mode TYPE c,
        lv_chng TYPE c,
        lv_charge_code TYPE string,
        lv_service_type TYPE string,
        lv_kschl TYPE kschl,
        lv_mapping_found TYPE c,
        lv_json_error TYPE string,
        lv_conversion_error TYPE string,
        lv_date_str TYPE string,
        lv_date TYPE dats,
        lv_rate_str TYPE string,
        lv_rate TYPE p DECIMALS 2,
        lv_currency TYPE waers,
        lv_vendor TYPE char10,
        lv_scale_id TYPE char18.

  DATA: lo_json TYPE REF TO /ui2/cl_json,
        lo_exception TYPE REF TO cx_root,
        lv_json_parsed TYPE c.

  DATA: lt_charge_code_map TYPE STANDARD TABLE OF lty_charge_code_map,
        lt_json_header TYPE STANDARD TABLE OF lty_json_header,
        lt_upload TYPE STANDARD TABLE OF lty_upload,
        lt_xldata TYPE STANDARD TABLE OF lty_xldata,
        lt_colm_map TYPE STANDARD TABLE OF lty_colm_map,
        lt_result TYPE STANDARD TABLE OF zlog_rate_upload_result.

  DATA: lw_charge_code_map TYPE lty_charge_code_map,
        lw_json_header TYPE lty_json_header,
        lw_upload TYPE lty_upload,
        lw_xldata TYPE lty_xldata,
        lw_colm_map TYPE lty_colm_map,
        lw_json_payload TYPE lty_json_payload,
        lw_result TYPE zlog_rate_upload_result.

  " Field symbols
  FIELD-SYMBOLS: <lfs_upload> TYPE lty_upload,
                 <lfs_value> TYPE any,
                 <lfs_json_field> TYPE any.

  " Constants
  CONSTANTS: lc_scenario_e TYPE char1 VALUE 'E',
             lc_x TYPE c VALUE 'X',
             lc_active TYPE c VALUE 'X',
             lc_success TYPE c VALUE 'S',
             lc_error TYPE c VALUE 'E',
             lc_warning TYPE c VALUE 'W',
             lc_date_format_api TYPE string VALUE 'YYYY-MM-DD',
             lc_date_format_sap TYPE string VALUE 'YYYYMMDD'.

  " Initialize counters
  CLEAR: ev_total_records, ev_success_count, ev_error_count.
  CLEAR: lv_total_records, lv_success_count, lv_error_count.
  REFRESH: et_result, lt_result.

  " Validate input
  IF iv_json_payload IS INITIAL.
    RAISE invalid_json.
  ENDIF.

  lv_json_payload = iv_json_payload.

  " Set default batch size
  IF iv_batch_size IS INITIAL OR iv_batch_size <= 0.
    lv_batch_size = 1000.
  ELSE.
    lv_batch_size = iv_batch_size.
  ENDIF.

  " Set flags
  lv_test_mode = iv_test_mode.
  lv_chng = iv_chng.

  " Step 1: Parse JSON payload
  " Note: JSON parsing implementation depends on SAP version
  " For SAP 7.40+: Use /UI2/CL_JSON
  " For earlier versions: Use CL_SXML_JSON_READER or custom parsing
  TRY.
      " Try using /UI2/CL_JSON (available in SAP_BASIS 7.40+)
      CREATE OBJECT lo_json TYPE /ui2/cl_json.

      " Parse JSON string to internal structure
      CALL METHOD lo_json->deserialize
        EXPORTING
          json = lv_json_payload
        CHANGING
          data = lw_json_payload.

      " Note: Method calls don't set SY-SUBRC, exceptions are caught in CATCH block

      " Extract header_data array
      lt_json_header = lw_json_payload-header_data.

      IF lt_json_header IS INITIAL.
        CONCATENATE 'No header_data found in JSON payload' INTO lv_json_error.
        RAISE invalid_json.
      ENDIF.

      lv_json_parsed = lc_x.

    CATCH cx_root INTO lo_exception.
      " If /UI2/CL_JSON is not available, try alternative method
      " For now, raise exception - implement alternative parsing if needed
      CLEAR lv_json_error.
      CALL METHOD lo_exception->get_text
        RECEIVING
          result = lv_json_error.
      IF sy-subrc <> 0.
        lv_json_error = 'Exception occurred during JSON parsing'.
      ENDIF.
      RAISE invalid_json.
  ENDTRY.

  " Step 2: Load charge code mapping table
  SELECT charge_code
         service_type
         kschl
         valid_from
         valid_to
         active
    FROM zscm_rfq_chrgmap
    INTO TABLE lt_charge_code_map
    WHERE active = lc_active.

  IF sy-subrc <> 0.
    " No mappings found - this is a warning, not an error
    " We'll handle missing mappings per record
  ENDIF.

  SORT lt_charge_code_map BY charge_code service_type.

  " Step 3: Load column mapping for scenario 'E' to understand field mappings
  SELECT zscenr
         zcolno
         zfield
         zdeser
    FROM zlog_colm_map
    INTO TABLE lt_colm_map
    WHERE zscenr = lc_scenario_e.

  IF sy-subrc <> 0.
    " Column mapping not found - will use default mappings
  ENDIF.

  SORT lt_colm_map BY zfield.

  " Step 4: Convert JSON records to GTY_UPLOAD structure
  lv_record_num = 0.

  LOOP AT lt_json_header INTO lw_json_header.
    lv_record_num = lv_record_num + 1.
    CLEAR: lw_upload, lv_charge_code, lv_service_type, lv_kschl, lv_mapping_found.
    CLEAR: lv_vendor, lv_scale_id, lv_rate, lv_currency, lv_date.

    " Extract charge code and service type
    lv_charge_code = lw_json_header-charge_code.
    CONDENSE lv_charge_code.
    TRANSLATE lv_charge_code TO UPPER CASE.

    " Get service type from header or from individual record
    IF lw_json_header-service_type IS NOT INITIAL.
      lv_service_type = lw_json_header-service_type.
    ELSE.
      " Use service_product from payload header
      lv_service_type = lw_json_payload-service_product.
    ENDIF.
    CONDENSE lv_service_type.
    TRANSLATE lv_service_type TO UPPER CASE.

    " Step 4.1: Map charge code to KSCHL
    READ TABLE lt_charge_code_map INTO lw_charge_code_map
                                   WITH KEY charge_code = lv_charge_code
                                            service_type = lv_service_type
                                   BINARY SEARCH.

    IF sy-subrc = 0.
      " Check validity period
      IF lw_charge_code_map-valid_from IS NOT INITIAL AND
         lw_charge_code_map-valid_to IS NOT INITIAL.
        " Validate current date is within validity period
        IF sy-datum < lw_charge_code_map-valid_from OR
           sy-datum > lw_charge_code_map-valid_to.
          " Mapping expired - try without service type
          READ TABLE lt_charge_code_map INTO lw_charge_code_map
                                         WITH KEY charge_code = lv_charge_code
                                         BINARY SEARCH.
        ENDIF.
      ENDIF.
    ELSE.
      " Try lookup without service type
      READ TABLE lt_charge_code_map INTO lw_charge_code_map
                                     WITH KEY charge_code = lv_charge_code
                                     BINARY SEARCH.
    ENDIF.

    IF sy-subrc = 0 AND lw_charge_code_map-kschl IS NOT INITIAL.
      lv_kschl = lw_charge_code_map-kschl.
      lv_mapping_found = lc_x.
    ELSE.
      " Charge code mapping not found
      CLEAR lw_result.
      lw_result-record_number = lv_record_num.
      lw_result-status = lc_error.
      lw_result-message_type = 'E'.
      lw_result-message_id = 'ZLOG'.
      lw_result-message_number = '001'.
      CONCATENATE 'Charge code mapping not found:' lv_charge_code
                  'for service type:' lv_service_type
                  INTO lw_result-message_text SEPARATED BY space.
      lw_result-error_details = lw_result-message_text.
      lw_result-flag = lc_x.
      lw_result-reason = lw_result-message_text.
      APPEND lw_result TO lt_result.
      lv_error_count = lv_error_count + 1.
      CONTINUE.
    ENDIF.

    " Step 4.2: Convert JSON fields to GTY_UPLOAD structure
    CLEAR lw_upload.

    " Field 'a' - Vendor (TDLNR)
    IF lw_json_header-vendor IS NOT INITIAL.
      lv_vendor = lw_json_header-vendor.
      CALL FUNCTION 'CONVERSION_EXIT_ALPHA_INPUT'
        EXPORTING
          input  = lv_vendor
        IMPORTING
          output = lv_vendor.
      IF sy-subrc = 0.
        lw_upload-a = lv_vendor.
      ENDIF.
    ELSEIF lw_json_payload-accountid IS NOT INITIAL.
      " Use accountid as vendor
      lv_vendor = lw_json_payload-accountid.
      CALL FUNCTION 'CONVERSION_EXIT_ALPHA_INPUT'
        EXPORTING
          input  = lv_vendor
        IMPORTING
          output = lv_vendor.
      IF sy-subrc = 0.
        lw_upload-a = lv_vendor.
      ENDIF.
    ENDIF.

    " Field 'b' - Condition Type (KSCHL)
    lw_upload-b = lv_kschl.

    " Field 'c' through 'j' - Additional condition key fields
    " These will be populated based on column mapping or default logic
    " Field 'c' - Could be loading zone, origin, etc.
    IF lw_json_header-origin IS NOT INITIAL.
      lw_upload-c = lw_json_header-origin.
    ENDIF.

    " Field 'd' - Could be destination zone
    IF lw_json_header-destination IS NOT INITIAL.
      lw_upload-d = lw_json_header-destination.
    ENDIF.

    " Field 'e' - Vehicle type or other field
    " (Will be mapped based on actual API structure)

    " Field 'f' - Additional field
    " (Will be mapped based on actual API structure)

    " Field 'g' - Additional field
    " (Will be mapped based on actual API structure)

    " Field 'h' - Additional field
    " (Will be mapped based on actual API structure)

    " Field 'i' - Additional field
    " (Will be mapped based on actual API structure)

    " Field 'j' - Additional field
    " (Will be mapped based on actual API structure)

    " Field 'k' - Scale ID (SCAID)
    IF lw_json_header-scale_id IS NOT INITIAL.
      lv_scale_id = lw_json_header-scale_id.
      CALL FUNCTION 'CONVERSION_EXIT_ALPHA_INPUT'
        EXPORTING
          input  = lv_scale_id
        IMPORTING
          output = lv_scale_id.
      IF sy-subrc = 0.
        lw_upload-k = lv_scale_id.
      ENDIF.
    ENDIF.

    " Date fields - Convert from API format (YYYY-MM-DD) to SAP format (YYYYMMDD)
    " Field for DATAB (valid from date)
    IF lw_json_header-validity_from IS NOT INITIAL.
      lv_date_str = lw_json_header-validity_from.
      " Remove dashes
      REPLACE ALL OCCURRENCES OF '-' IN lv_date_str WITH ''.
      " Convert to date
      lv_date = lv_date_str.
      " Find column mapping for DATAB
      READ TABLE lt_colm_map INTO lw_colm_map
                             WITH KEY zfield = 'DATAB'
                             BINARY SEARCH.
      IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
        UNASSIGN <lfs_value>.
        ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
        IF <lfs_value> IS ASSIGNED.
          <lfs_value> = lv_date.
        ENDIF.
      ENDIF.
    ENDIF.

    " Field for DATBI (valid to date)
    IF lw_json_header-validity_to IS NOT INITIAL.
      lv_date_str = lw_json_header-validity_to.
      " Remove dashes
      REPLACE ALL OCCURRENCES OF '-' IN lv_date_str WITH ''.
      " Convert to date
      lv_date = lv_date_str.
      " Find column mapping for DATBI
      READ TABLE lt_colm_map INTO lw_colm_map
                             WITH KEY zfield = 'DATBI'
                             BINARY SEARCH.
      IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
        UNASSIGN <lfs_value>.
        ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
        IF <lfs_value> IS ASSIGNED.
          <lfs_value> = lv_date.
        ENDIF.
      ENDIF.
    ENDIF.

    " Rate and Currency fields
    IF lw_json_header-rate IS NOT INITIAL.
      lv_rate_str = lw_json_header-rate.
      CONDENSE lv_rate_str.
      " Convert string to packed number
      lv_rate = lv_rate_str.
      " Find column mapping for KBETR
      READ TABLE lt_colm_map INTO lw_colm_map
                             WITH KEY zfield = 'KBETR'
                             BINARY SEARCH.
      IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
        UNASSIGN <lfs_value>.
        ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
        IF <lfs_value> IS ASSIGNED.
          <lfs_value> = lv_rate.
        ENDIF.
      ENDIF.
    ENDIF.

    IF lw_json_header-currency IS NOT INITIAL.
      lv_currency = lw_json_header-currency.
      " Find column mapping for KONWA
      READ TABLE lt_colm_map INTO lw_colm_map
                             WITH KEY zfield = 'KONWA'
                             BINARY SEARCH.
      IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
        UNASSIGN <lfs_value>.
        ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
        IF <lfs_value> IS ASSIGNED.
          <lfs_value> = lv_currency.
        ENDIF.
      ENDIF.
    ENDIF.

    " Map additional fields based on column mapping
    " This is a simplified version - full implementation would map all fields
    " from JSON to GTY_UPLOAD based on ZLOG_COLM_MAP configuration

    " Append to upload table
    APPEND lw_upload TO lt_upload.

  ENDLOOP.

  " Validate we have records to process
  DESCRIBE TABLE lt_upload LINES lv_total_records.
  ev_total_records = lv_total_records.

  IF lv_total_records = 0.
    RAISE conversion_error.
  ENDIF.

  " Step 5: Call existing Function Module Z_SCM_RATE_UPLOAD_IMPFF
  CLEAR: lt_result.

  TRY.
      CALL FUNCTION 'Z_SCM_RATE_UPLOAD_IMPFF'
        EXPORTING
          iv_chng       = lv_chng
          iv_batch_size = lv_batch_size
          iv_test_mode  = lv_test_mode
        IMPORTING
          ev_total_records = lv_total_records
          ev_success_count = lv_success_count
          ev_error_count   = lv_error_count
        TABLES
          it_upload = lt_upload
          it_xldata = lt_xldata
          et_result = lt_result
        EXCEPTIONS
          invalid_input     = 1
          validation_error = 2
          processing_error  = 3
          OTHERS            = 4.

      IF sy-subrc <> 0.
        CASE sy-subrc.
          WHEN 1.
            RAISE conversion_error.
          WHEN 2.
            RAISE conversion_error.
          WHEN 3.
            RAISE processing_error.
          WHEN OTHERS.
            RAISE processing_error.
        ENDCASE.
      ENDIF.

    CATCH cx_root INTO lo_exception.
      lv_conversion_error = lo_exception->get_text( ).
      RAISE processing_error.
  ENDTRY.

  " Step 6: Copy results to export table
  LOOP AT lt_result INTO lw_result.
    CLEAR et_result.
    et_result-record_number = lw_result-record_number.
    et_result-status = lw_result-status.
    et_result-message_type = lw_result-message_type.
    et_result-message_id = lw_result-message_id.
    et_result-message_number = lw_result-message_number.
    et_result-message_text = lw_result-message_text.
    et_result-condition_number = lw_result-condition_number.
    et_result-error_details = lw_result-error_details.
    et_result-flag = lw_result-flag.
    et_result-reason = lw_result-reason.
    APPEND et_result.
  ENDLOOP.

  " Set export values
  ev_total_records = lv_total_records.
  ev_success_count = lv_success_count.
  ev_error_count = lv_error_count.

ENDFUNCTION.
" END: Cursor Generated Code

