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
*& - Scenario-based dynamic field mapping via ZSCM_RFQ_JSONMAP
*& - Charge code to condition type mapping via ZSCM_RFQ_CHRGMAP
*& - Data conversion from JSON to GTY_UPLOAD structure using mapping tables
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

  TYPES: BEGIN OF lty_json_field_map,
           zscenr TYPE char1,
           json_field TYPE char40,
           upload_field TYPE char3,
           sap_field TYPE fieldname,
           conversion TYPE char10,
           mandatory TYPE c,
           active TYPE c,
         END OF lty_json_field_map.

  TYPES: BEGIN OF lty_t685,
           kschl TYPE kschl,
           kozgf TYPE kozgf,
         END OF lty_t685.

  TYPES: BEGIN OF lty_t682i,
           kozgf TYPE kozgf,
           kolnr TYPE kolnr,
           kotabnr TYPE kotabnr,
         END OF lty_t682i.

  TYPES: BEGIN OF lty_t681e,
           kvewe TYPE kvewe,
           kotabnr TYPE kotabnr,
           fsetyp TYPE fsetyp,
           fselnr TYPE fselnr,
           sefeld TYPE sefeld,
           fsetxt TYPE fsetxt,
         END OF lty_t681e.

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
        lv_scale_id TYPE char18,
        lv_scenario TYPE char1,
        lv_json_field_name TYPE string,
        lv_upload_field_name TYPE char3,
        lv_conversion_rule TYPE char10,
        lv_field_value TYPE string,
        lv_kotabnr TYPE kotabnr,
        lv_kolnr TYPE kolnr,
        lv_kozgf TYPE kozgf,
        lv_match_count TYPE i,
        lv_max_match_count TYPE i,
        lv_best_kotabnr TYPE kotabnr,
        lv_best_kolnr TYPE kolnr,
        lv_field_found TYPE c,
        lv_json_field_value TYPE string.

  DATA: lo_json TYPE REF TO /ui2/cl_json,
        lo_exception TYPE REF TO cx_root,
        lv_json_parsed TYPE c.

  DATA: lt_charge_code_map TYPE STANDARD TABLE OF lty_charge_code_map,
        lt_json_header TYPE STANDARD TABLE OF lty_json_header,
        lt_upload TYPE STANDARD TABLE OF lty_upload,
        lt_xldata TYPE STANDARD TABLE OF lty_xldata,
        lt_colm_map TYPE STANDARD TABLE OF lty_colm_map,
        lt_json_field_map TYPE STANDARD TABLE OF lty_json_field_map,
        lt_result TYPE STANDARD TABLE OF zlog_rate_upload_result,
        lt_t685 TYPE STANDARD TABLE OF lty_t685,
        lt_t682i TYPE STANDARD TABLE OF lty_t682i,
        lt_t682i_filter TYPE STANDARD TABLE OF lty_t682i,
        lt_t681e TYPE STANDARD TABLE OF lty_t681e,
        lt_t681e_filter TYPE STANDARD TABLE OF lty_t681e.

  DATA: lw_charge_code_map TYPE lty_charge_code_map,
        lw_json_header TYPE lty_json_header,
        lw_upload TYPE lty_upload,
        lw_xldata TYPE lty_xldata,
        lw_colm_map TYPE lty_colm_map,
        lw_json_field_map TYPE lty_json_field_map,
        lw_json_payload TYPE lty_json_payload,
        lw_result TYPE zlog_rate_upload_result,
        lw_t685 TYPE lty_t685,
        lw_t682i TYPE lty_t682i,
        lw_t681e TYPE lty_t681e.

  " Field symbols
  FIELD-SYMBOLS: <lfs_upload> TYPE lty_upload,
                 <lfs_value> TYPE any,
                 <lfs_json_field> TYPE any,
                 <lfs_upload_field> TYPE any.

  " Constants
  CONSTANTS: lc_scenario_e TYPE char1 VALUE 'E',
             lc_x TYPE c VALUE 'X',
             lc_active TYPE c VALUE 'X',
             lc_success TYPE c VALUE 'S',
             lc_error TYPE c VALUE 'E',
             lc_warning TYPE c VALUE 'W',
             lc_date_format_api TYPE string VALUE 'YYYY-MM-DD',
             lc_date_format_sap TYPE string VALUE 'YYYYMMDD',
             lc_conv_alpha TYPE char10 VALUE 'ALPHA',
             lc_conv_date TYPE char10 VALUE 'DATE',
             lc_conv_number TYPE char10 VALUE 'NUMBER',
             lc_conv_none TYPE char10 VALUE 'NONE',
             lc_f TYPE kappl VALUE 'F',
             lc_a TYPE kvewe VALUE 'A'.

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

  " Step 2: Determine scenario from JSON payload
  " Map service_product to scenario (default to 'E' if not mapped)
  " TODO: Create mapping table or use service_product directly if it contains scenario
  IF lw_json_payload-service_product IS NOT INITIAL.
    " For now, default to scenario 'E'
    " In future, can add a mapping table: service_product -> scenario
    lv_scenario = lc_scenario_e.
  ELSE.
    " Default to scenario 'E'
    lv_scenario = lc_scenario_e.
  ENDIF.

  " Step 3: Load JSON field mapping table for the determined scenario
  SELECT zscenr
         json_field
         upload_field
         sap_field
         conversion
         mandatory
         active
    FROM zscm_rfq_jsonmap
    INTO TABLE lt_json_field_map
    WHERE zscenr = lv_scenario
      AND active = lc_active.

  IF sy-subrc <> 0.
    " JSON field mapping not found - this is an error
    CLEAR lw_result.
    lw_result-status = lc_error.
    lw_result-message_type = 'E'.
    lw_result-message_id = 'ZLOG'.
    lw_result-message_number = '002'.
    CONCATENATE 'JSON field mapping not found for scenario:' lv_scenario
                INTO lw_result-message_text SEPARATED BY space.
    lw_result-error_details = lw_result-message_text.
    APPEND lw_result TO lt_result.
    RAISE mapping_error.
  ENDIF.

  SORT lt_json_field_map BY json_field.

  " Step 4: Load charge code mapping table
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

  " Step 5: Load column mapping for the determined scenario
  SELECT zscenr
         zcolno
         zfield
         zdeser
    FROM zlog_colm_map
    INTO TABLE lt_colm_map
    WHERE zscenr = lv_scenario.

  IF sy-subrc <> 0.
    " Column mapping not found - will use default mappings
  ENDIF.

  SORT lt_colm_map BY zfield.

  " Step 6: Convert JSON records to GTY_UPLOAD structure using dynamic mapping
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

    " Step 6.1.1: Derive KOTABNR (condition table number) from access sequence
    " Load access sequence (KOZGF) for the KSCHL
    CLEAR: lw_t685, lv_kozgf, lv_kotabnr, lv_kolnr.
    READ TABLE lt_t685 INTO lw_t685
                       WITH KEY kschl = lv_kschl
                       BINARY SEARCH.
    
    IF sy-subrc = 0 AND lw_t685-kozgf IS NOT INITIAL.
      lv_kozgf = lw_t685-kozgf.
      
      " Load condition tables (KOTABNR) for the access sequence
      CLEAR: lt_t682i_filter.
      lt_t682i_filter[] = lt_t682i[].
      DELETE lt_t682i_filter WHERE kozgf <> lv_kozgf.
      
      IF lt_t682i_filter IS NOT INITIAL.
        " If multiple condition tables exist, find the best match
        DESCRIBE TABLE lt_t682i_filter LINES lv_match_count.
        
        IF lv_match_count > 1.
          " Multiple condition tables - find best match based on available JSON fields
          CLEAR: lv_max_match_count, lv_best_kotabnr, lv_best_kolnr.
          
          LOOP AT lt_t682i_filter INTO lw_t682i.
            " Get field selection for this condition table
            CLEAR: lt_t681e_filter.
            lt_t681e_filter[] = lt_t681e[].
            DELETE lt_t681e_filter WHERE kotabnr <> lw_t682i-kotabnr.
            DELETE lt_t681e_filter WHERE kvewe <> lc_a.
            
            " Count how many required fields have values in JSON
            CLEAR: lv_match_count.
            LOOP AT lt_t681e_filter INTO lw_t681e.
              IF lw_t681e-sefeld IS NOT INITIAL.
                " Check if this SAP field has a corresponding JSON field with value
                " First, find the JSON field mapping for this SAP field
                READ TABLE lt_json_field_map INTO lw_json_field_map
                                              WITH KEY sap_field = lw_t681e-sefeld
                                              BINARY SEARCH.
                IF sy-subrc = 0 AND lw_json_field_map-json_field IS NOT INITIAL.
                  " Check if JSON field has a value
                  CLEAR: lv_json_field_value, lv_field_found.
                  CASE lw_json_field_map-json_field.
                    WHEN 'vendor'.
                      IF lw_json_header-vendor IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-vendor.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'accountid'.
                      IF lw_json_payload-accountid IS NOT INITIAL.
                        lv_json_field_value = lw_json_payload-accountid.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'origin'.
                      IF lw_json_header-origin IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-origin.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'destination'.
                      IF lw_json_header-destination IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-destination.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'scale_id'.
                      IF lw_json_header-scale_id IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-scale_id.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'validity_from'.
                      IF lw_json_header-validity_from IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-validity_from.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'validity_to'.
                      IF lw_json_header-validity_to IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-validity_to.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'rate'.
                      IF lw_json_header-rate IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-rate.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'currency'.
                      IF lw_json_header-currency IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-currency.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'service_type'.
                      IF lw_json_header-service_type IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-service_type.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN 'uom'.
                      IF lw_json_header-uom IS NOT INITIAL.
                        lv_json_field_value = lw_json_header-uom.
                        lv_field_found = lc_x.
                      ENDIF.
                    WHEN OTHERS.
                      " Try to get value from JSON header structure
                      " This is a simplified approach - can be enhanced
                  ENDCASE.
                  
                  IF lv_field_found = lc_x AND lv_json_field_value IS NOT INITIAL.
                    lv_match_count = lv_match_count + 1.
                  ENDIF.
                ENDIF.
              ENDIF.
            ENDLOOP.
            
            " Track the condition table with maximum matching fields
            IF lv_match_count > lv_max_match_count.
              lv_max_match_count = lv_match_count.
              lv_best_kotabnr = lw_t682i-kotabnr.
              lv_best_kolnr = lw_t682i-kolnr.
            ENDIF.
          ENDLOOP.
          
          " Use the best matching condition table
          IF lv_best_kotabnr IS NOT INITIAL.
            lv_kotabnr = lv_best_kotabnr.
            lv_kolnr = lv_best_kolnr.
          ELSE.
            " If no match found, use the first one (by sequence)
            SORT lt_t682i_filter BY kolnr.
            READ TABLE lt_t682i_filter INTO lw_t682i INDEX 1.
            IF sy-subrc = 0.
              lv_kotabnr = lw_t682i-kotabnr.
              lv_kolnr = lw_t682i-kolnr.
            ENDIF.
          ENDIF.
        ELSE.
          " Single condition table - use it directly
          READ TABLE lt_t682i_filter INTO lw_t682i INDEX 1.
          IF sy-subrc = 0.
            lv_kotabnr = lw_t682i-kotabnr.
            lv_kolnr = lw_t682i-kolnr.
          ENDIF.
        ENDIF.
      ENDIF.
    ENDIF.

    " Step 6.2: Convert JSON fields to GTY_UPLOAD structure using dynamic mapping
    CLEAR lw_upload.

    " First, map charge_code to KSCHL (field 'b') - this is special handling
    " Charge code is already mapped to KSCHL in Step 6.1
    READ TABLE lt_json_field_map INTO lw_json_field_map
                                  WITH KEY json_field = 'charge_code'
                                  BINARY SEARCH.
    IF sy-subrc = 0 AND lw_json_field_map-upload_field IS NOT INITIAL.
      lv_upload_field_name = lw_json_field_map-upload_field.
      UNASSIGN <lfs_upload_field>.
      ASSIGN COMPONENT lv_upload_field_name OF STRUCTURE lw_upload TO <lfs_upload_field>.
      IF <lfs_upload_field> IS ASSIGNED.
        <lfs_upload_field> = lv_kschl.
      ENDIF.
    ELSE.
      " Default to field 'b' if mapping not found
      lw_upload-b = lv_kschl.
    ENDIF.

    " Map KOTABNR sequence number (KOLNR) if derived
    " Note: The sequence number (KOLNR) is stored in the upload structure
    " This is used by Z_SCM_RATE_UPLOAD_IMPFF to select the correct KOTABNR
    " when multiple condition tables exist for the same access sequence
    IF lv_kolnr IS NOT INITIAL.
      READ TABLE lt_colm_map INTO lw_colm_map
                             WITH KEY zfield = 'KOTABNR'
                             BINARY SEARCH.
      IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
        UNASSIGN <lfs_value>.
        ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
        IF <lfs_value> IS ASSIGNED.
          " Store the sequence number (KOLNR) as index for KOTABNR selection
          " This matches the logic in Z_SCM_RATE_UPLOAD_IMPFF which uses this
          " index to select the correct KOTABNR from the filtered list
          <lfs_value> = lv_kolnr.
        ENDIF.
      ENDIF.
    ENDIF.

    " Now map all other JSON fields dynamically
    " Map vendor field
    IF lw_json_header-vendor IS NOT INITIAL.
      lv_json_field_name = 'vendor'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-upload_field IS NOT INITIAL.
        lv_upload_field_name = lw_json_field_map-upload_field.
        lv_field_value = lw_json_header-vendor.
        lv_conversion_rule = lw_json_field_map-conversion.
        UNASSIGN <lfs_upload_field>.
        ASSIGN COMPONENT lv_upload_field_name OF STRUCTURE lw_upload TO <lfs_upload_field>.
        IF <lfs_upload_field> IS ASSIGNED.
          " Apply conversion if needed
          IF lv_conversion_rule = lc_conv_alpha.
            CALL FUNCTION 'CONVERSION_EXIT_ALPHA_INPUT'
              EXPORTING
                input  = lv_field_value
              IMPORTING
                output = lv_field_value.
            IF sy-subrc = 0.
              <lfs_upload_field> = lv_field_value.
            ENDIF.
          ELSE.
            <lfs_upload_field> = lv_field_value.
          ENDIF.
        ENDIF.
      ENDIF.
    ELSEIF lw_json_payload-accountid IS NOT INITIAL.
      " Use accountid as vendor if vendor not found
      lv_json_field_name = 'accountid'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-upload_field IS NOT INITIAL.
        lv_upload_field_name = lw_json_field_map-upload_field.
        lv_field_value = lw_json_payload-accountid.
        lv_conversion_rule = lw_json_field_map-conversion.
        UNASSIGN <lfs_upload_field>.
        ASSIGN COMPONENT lv_upload_field_name OF STRUCTURE lw_upload TO <lfs_upload_field>.
        IF <lfs_upload_field> IS ASSIGNED.
          " Apply conversion if needed
          IF lv_conversion_rule = lc_conv_alpha.
            CALL FUNCTION 'CONVERSION_EXIT_ALPHA_INPUT'
              EXPORTING
                input  = lv_field_value
              IMPORTING
                output = lv_field_value.
            IF sy-subrc = 0.
              <lfs_upload_field> = lv_field_value.
            ENDIF.
          ELSE.
            <lfs_upload_field> = lv_field_value.
          ENDIF.
        ENDIF.
      ENDIF.
    ENDIF.

    " Map scale_id field
    IF lw_json_header-scale_id IS NOT INITIAL.
      lv_json_field_name = 'scale_id'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-upload_field IS NOT INITIAL.
        lv_upload_field_name = lw_json_field_map-upload_field.
        lv_field_value = lw_json_header-scale_id.
        lv_conversion_rule = lw_json_field_map-conversion.
        UNASSIGN <lfs_upload_field>.
        ASSIGN COMPONENT lv_upload_field_name OF STRUCTURE lw_upload TO <lfs_upload_field>.
        IF <lfs_upload_field> IS ASSIGNED.
          " Apply conversion if needed
          IF lv_conversion_rule = lc_conv_alpha.
            CALL FUNCTION 'CONVERSION_EXIT_ALPHA_INPUT'
              EXPORTING
                input  = lv_field_value
              IMPORTING
                output = lv_field_value.
            IF sy-subrc = 0.
              <lfs_upload_field> = lv_field_value.
            ENDIF.
          ELSE.
            <lfs_upload_field> = lv_field_value.
          ENDIF.
        ENDIF.
      ENDIF.
    ENDIF.

    " Map origin field
    IF lw_json_header-origin IS NOT INITIAL.
      lv_json_field_name = 'origin'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-upload_field IS NOT INITIAL.
        lv_upload_field_name = lw_json_field_map-upload_field.
        lv_field_value = lw_json_header-origin.
        UNASSIGN <lfs_upload_field>.
        ASSIGN COMPONENT lv_upload_field_name OF STRUCTURE lw_upload TO <lfs_upload_field>.
        IF <lfs_upload_field> IS ASSIGNED.
          <lfs_upload_field> = lv_field_value.
        ENDIF.
      ENDIF.
    ENDIF.

    " Map destination field
    IF lw_json_header-destination IS NOT INITIAL.
      lv_json_field_name = 'destination'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-upload_field IS NOT INITIAL.
        lv_upload_field_name = lw_json_field_map-upload_field.
        lv_field_value = lw_json_header-destination.
        UNASSIGN <lfs_upload_field>.
        ASSIGN COMPONENT lv_upload_field_name OF STRUCTURE lw_upload TO <lfs_upload_field>.
        IF <lfs_upload_field> IS ASSIGNED.
          <lfs_upload_field> = lv_field_value.
        ENDIF.
      ENDIF.
    ENDIF.

    " Map fields that require column mapping (DATAB, DATBI, KBETR, KONWA, etc.)
    " These fields are mapped via ZLOG_COLM_MAP to determine the GTY_UPLOAD column
    " Date fields - Convert from API format (YYYY-MM-DD) to SAP format (YYYYMMDD)
    IF lw_json_header-validity_from IS NOT INITIAL.
      lv_json_field_name = 'validity_from'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-sap_field IS NOT INITIAL.
        " Find column mapping for the SAP field
        READ TABLE lt_colm_map INTO lw_colm_map
                               WITH KEY zfield = lw_json_field_map-sap_field
                               BINARY SEARCH.
        IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
          lv_date_str = lw_json_header-validity_from.
          " Remove dashes
          REPLACE ALL OCCURRENCES OF '-' IN lv_date_str WITH ''.
          " Convert to date
          lv_date = lv_date_str.
          UNASSIGN <lfs_value>.
          ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
          IF <lfs_value> IS ASSIGNED.
            <lfs_value> = lv_date.
          ENDIF.
        ENDIF.
      ENDIF.
    ENDIF.

    " Field for DATBI (valid to date)
    IF lw_json_header-validity_to IS NOT INITIAL.
      lv_json_field_name = 'validity_to'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-sap_field IS NOT INITIAL.
        " Find column mapping for the SAP field
        READ TABLE lt_colm_map INTO lw_colm_map
                               WITH KEY zfield = lw_json_field_map-sap_field
                               BINARY SEARCH.
        IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
          lv_date_str = lw_json_header-validity_to.
          " Remove dashes
          REPLACE ALL OCCURRENCES OF '-' IN lv_date_str WITH ''.
          " Convert to date
          lv_date = lv_date_str.
          UNASSIGN <lfs_value>.
          ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
          IF <lfs_value> IS ASSIGNED.
            <lfs_value> = lv_date.
          ENDIF.
        ENDIF.
      ENDIF.
    ENDIF.

    " Rate field - mapped via ZLOG_COLM_MAP
    IF lw_json_header-rate IS NOT INITIAL.
      lv_json_field_name = 'rate'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-sap_field IS NOT INITIAL.
        " Find column mapping for the SAP field
        READ TABLE lt_colm_map INTO lw_colm_map
                               WITH KEY zfield = lw_json_field_map-sap_field
                               BINARY SEARCH.
        IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
          lv_rate_str = lw_json_header-rate.
          CONDENSE lv_rate_str.
          " Convert string to packed number
          lv_rate = lv_rate_str.
          UNASSIGN <lfs_value>.
          ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
          IF <lfs_value> IS ASSIGNED.
            <lfs_value> = lv_rate.
          ENDIF.
        ENDIF.
      ENDIF.
    ENDIF.

    " Currency field - mapped via ZLOG_COLM_MAP
    IF lw_json_header-currency IS NOT INITIAL.
      lv_json_field_name = 'currency'.
      READ TABLE lt_json_field_map INTO lw_json_field_map
                                    WITH KEY json_field = lv_json_field_name
                                    BINARY SEARCH.
      IF sy-subrc = 0 AND lw_json_field_map-sap_field IS NOT INITIAL.
        " Find column mapping for the SAP field
        READ TABLE lt_colm_map INTO lw_colm_map
                               WITH KEY zfield = lw_json_field_map-sap_field
                               BINARY SEARCH.
        IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
          lv_currency = lw_json_header-currency.
          UNASSIGN <lfs_value>.
          ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
          IF <lfs_value> IS ASSIGNED.
            <lfs_value> = lv_currency.
          ENDIF.
        ENDIF.
      ENDIF.
    ENDIF.

    " Map other JSON fields dynamically
    " Loop through JSON field mapping table and map remaining fields
    LOOP AT lt_json_field_map INTO lw_json_field_map.
      " Skip already mapped fields
      IF lw_json_field_map-json_field = 'charge_code' OR
         lw_json_field_map-json_field = 'vendor' OR
         lw_json_field_map-json_field = 'accountid' OR
         lw_json_field_map-json_field = 'scale_id' OR
         lw_json_field_map-json_field = 'origin' OR
         lw_json_field_map-json_field = 'destination' OR
         lw_json_field_map-json_field = 'validity_from' OR
         lw_json_field_map-json_field = 'validity_to' OR
         lw_json_field_map-json_field = 'rate' OR
         lw_json_field_map-json_field = 'currency'.
        CONTINUE.
      ENDIF.

      " Get JSON field value based on field name
      CLEAR lv_field_value.
      CASE lw_json_field_map-json_field.
        WHEN 'service_type'.
          IF lw_json_header-service_type IS NOT INITIAL.
            lv_field_value = lw_json_header-service_type.
          ENDIF.
        WHEN 'uom'.
          IF lw_json_header-uom IS NOT INITIAL.
            lv_field_value = lw_json_header-uom.
          ENDIF.
        WHEN 'min_value'.
          IF lw_json_header-min_value IS NOT INITIAL.
            lv_field_value = lw_json_header-min_value.
          ENDIF.
        WHEN 'max_value'.
          IF lw_json_header-max_value IS NOT INITIAL.
            lv_field_value = lw_json_header-max_value.
          ENDIF.
        WHEN 'scale_value'.
          IF lw_json_header-scale_value IS NOT INITIAL.
            lv_field_value = lw_json_header-scale_value.
          ENDIF.
        WHEN OTHERS.
          " Unknown field - skip
          CONTINUE.
      ENDCASE.

      IF lv_field_value IS NOT INITIAL.
        CONDENSE lv_field_value.

        " If SAP field is specified, use column mapping
        IF lw_json_field_map-sap_field IS NOT INITIAL.
          READ TABLE lt_colm_map INTO lw_colm_map
                                 WITH KEY zfield = lw_json_field_map-sap_field
                                 BINARY SEARCH.
          IF sy-subrc = 0 AND lw_colm_map-zcolno IS NOT INITIAL.
            UNASSIGN <lfs_value>.
            ASSIGN COMPONENT lw_colm_map-zcolno OF STRUCTURE lw_upload TO <lfs_value>.
            IF <lfs_value> IS ASSIGNED.
              " Apply conversion if needed
              IF lw_json_field_map-conversion = lc_conv_date.
                " Date conversion
                REPLACE ALL OCCURRENCES OF '-' IN lv_field_value WITH ''.
                <lfs_value> = lv_field_value.
              ELSEIF lw_json_field_map-conversion = lc_conv_number.
                " Number conversion
                lv_rate = lv_field_value.
                <lfs_value> = lv_rate.
              ELSEIF lw_json_field_map-conversion = lc_conv_alpha.
                " Alpha conversion
                CALL FUNCTION 'CONVERSION_EXIT_ALPHA_INPUT'
                  EXPORTING
                    input  = lv_field_value
                  IMPORTING
                    output = lv_field_value.
                IF sy-subrc = 0.
                  <lfs_value> = lv_field_value.
                ENDIF.
              ELSE.
                " No conversion
                <lfs_value> = lv_field_value.
              ENDIF.
            ENDIF.
          ENDIF.
        ELSEIF lw_json_field_map-upload_field IS NOT INITIAL.
          " Direct mapping to GTY_UPLOAD field
          lv_upload_field_name = lw_json_field_map-upload_field.
          UNASSIGN <lfs_upload_field>.
          ASSIGN COMPONENT lv_upload_field_name OF STRUCTURE lw_upload TO <lfs_upload_field>.
          IF <lfs_upload_field> IS ASSIGNED.
            " Apply conversion if needed
            IF lw_json_field_map-conversion = lc_conv_alpha.
              CALL FUNCTION 'CONVERSION_EXIT_ALPHA_INPUT'
                EXPORTING
                  input  = lv_field_value
                IMPORTING
                  output = lv_field_value.
              IF sy-subrc = 0.
                <lfs_upload_field> = lv_field_value.
              ENDIF.
            ELSE.
              <lfs_upload_field> = lv_field_value.
            ENDIF.
          ENDIF.
        ENDIF.
      ENDIF.
    ENDLOOP.

    " Append to upload table
    APPEND lw_upload TO lt_upload.

  ENDLOOP.

  " Validate we have records to process
  DESCRIBE TABLE lt_upload LINES lv_total_records.
  ev_total_records = lv_total_records.

  IF lv_total_records = 0.
    RAISE conversion_error.
  ENDIF.

  " Step 7: Call existing Function Module Z_SCM_RATE_UPLOAD_IMPFF
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

  " Step 8: Copy results to export table
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

