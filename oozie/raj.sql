WITH A01 as (
SELECT *
FROM`prj--53184-eap-dev.eap_dev.MEAPA01_TXN_HIST_TBL`
),
A04_TEMP as (
SELECT
a01.EAPA01_MKI_BRAND_N,      # correct
A01.EAPA01_MKI_MODEL_N,
A01.EAPA01_MKI_SEGMENT_N,
A01.EAPA01_MKI_MODEL_YEAR_R,
A01.EAPA01_MKI_VEH_SALE_MKT_C,

A01.EAPA01_ORD_D,
A01.EAPA01_EVNT_TYPE_C,
A01.EAPA01_ORD_LAST_MOD_S,
A01.EAPA01_DLR_BRAND_C,
A01.EAPA01_DLR_PA_C,
A01.EAPA01_DLR_SLS_C,
A01.EAPA01_DLR_N,
A01.EAPA01_DLR_ADD_TOWN_X,
A01.EAPA01_DLR_ADD_LN1_X,
A01.EAPA01_DLR_PHONE_R,
A01.EAPA01_DLR_STATE_C,
A09.EAPA09_STATE_PROV_N,
A01.EAPA01_DLR_ZIP_C,
A01.EAPA01_DLR_EMAIL_X,
A01.EAPA01_DLR_CNTRY_ISO_C,
A01.EAPA01_DLR_CNTRY_NAME_N,
A01.EAPA01_PRD_CTLG_D,
A01.EAPA01_PRD_IMAGE_U,
A04.EAPA04_WERS_FAM_C,
A04.EAPA04_WERS_FTR_C,
A04.EAPA04_WERS_FTR_X,
row_number() over(partition by A01.EAPA01_EVNT_TYPE_C, A01.EAPA01_ORD_LAST_MOD_S order by A01.EAPA01_ORD_LAST_MOD_S desc) as row_num
FROM A01
 LEFT JOIN `prj--53184-eap-dev.eap_dev.MEAPA04_WERS_FTR_HIST_TBL` A04
 ON A01.EAPA01_ORD_D = A04.EAPA01_ORD_D
	AND A01.EAPA01_EVNT_TYPE_C = A04.EAPA01_EVNT_TYPE_C
	AND A01.EAPA01_ORD_LAST_MOD_S = A04.EAPA01_ORD_LAST_MOD_S
LEFT JOIN `prj--53184-eap-dev.eap_dev.MEAPA09_STATE_PROVINCE_TBL` A09
	ON A01.EAPA01_DLR_CNTRY_C = A09.EAPA08_CNTRY_C
	AND A01.EAPA01_DLR_STATE_C = A09.EAPA09_STATE_PROV_C
)

SELECT
A01.EAPA01_ORD_D as gborec01_code,
A01.EAPA01_ORD_LAST_MOD_S as gborec01_last_modified_utc_timestamp,
A01.EAPA01_ORD_TYPE_C as gborec01_order_type,
A01.EAPA01_EVNT_TYPE_C as gborec01_event_type,
CAST(NULL AS STRING) as gborec01_total_feed_count,
CAST(NULL AS STRING) as gborec01_batch_quantity,
-- gborec02_vendor_feed_data
  ARRAY
		(
			SELECT
					AS STRUCT
							(
									SELECT
											AS STRUCT
															CAST(NULL AS STRING) gborec03_cart_checkout_na_ready_flag,
															CAST(NULL AS STRING)   gborec03_isdealer_emp_enrolled,
															CAST(NULL AS STRING)   gborec03_isdealer_resv_enrolled,
															CAST(NULL AS STRING)   gborec03_reservation_na_ready_flag,
															CAST(NULL AS STRING)   gborec03_bevna_enrolled_flag,
															CAST(NULL AS STRING)   gborec03_cart_checkout_na_ready_flag_ev,
															CAST(NULL AS STRING)   gborec03_is_dealer_order_enrolled,
															CAST(NULL AS STRING)   gborec03_is_deactivated_na_flag,
															CAST(NULL AS STRING)   gborec03_order_na_ready_flag,
															CAST(NULL AS STRING)   gborec03_ev_eu_certification_flag,
															CAST(NULL AS STRING)   gborec03_delivery_preferred_flag,
															CAST(NULL AS STRING)  gborec03_ald_flag,
															CAST(NULL AS STRING)  gborec03_dealer_wallbox_enrolled,
															CAST(NULL AS STRING)  gborec03_remote_delivery_na_flag
							)
							AS gborec03_eligibility_flags,
							CAST(NULL AS STRING) gborec02_dealerid
		) as gborec02_vendor_feed_data,

STRUCT(A01.EAPA01_REGN_C as gborec04_region_code,
A07.EAPA07_REGN_N as gborec04_region_name,
A01.EAPA01_CNTRY_C as gborec04_country_code,
A08.EAPA08_CNTRY_N as gborec04_country_name,
A01.EAPA01_CNTRCT_D as gborec04_contract_id,
A01.EAPA01_CNTRCT_STAT_C as gborec04_contract_status,
Safe_cast(A01.EAPA01_VEH_SOLD_Y as date) as gborec04_vehicle_sold_date,
A01.EAPA01_DEAL_JACKET_D as gborec04_deal_jacket_id,
Safe_cast(A01.EAPA01_CUSTOMS_CLRN_Y AS date) as gborec04_custom_clearance_date,
A01.EAPA01_DLR_ORD_D as gborec04_dealer_order_id,
A01.EAPA01_VEH_BODY_C as gborec04_body_type,
A01.EAPA01_ORBIT_R as gborec04_orbit_number,
 CAST(A01.EAPA01_PRE_APPVL_A AS NUMERIC) as gborec04_preapproval_rndup_amount,
Safe_cast(A01.EAPA01_PRE_APPVL_EXP_S AS TIMESTAMP) as gborec04_preapproval_expiry_utc_timestamp,
CAST(NULL AS DATE) as gborec04_preapproval_expiry_est,
A01.EAPA01_ADDITONAL_VIN_C as gborec04_additional_vin,
A01.EAPA01_ORD_PL_LANG_C as gborec04_order_placed_language,
A01.EAPA01_ORD_PLACED_MKT_D as gborec04_order_placed_market,
A01.EAPA01_DERIVED_CART_ID_D as gborec04_derived_cart_id,
A01.EAPA01_ORD_STAT_C as gborec04_status,
Safe_cast(A01.EAPA01_ORD_CRT_S AS TIMESTAMP) as gborec04_created_utc_timestamp,
PARSE_DATE('%d/%m/%y',A01.EAPA01_PRE_APPVL_EXP_EST_Y) AS gborec04_created_est,
CAST(NULL AS STRING) as gborec04_region,
A01.EAPA01_CHNL_X as gborec04_channel,
A01.EAPA01_MODEM_CONSENT_F as gborec04_modem_auth_consent,
Safe_cast(A01.EAPA01_PURCH_REQ_S AS TIMESTAMP) as gborec04_purchase_requested_date_utc_timestamp,
PARSE_DATE('%d/%m/%y', A01.EAPA01_PURCH_REQ_EST_Y) AS gborec04_purchase_requested_date_est,
A01.EAPA01_REQ_STAT_C as gborec04_request_status,
A01.EAPA01_DLR_CANC_REASON_X as gborec04_dealer_cancellation_reason,
 A01.EAPA01_PMT_ISO_C as gborec04_payment_iso_code,
CAST(A01.EAPA01_PMT_AMT_A AS NUMERIC) as gborec04_payment_amount,
CAST(NULL AS STRING) as gborec04_protection_plan_data,
A01.EAPA01_GRPBY_D as gborec04_groupby_id,
STRUCT(CAST(A01.EAPA01_USR_D AS STRING) as gborec05_uid,CAST(A01.EAPA01_USERTYPE_X AS STRING) AS gborec05_user_type ) AS gborec05_user,
STRUCT(CAST(A01.EAPA01_PYMT_MODE_X AS STRING) as gborec06_payment_mode,STRUCT(CAST(NULL AS STRING) as gborec07_payment_type_description)as gborec07_payment_info) as gborec06_payment,
STRUCT(CAST(NULL AS STRING) as gborec08_summary) as gborec08_price_summary,
STRUCT(CAST(A01.EAPA01_PYMT_TYPE_X AS STRING) as gborec09_payment_type)as gborec09_payment_method,
STRUCT(A01.EAPA01_PRICE_ISO_CURRENCY_A as gborec10_currency_iso,A01.EAPA01_PRICE_ISO_CURRENCY_C as gborec10_currency_name,CAST(NULL AS NUMERIC) as gborec10_value) as gborec10_price,

-- gborec04_data.gborec11_detailed_price_summary
STRUCT(CAST(NULL AS STRING) as gborec11_nondealerized_price,

STRUCT(CAST(A01.EAPA01_DPR_TOT_DLR_A AS NUMERIC) as gborec12_total_dealer_price,
CAST(A01.EAPA01_DPR_PROMO_DLR_A AS NUMERIC) as gborec12_promotional_dealer_price,
CAST(A01.EAPA01_DPR_DLR_ADJ_A AS NUMERIC) as gborec12_dealer_adjustment_price,
CAST(A01.EAPA01_DPR_DLR_ADJ_TYPE_C AS STRING) as gborec12_dealer_adjustment_type,
CAST(A01.EAPA01_DPR_DLR_OTR_JSON_X AS STRING) as gborec12_dealer_otr_costs,
CAST(A01.EAPA01_DPR_DLR_TOT_OTR_JSON_X AS STRING) as gborec12_dealer_total_otr_costs,
CAST(A01.EAPA01_DPR_TOT_INC_A AS NUMERIC) as gborec12_total_incentives,
CAST(A01.EAPA01_DPR_DLR_SELL_VAT_A AS NUMERIC) as gborec12_dealer_selling_price_vat_amount,
CAST(NULL AS NUMERIC) as gborec12_total_vehicle_price_with_savings,
CAST(NULL AS NUMERIC) as gborec12_dealer_total_otr_costs_with_reg_tax
) as gborec12_dealerized_price,

--gborec13_driveaway_price

STRUCT(CAST(NULL AS INT64) as gborec13_accessory_price,
CAST(NULL AS INT64)  as gborec13_dealer_delivery_fee,
CAST(NULL AS INT64) as gborec13_incentive,
CAST(NULL AS INT64) as gborec13_mlp,
CAST(NULL AS INT64) as gborec13_option_price,
CAST(NULL AS NUMERIC) as gborec13_stamp_duty,
CAST(NULL AS NUMERIC) as gborec13_vehicle_driveaway_price,
CAST(NULL AS NUMERIC) as gborec13_administration_fee,
CAST(NULL AS NUMERIC) as gborec13_ctp_gst,
CAST(NULL AS INT64) as gborec13_ctpi_gst,
CAST(NULL AS INT64) as gborec13_emergency_services_levy,
CAST(NULL AS INT64) as gborec13_fire_levy,
CAST(NULL AS INT64) as gborec13_general_issue_number_plate_fee,
CAST(NULL AS INT64) as gborec13_motor_tax,
CAST(NULL AS INT64) as gborec13_hybrid_discount,
CAST(NULL AS NUMERIC) as gborec13_lct,
CAST(NULL AS NUMERIC) as gborec13_lifetime_support_fund,
CAST(NULL AS NUMERIC) as gborec13_prescribed_flatfee,
CAST(NULL AS NUMERIC) as gborec13_recording_fee,
CAST(NULL AS NUMERIC) as gborec13_registration_amount,
CAST(NULL AS NUMERIC) as gborec13_registration_fee,
CAST(NULL AS NUMERIC) as gborec13_registration_plates,
CAST(NULL AS STRING) as gborec13_risk_zone,
CAST(NULL AS INT64) as gborec13_road_safety_levy,
CAST(NULL AS NUMERIC) as gborec13_tac_gst,
CAST(NULL AS NUMERIC) as gborec13_total_registration,
CAST(NULL AS NUMERIC) as gborec13_traffic_improvement_fee,
CAST(NULL AS INT64) as gborec13_vehicle_tax,
CAST(NULL AS STRING) as borec13_ctp_district,
CAST(NULL AS NUMERIC) as gborec13_ctpi_amount,
CAST(NULL AS INT64) as gborec13_ctpi_charge,
CAST(NULL AS STRING) as gborec13_ctp_zone,
CAST(NULL AS NUMERIC) as gborec13_tac_amount,
CAST(NULL AS INT64) as gborec13_tac_charge,
CAST(NULL AS INT64) as gborec13_tac_duty,
CAST(NULL AS INT64) as gborec13_ctp_admin_levy,
CAST(NULL AS NUMERIC) as gborec13_ctp_amount,
CAST(NULL AS NUMERIC) as gborec13_ctp_duty,
CAST(NULL AS NUMERIC) as gborec13_ctp_fee,
CAST(NULL AS NUMERIC) as gborec13_maib_amount
) as gborec13_driveaway_price,

CAST(A01.EAPA01_PSM_ORD_DEPOSIT_A AS NUMERIC) as gborec11_order_deposit_price,
CAST(NULL AS NUMERIC) as gborec11_estimated_cash_price,
CAST(A01.EAPA01_PSM_RESV_DEPOSIT_A AS NUMERIC) as gborec11_reservation_deposit_price
) as gborec11_detailed_price_summary,

--gborec14_entries

ARRAY
			(
				SELECT AS STRUCT
					(
							SELECT AS STRUCT
							CAST(NULL AS STRING) AS gborec15_dealer_code,
						  CAST(A011.EAPA01_DLR_BRAND_C AS STRING) as gborec15_dealer_brand_code,
							CAST(A011.EAPA01_DLR_PA_C AS STRING) as gborec15_dealer_pa_code,
							CAST(A011.EAPA01_DLR_SLS_C AS STRING) as gborec15_dealer_sales_code,
							CAST(A011.EAPA01_DLR_N AS STRING) as gborec15_dealer_name,
							STRUCT
							(
									CAST(A011.EAPA01_DLR_ADD_TOWN_X AS STRING) as gborec16_dealer_address_town,
									CAST(A011.EAPA01_DLR_ADD_LN1_X AS STRING) as gborec16_dealer_address_line1,
									CAST(A011.EAPA01_DLR_PHONE_R AS STRING) as gborec16_dealer_phone_number,
									CAST(A011.EAPA01_DLR_STATE_C AS STRING) as gborec16_dealer_state_province_code,
									CAST(A011.EAPA09_STATE_PROV_N AS STRING) as gborec16_dealer_state_province_name,
									CAST(A011.EAPA01_DLR_ZIP_C AS STRING) as gborec16_dealer_postal_code,
									CAST(A011.EAPA01_DLR_EMAIL_X AS STRING) as gborec16_dealer_email_address,

										STRUCT
										(
									CAST(A011.EAPA01_DLR_CNTRY_ISO_C AS STRING) as gborec17_dealer_country_iso,
								  CAST(A011.EAPA01_DLR_CNTRY_NAME_N AS STRING) as gborec17_dealer_country_iso_name
										)
								AS gborec17_dealer_country
					   )
							AS gborec16_dealer_address
					)
				AS gborec15_dealer,
				STRUCT
					(
						CAST(A011.EAPA01_PRD_CTLG_D AS STRING) as gborec18_product_catalog_id,
						CAST(A011.EAPA01_PRD_IMAGE_U AS STRING) as gborec18_product_image_url,
						-- [STRUCT (STRUCT(CAST(A011.EAPA04_WERS_FAM_C AS STRING) as gborec20_family),
						-- [STRUCT (CAST(A011.EAPA04_WERS_FTR_C AS STRING) AS gborec21_code, CAST(A011.EAPA04_WERS_FTR_X AS STRING) AS gborec21_description,
						-- [CAST('1' AS STRING) ]) ]
						-- 	)],
							      ARRAY
													(
													SELECT AS STRUCT
															(
															SELECT AS STRUCT
														CAST(A011.EAPA04_WERS_FAM_C AS STRING) AS gborec20_wers_family_code
													)
												AS gborec20_family,
												ARRAY
													(
															SELECT AS STRUCT
														CAST(A011.EAPA04_WERS_FTR_C AS STRING) AS gborec21_code,
														CAST(A011.EAPA04_WERS_FTR_X AS STRING) AS gborec21_description
													)
												AS gborec21_values
											)
										AS gborec19_features,
									STRUCT
									(
										CAST(A011.EAPA01_MKI_BRAND_N AS STRING) AS gborec22_brand,
										CAST(A011.EAPA01_MKI_MODEL_N AS STRING) AS gborec22_name_plate,
										CAST(A011.EAPA01_MKI_SEGMENT_N AS STRING) AS gborec22_segment,
										CAST(A011.EAPA01_MKI_MODEL_YEAR_R AS STRING) AS gborec22_model_year,
										CAST(A011.EAPA01_MKI_VEH_SALE_MKT_C AS STRING) AS gborec22_sales_market
									)
						AS gborec22_marketing_identity,
							CAST(NULL AS STRING) AS gborec18_gux_category_codes
					)
				AS gborec18_product
	 FROM A04_TEMP A011
  WHERE A011.EAPA01_ORD_D = A01.EAPA01_ORD_D
	AND A011.EAPA01_EVNT_TYPE_C = A01.EAPA01_EVNT_TYPE_C
	AND A011.EAPA01_ORD_LAST_MOD_S = A01.EAPA01_ORD_LAST_MOD_S
	AND A011.row_num = 1
			)
			AS gborec14_entries,

--gborec18_gux_category_codes

Safe_cast(A01.EAPA01_ORD_S AS TIMESTAMP) as gborec04_order_date_utc_timestamp,
PARSE_DATE('%d/%m/%y', A01.EAPA01_ORD_EST_Y) AS gborec04_order_date_est,
CAST(A01.EAPA01_ITEM_C AS STRING) as gborec04_item_number,

--gborec23_finance_app_data

STRUCT(CAST(A01.EAPA01_FAP_APPL_D AS STRING) as gborec23_application_id,
CAST(A01.EAPA01_FAP_PROD_TYPE_C AS STRING) as gborec23_finance_product_type,
CAST(A01.EAPA01_FAP_FIN_PROVIDER_X AS STRING) as gborec23_finance_provider,
CAST(A01.EAPA01_FAP_QUOTE_X AS STRING) as gborec23_quote,
CAST(A01.EAPA01_FAP_STAT_C AS STRING) as gborec23_status,
Safe_cast(A01.EAPA01_FAP_STAT_UPDT_S AS TIMESTAMP) as gborec23_status_updated_time_utc_timestamp,
PARSE_DATE('%d/%m/%y',A01.EAPA01_FAP_STAT_UPDT_EST_Y) as gborec23_status_updated_time_EST
) as gborec23_finance_app_data,

CAST(A01.EAPA01_FIN_REMINDER_X AS STRING) as gborec04_finance_reminder,
CAST(A01.EAPA01_FORD_AFFLL_STAT_C AS STRING) as gborec04_ford_affiliation_status,
CAST(A01.EAPA01_FORD_IND_PAYT_OP_C AS STRING) as gborec04_ford_indicative_payment_option,
CAST(A01.EAPA01_VIN_ADDED_F AS STRING) as gborec04_vin_added,
STRUCT(CAST(A01.EAPA01_VD_VIN_C AS STRING) as gborec24_vin,
CAST(NULL AS STRING) as gborec24_state) as gborec24_vehicle_data,
CAST(A01.EAPA01_PTP_JSON_X AS STRING) as gborec04_protection_plan,


--gborec04_data.gborec25_refund_info.gborec26_downpayment_deposit
STRUCT(
	STRUCT(CAST(A01.EAPA01_RFA_DOWNPYMT_RFND_A AS NUMERIC) as gborec26_refunded_amount,
Safe_cast(A01.EAPA01_RFA_DOWNPYMT_RFND_S AS TIMESTAMP) as gborec26_refunded_date_utc_timestamp,
PARSE_DATE('%d/%m/%y', A01.EAPA01_RFA_DOWNPYMT_RFND_EST_Y) AS gborec26_refunded_date_est,
CAST(A01.EAPA01_RFA_DOWNPYMT_RFND_C AS STRING) as gborec26_refunded_status,
STRUCT(CAST(A01.EAPA01_RFA_DOWNPYMTTYPE_X AS STRING) as gborec27_payment_type) as gborec27_payment_info
) as gborec26_downpayment_deposit,

--gborec28_reservation_deposit
STRUCT(CAST(A01.EAPA01_RFA_RESV_RFND_A AS NUMERIC) as gborec28_refunded_amount,
Safe_cast(A01.EAPA01_RFA_RESV_RFND_S AS TIMESTAMP) as gborec28_refunded_date_utc_timestamp,
PARSE_DATE('%d/%m/%y',A01.EAPA01_RFA_RESV_RFND_EST_Y) AS gborec28_refunded_date_est,
CAST(A01.EAPA01_RFA_RESV_RFND_C AS STRING) as gborec28_refunded_status,

STRUCT(CAST(A01.EAPA01_RFA_RESVTYPE_X AS STRING) as gborec29_payment_type) as gborec29_payment_info
) as gborec28_reservation_deposit,

--gborec30_dealerdeposit
STRUCT(CAST(A01.EAPA01_RFA_DLRDEPST_RFND_A AS numeric) as gborec30_refunded_amount,
Safe_cast(A01.EAPA01_RFA_DLRDEPST_RFND_S AS TIMESTAMP) as gborec30_refunded_date_utc_timestamp,
PARSE_DATE('%d/%m/%y',A01.EAPA01_RFA_DLRDEPST_RFND_EST_Y) AS gborec30_refunded_date_est,
CAST(A01.EAPA01_RFA_DLRDEPST_RFND_C AS STRING) as gborec30_refunded_status,

STRUCT(CAST(A01.EAPA01_RFA_DLRDEPSTTYPE_X AS STRING) as gborec31_payment_type) as gborec31_payment_info
) as gborec30_dealerdeposit,

STRUCT(CAST(NULL AS STRING) as gborec32_payment_type) as gborec32_payment_info
) as gborec25_refund_info,


--gborec33_accessory_data
STRUCT(CAST(A01.EAPA01_ACS_CURR_CONFIG_X AS STRING) as gborec33_current_config,
CAST(A01.EAPA01_ACS_TOT_A AS STRING) as gborec33_total_price,
CAST(A01.EAPA01_ACS_TOT_CASH_A AS STRING) as gborec33_total_cash_price,
CAST(A01.EAPA01_ACS_TOT_FIN_A AS STRING) as gborec33_total_finance_price,
CAST(NULL AS STRING) as gborec33_catalog_id,

--gborec34_accessories
array(select as struct CAST(A02.EAPA02_ACS_ACCSS_C AS STRING) as gborec34_code,
CAST(A02.EAPA02_ACS_SHORT_DESC_X AS STRING) as gborec34_short_description,
CAST(NULL AS STRING) as gborec34_medium_description,
CAST(A02.EAPA02_ACS_CHLD_JSON_X AS STRING) as gborec34_accessory_details,
CAST(A02.EAPA02_ACS_LONG_DESC_X AS STRING) as gborec34_long_description,
CAST(NULL AS NUMERIC) as gborec34_msrp,

CAST(A02.EAPA02_ACS_DLR_SELL_A AS NUMERIC) as gborec34_dealer_selling_price,
CAST(A02.EAPA02_ACS_DSCLM_X AS STRING) as gborec34_disclaimer,
CAST(A02.EAPA02_ACS_SEQ_R AS INT64) as gborec34_sequence,

CAST(A02.EAPA02_ACS_WARR_INFO_X AS STRING) as gborec34_warranty_information,
CAST(A02.EAPA02_ACS_MAPPED_PART_C AS STRING) as gborec34_mapped_part_id,
CAST(A02.EAPA02_ACS_PROD_TYP_C AS STRING) as gborec34_product_type,
CAST(NULL AS BOOL) as gborec34_is_top_seller,
CAST(NULL AS BOOL) as gborec34_is_most_visited,

CAST(A02.EAPA02_ACS_STATE_C AS STRING) as gborec34_state,
STRUCT(CAST(A02.EAPA02_ACS_FIN_TYPE_X AS STRING) as gborec35_financetype) as gborec35_finance_data,
CAST(A02.EAPA02_ACS_IMG_ASSET_X AS STRING) as gborec34_image_assets,
CAST(NULL AS STRING) as gborec34_eprel_id,
CAST(NULL AS BOOL) as gborec34_apply_finance
from `prj--53184-eap-dev.eap_dev.MEAPA02_ACCSS_HIST_TBL` A02 WHERE A01.EAPA01_ORD_D = A02.EAPA01_ORD_D
	AND A01.EAPA01_EVNT_TYPE_C = A02.EAPA01_EVNT_TYPE_C
	AND A01.EAPA01_ORD_LAST_MOD_S = A02.EAPA01_ORD_LAST_MOD_S
) as gborec34_accessories,

CAST(A01.EAPA01_ACS_DLR_ADDON_JSON_X AS STRING) as gborec33_dealer_addons) as gborec33_accessory_data,


-- gborec36_charging_data
STRUCT(CAST(A01.EAPA01_CHG_CURR_CONFIG_X AS STRING) as gborec36_current_config,
CAST(A01.EAPA01_CHG_TOT_A AS STRING) as gborec36_total_price,
CAST(A01.EAPA01_CHG_TOT_CASH_A AS STRING) as gborec36_total_cash_price,
CAST(A01.EAPA01_CHG_TOT_FIN_A AS STRING) as gborec36_total_finance_price,
CAST(A01.EAPA01_CHG_CTLG_D AS STRING) as gborec36_catalog_id,

--gborec04_data.gborec36_charging_data.gborec37_accessories
array(
	select as struct
	  CAST(A03.EAPA03_CHG_ACCSS_C AS STRING) as gborec37_code,
		CAST(A03.EAPA03_CHG_SHORT_DESC_X AS STRING) as gborec37_short_description,
		CAST(NULL AS STRING) as gborec37_medium_description,
		CAST(A03.EAPA03_CHG_CHLD_JSON_X AS STRING) as gborec37_accessory_details,
		CAST(A03.EAPA03_CHG_LONG_DESC_X AS STRING) as gborec37_long_description,
		CAST(A03.EAPA03_CHG_MSRP_A AS NUMERIC) as gborec37_msrp,
		CAST(A03.EAPA03_CHG_IMG_ASSET_X AS STRING) as gborec37_image_assets,
		CAST(A03.EAPA03_CHG_DLR_SELL_A AS NUMERIC) as gborec37_dealer_selling_price,
		CAST(A03.EAPA03_CHG_DSCLM_X AS STRING) as gborec37_disclaimer,
		CAST(A03.EAPA03_CHG_SEQ_R AS INT64) as gborec37_sequence,
		CAST(A03.EAPA03_CHG_WARR_INFO_X AS STRING) as gborec37_warranty_information,
		CAST(A03.EAPA03_CHG_MAPPED_PART_C AS STRING) as gborec37_mapped_part_id,
		CAST(A03.EAPA03_CHG_PROD_TYP_C AS STRING) as gborec37_product_type,
		CAST(NULL AS BOOL) as gborec37_is_top_seller,
		CAST(NULL AS BOOL) as gborec37_is_most_visited,
		CAST(A03.EAPA03_CHG_STATE_C AS STRING) as gborec37_state,
		STRUCT(CAST(A03.EAPA03_CHG_FIN_TYPE_X AS STRING) as gborec38_financetype) as gborec38_finance_data,
		CAST(NULL AS STRING) as gborec37_eprel_id,
		CAST(A03.EAPA03_CHG_APPLY_FIN_F AS BOOL) as gborec37_apply_finance
		from `prj--53184-eap-dev.eap_dev.MEAPA03_CHARG_ACCSS_HIST_TBL` A03
		WHERE A01.EAPA01_ORD_D = A03.EAPA01_ORD_D
			AND A01.EAPA01_EVNT_TYPE_C = A03.EAPA01_EVNT_TYPE_C
			AND A01.EAPA01_ORD_LAST_MOD_S = A03.EAPA01_ORD_LAST_MOD_S
) as gborec37_accessories,


--gborec04_data.gborec36_charging_data.gborec39_quote_flags
STRUCT(CAST(NULL AS STRING) as gborec39_protectionplan_included_warranty) as gborec39_quote_flags,

CAST(A01.EAPA01_CHG_DLR_ADDON_X AS STRING) as gborec36_dealer_addons,
Safe_cast(A01.EAPA01_CHG_INST_REQ_Y AS TIMESTAMP) as gborec36_installation_requested_date_utc_timestamp,
CAST(NULL AS DATE) as gborec36_installation_requested_date_est,
CAST(A01.EAPA01_CHG_INST_REQ_STS_X AS STRING) as gborec36_installation_requested_status
) as gborec36_charging_data,

-- gborec04_data.gborec40_delivery_method

STRUCT(CAST(A01.EAPA01_DMD_DLVY_METHOD_TYPE_C AS STRING) as gborec40_type,
CAST(A01.EAPA01_DMD_DLVY_FEE_A AS NUMERIC) as gborec40_amount,
CAST(A01.EAPA01_DMD_APPLY_FIN_F AS BOOL) as gborec40_apply_finance) as gborec40_delivery_method,

--gborec41_refund_information

array(select as struct CAST(A28.EAPA28_DEPST_TYP_X AS STRING) as gborec41_deposit_type,
CAST(A28.EAPA28_RFND_AMT_A AS NUMERIC) as gborec41_refunded_amount,
Safe_cast(A28.EAPA28_RFND_DT_S AS TIMESTAMP) as gborec41_refunded_date,
CAST(A28.EAPA28_RFND_STATUS_C AS STRING) as gborec41_refunded_status,
STRUCT(CAST(A28.EAPA28_RFND_PYMTTYPE_X AS STRING) as gborec42_payment_type) as gborec42_payment_info
from `prj--53184-eap-dev.eap_dev.MEAPA28_REFUND_INFO_HIST_TBL` A28 WHERE A01.EAPA01_ORD_D = A28.EAPA01_ORD_D
) as gborec41_refund_information,


CAST(A01.EAPA01_CARBON_EMISSION_R AS NUMERIC) as gborec04_carbon_emission,
CAST(A01.EAPA01_ENERGY_CONSUM_KM_R AS NUMERIC) as gborec04_energy_consumption_km,
CAST(A01.EAPA01_ENERGY_CONSUM_MILE_R AS NUMERIC) as gborec04_energy_consumption_miles,
PARSE_DATE('%d/%m/%y',A01.EAPA01_BUILD_WEEK_Y) AS gborec04_build_week,
STRUCT(PARSE_DATE('%d/%m/%y',A01.EAPA01_ETA_START_Y) AS gborec43_eta_start_date,
PARSE_DATE('%d/%m/%y',A01.EAPA01_ETA_END_Y) AS gborec43_eta_end_date) as gborec43_eta,

STRUCT(A01.EAPA01_RGSTR_D as gborec44_registration_number,
A01.EAPA01_RGSTR_S as gborec44_registration_date_utc_timestamp,
PARSE_DATE('%d/%m/%y',A01.EAPA01_RGSTR_EST_Y) AS gborec44_registration_date_est) as gborec44_registration_details,

--gborec04_data.gborec45_tradein
array(select as struct CAST(A24.EAPA24_TRD_APPRAISAL_D AS STRING) as gborec45_appraisal_id,
CAST(A24.EAPA24_TRD_BAL_OWED_A AS NUMERIC) as gborec45_balance_owed,
CAST(A24.EAPA24_TRD_VEH_MAKE_N AS STRING) as gborec45_make,
CAST(A24.EAPA24_TRD_VEH_MODEL_X AS STRING) as gborec45_model,
CAST(A24.EAPA24_TRD_VEH_ENGINE_TYPE_X AS STRING) as gborec45_engine_type,
CAST(A24.EAPA24_TRD_VEH_TRANSMISSION_X AS STRING) as gborec45_transmission,
CAST(A24.EAPA24_TRD_APPLY_FIN_F AS BOOL) as gborec45_apply_finance_for_tradein,
Safe_cast(A24.EAPA24_TRD_EXPIR_S AS TIMESTAMP) as gborec45_expiry_date,
CAST(A24.EAPA24_TRD_EXPIR_EST_Y AS DATE) as gborec45_expiry_date_est,
CAST(A24.EAPA24_TRD_APPRAISAL_A AS NUMERIC) as gborec45_appraisal_amount,
CAST(A24.EAPA24_TRD_NET_VALUE_A AS NUMERIC) as gborec45_net_value,
CAST(A24.EAPA24_TRD_VEH_MODEL_YEAR_R AS INT64) as gborec45_year,
CAST(A24.EAPA24_TRD_EST_LEASE_TERMNT_A AS NUMERIC) as gborec45_est_lease_termination_amount,
CAST(A24.EAPA24_TRD_IMAGE_U AS STRING) as gborec45_image_url,
CAST(A24.EAPA24_TRD_IS_ESTD_TRADEIN_F AS STRING) as gborec45_is_estimated_tradein,
STRUCT(CAST(NULL AS STRING) as gborec46_ownership_type,
CAST(NULL AS STRING) as gborec46_condition_rating) as gborec46_attributes_map
from `prj--53184-eap-dev.eap_dev.MEAPA24_TRADEIN_HIST_TBL` A24 WHERE A01.EAPA01_ORD_D = A24.EAPA01_ORD_D
	 AND A01.EAPA01_EVNT_TYPE_C = A24.EAPA01_EVNT_TYPE_C
	 AND A01.EAPA01_ORD_LAST_MOD_S = A24.EAPA01_ORD_LAST_MOD_S
) as gborec45_tradein,

CAST(A01.EAPA01_DLVY_MODE_C AS STRING) as gborec04_delivery_mode,
CAST(A01.EAPA01_FIN_PROCESS_ACTN_C AS STRING) as gborec04_finance_process_action,
Safe_cast(A01.EAPA01_CANC_S AS TIMESTAMP) as gborec04_cancelled_date_utc_timestamp,
PARSE_DATE('%d/%m/%y', A01.EAPA01_CANC_EST_Y) AS gborec04_cancelled_date_est,
CAST(A01.EAPA01_CANC_REASON_X AS STRING) as gborec04_cancellation_reason,
Safe_cast(A01.EAPA01_DLR_ACTN_S AS TIMESTAMP) as gborec04_dealer_action_date_utc_timestamp,
PARSE_DATE('%d/%m/%y', A01.EAPA01_DLR_ACTN_EST_Y) AS gborec04_dealer_action_date_est,
CAST(A01.EAPA01_PRE_APPVD_F AS STRING) as gborec04_pre_approved,
CAST(A01.EAPA01_LITE_CONFIG_F AS BOOL) as gborec04_lite_config,
Safe_cast(A01.EAPA01_PROD_SCHED_S AS TIMESTAMP)  as gborec04_scheduled_date_utc_timestamp,
CAST(null AS DATE)  as gborec04_scheduled_date_est,
CAST(A01.EAPA01_CUST_CONSENT_STAT_C AS STRING) as gborec04_customer_consent_status,
Safe_cast(A01.EAPA01_CUST_CONSENT_STAT_S AS TIMESTAMP) as gborec04_customer_consent_status_updated_date,
PARSE_DATE('%d/%m/%y', A01.EAPA01_PROD_SCHED_EST_Y) AS gborec04_customer_consent_status_updated_date_est,
CAST(A01.EAPA01_REC_STAT_C AS STRING) as gborec04_rec_status_code,
CAST(A01.EAPA01_GLOBAL_ORDER_D AS STRING) as gborec04_global_order_id,
CAST(A01.EAPA01_SIGN_AT_STORE_F AS BOOL) as gborec04_sign_atstore,
CAST(A01.EAPA01_IND_JOURNEY_TYPE_F AS BOOL) as gborec04_is_showroom_journey,
CAST(A01.EAPA01_ISWAITLISTED_F AS BOOL) as gborec04_is_waitlisted,
Safe_cast(A01.EAPA01_CART_CRT_S AS TIMESTAMP) as gborec04_cart_created_utc_timestamp,
CAST(A01.EAPA01_CART_CRT_EST_Y AS DATE) as gborec04_cart_created_est,
Safe_cast(A01.EAPA01_WAITLIST_S AS TIMESTAMP) as gborec04_wait_list_utc_timestamp,
CAST(A01.EAPA01_WAITLIST_EST_Y AS DATE) as gborec04_wait_list_est,
A01.EAPA01_WAITLISTTYPE_X as gborec04_wave_type,
A01.EAPA01_PRIOR_CTLG_D as gborec04_prior_catalog_id,
CAST(A01.EAPA01_PRIOR_MODEL_YEAR_R AS INT64) as gborec04_prior_model_year,
Safe_cast(NULL AS TIMESTAMP) as gborec04_estimated_date_by_leadtime,
CAST(A01.EAPA01_CART_IND_F AS BOOL) as gborec04_cart_indicator,
A01.EAPA01_QUOTE_ID_D as gborec04_quote_id,
CAST(NULL AS STRING) as gborec04_unbuildable_flag,
A01.EAPA01_WALLBOX_CONSENT_STAT_C as gborec04_wallbox_consent,

--gborec47_marketing_properties------------
array
 (
	 select as struct
	  CAST(NULL AS STRING) as gborec47_code,
		CAST(NULL AS STRING) as gborec47_marketing_group_code,
		CAST(NULL AS STRING) as gborec47_short_description,
		CAST(NULL AS STRING) as gborec47_type
 )
 as gborec47_marketing_properties
)
as gborec04_data,


CAST(A01.EAPA01_SCA_CONSUMER_R AS numeric) as gborec01_sca_consumer_number,
A01.EAPA01_SCA_CNTRY_ISO_C as gborec01_sca_country_iso,
CAST(NULL AS STRING) as gborec01_continue_at_dlr_flag,
CAST(NULL AS STRING) as gborec01_rawpayload_json,
A01.EAPA01_CRT_USR_D as gborec01_create_user_id,
A01.EAPA01_CRT_S as gborec01_create_timestamp,
A01.EAPA01_LST_UPDT_USR_D as gborec01_last_update_user_id,
A01.EAPA01_LST_UPDT_S as gborec01_last_update_timestamp,
A01.EAPA01_SRC_LOADTIME_S as gborec01_df_bq_write_time,
Safe_cast(NULL AS TIMESTAMP) as gborec01_eff_in_ts

FROM A01
LEFT JOIN `prj--53184-eap-dev.eap_dev.MEAPA07_REGION_TBL` A07
	ON A01.EAPA01_REGN_C = A07.EAPA07_REGN_C
LEFT JOIN `prj--53184-eap-dev.eap_dev.MEAPA08_COUNTRY_TBL` A08
	ON A01.EAPA01_REGN_C = A08.EAPA07_REGN_C
	AND A01.EAPA01_CNTRY_C = A08.EAPA08_CNTRY_C
--	LIMIT 10;
 WHERE A01.EAPA01_ORD_D ='test-03280013';
 --	A01.EAPA01_ORD_D = 'test-03280013';
