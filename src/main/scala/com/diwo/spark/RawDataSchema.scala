package com.diwo.spark

/**
  * This Actor reads the data from csv file with
  * option having headers/not headers and persists the data
  * to cassendra and sends dataframe to Cassendra actor
  */
case  class RawDataSchema(
                           brnd_cd:String,sto_loc_id:String,shpg_trxn_id:String,shpg_trxn_ln_dt:String,trxn_bgn_tm:String,src_rgst_nbr:String,
                           itm_sku_num:String,src_sls_prsn_num:String,src_sls_cshr_num:String,	fp_sls_amt:String,fp_sls_qty:String,clr_sls_amt:String,
                           clr_sls_qty:String,md_sls_amt:String,md_sls_qty:String,gvwy_sls_amt:String,gvwy_sls_qty:String,net_sales:String,
                           net_sls_qty:String,return_sales:String,return_qty:String,angel_trxn:String,	wt_unt_cst_amt:String,cur_tkt_rtl_unt_amt:String
 )
