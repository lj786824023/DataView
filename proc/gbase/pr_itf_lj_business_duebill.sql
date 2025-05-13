DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_itf_lj_business_duebill"(
	IN IN_TX_DATE	VARCHAR(8),
   OUT OUT_FLAG		VARCHAR(10)
)
LABLE:BEGIN
 
/*=================================================================+
  应用名称 LSYW
  模块名称 产品主题
  功能描述 绿金业务借据表
  程序名称 PROC_D_CRDT_CARD_TX_DAY_SUM_D
  参    数 IN  IN_TX_DATE VARCHAR(8)  数据日期
           OUT OUT_FLAG   VARCHAR(10) 输出参数
  返 回 值 0 成功 非 0 失败
  算    法 删除/插入
  创建人员 LIANG XUEBIN
  创建日期 2021/10/08
  修改人员 
  --  修改日期 
  修改原因 
+=================================================================*/

    declare v_data_last_month       date;
	DECLARE ETL_STEP_NO		INTEGER			DEFAULT 1;
	DECLARE ETL_SQL_UNIT	VARCHAR(60)		DEFAULT 'pr_itf_lj_business_duebill';
	DECLARE TX_DATE             VARCHAR(8);
	DECLARE LAST_TX_DATE        DATE;
	DECLARE THIS_QUART_BEGIN    DATE;
	DECLARE THIS_MONTH_BEGIN    DATE;
	DECLARE THIS_MONTH_END      DATE;
	DECLARE LAST_MONTH_BEGIN    DATE;
	DECLARE LAST_MONTH_END      DATE;
	DECLARE THIS_YEAR_BEGIN     DATE;
	DECLARE LAST_YEAR_END       DATE;
	DECLARE NULL_DATE           DATE;
	DECLARE ILL_DATE            DATE;
	DECLARE MAX_DATE            DATE;
	DECLARE INIT_DATE           DATE;
	DECLARE TX_DATE_8           VARCHAR(8);
	DECLARE LAST_TX_DATE_8      VARCHAR(8);
	DECLARE THIS_QUART_BEGIN_8  VARCHAR(8);
	DECLARE THIS_MONTH_BEGIN_8  VARCHAR(8);
	DECLARE THIS_MONTH_END_8    VARCHAR(8);
	DECLARE LAST_MONTH_BEGIN_8  VARCHAR(8);
	DECLARE LAST_MONTH_END_8    VARCHAR(8);
	DECLARE THIS_YEAR_BEGIN_8   VARCHAR(8);
	DECLARE LAST_YEAR_END_8     VARCHAR(8);
	DECLARE NULL_DATE_8         VARCHAR(8);
	DECLARE ILL_DATE_8          VARCHAR(8);
	DECLARE MAX_DATE_8          VARCHAR(8);
	DECLARE INIT_DATE_8         VARCHAR(8);
	
    -- SET TX_DATE = TO_DATE(IN_TX_DATE,'YYYYMMDD'); --  批次日期 日期类型
    SET TX_DATE =   IN_TX_DATE ;
    SET LAST_TX_DATE = TO_DATE(IN_TX_DATE,'YYYYMMDD')-1;  --  昨日批次日期	日期类型
    SET THIS_QUART_BEGIN = DATE(CONCAT(YEAR(IN_TX_DATE),'-',ELT(QUARTER(IN_TX_DATE),1,4,7,10),'-',1)); --  当季初 日期类型
    SET THIS_MONTH_BEGIN = LAST_DAY(ADD_MONTHS(TO_DATE(IN_TX_DATE,'YYYYMMDD'),-1)) + 1; --  当月初 日期类型
    SET THIS_MONTH_END = LAST_DAY(TO_DATE(IN_TX_DATE,'YYYYMMDD')); --  当月末 日期类型
    SET LAST_MONTH_BEGIN = LAST_DAY(ADD_MONTHS(TO_DATE(IN_TX_DATE,'YYYYMMDD'),-2)) + 1; --  上月初  日期类型
    SET LAST_MONTH_END = LAST_DAY(ADD_MONTHS(TO_DATE(IN_TX_DATE,'YYYYMMDD'),-1)); --  上月末	日期类型
    SET THIS_YEAR_BEGIN = TRUNC(TO_DATE(IN_TX_DATE,'YYYYMMDD'),'YYYY'); --  当年初 日期类型
    SET LAST_YEAR_END = TRUNC(TO_DATE(IN_TX_DATE,'YYYYMMDD'),'YYYY') - 1; --  上年末 日期类型
    SET NULL_DATE = TO_DATE('0001-01-01','YYYY-MM-DD'); --  空日期 日期类型 0001-01-01
    SET ILL_DATE = TO_DATE('0001-01-02','YYYY-MM-DD'); --  错误日期	日期类型 0001-01-02
    SET MAX_DATE = TO_DATE('9999-12-31','YYYY-MM-DD'); --  拉链最大日期	日期类型 9999-12-31
    SET INIT_DATE = TO_DATE('1900-01-01','YYYY-MM-DD'); --  初始日期 日期类型 1900-01-01
    SET TX_DATE_8 = IN_TX_DATE; --  批次日期 8位字符串类型
    SET LAST_TX_DATE_8 = TO_CHAR(TO_DATE(IN_TX_DATE,'YYYYMMDD')-1,'YYYYMMDD'); --  昨日批次日期 8位字符串类型
    SET THIS_QUART_BEGIN_8 = TO_CHAR(DATE(CONCAT(YEAR(IN_TX_DATE),'-',ELT(QUARTER(IN_TX_DATE),1,4,7,10),'-',1)),'YYYYMMDD'); --  当季初	8位字符串类型
    SET THIS_MONTH_BEGIN_8 = TO_CHAR(LAST_DAY(ADD_MONTHS(TO_DATE(IN_TX_DATE,'YYYYMMDD'),-1)) + 1,'YYYYMMDD');  --  当月初	8位字符串类型
    SET THIS_MONTH_END_8 = TO_CHAR(LAST_DAY(TO_DATE(IN_TX_DATE,'YYYYMMDD')),'YYYYMMDD'); --  当月末  8位字符串类型
    SET LAST_MONTH_BEGIN_8 = TO_CHAR(LAST_DAY(ADD_MONTHS(TO_DATE(IN_TX_DATE,'YYYYMMDD'),-2)) + 1,'YYYYMMDD'); --  上月初  8位字符串类型
    SET LAST_MONTH_END_8 = TO_CHAR(LAST_DAY(ADD_MONTHS(TO_DATE(IN_TX_DATE,'YYYYMMDD'),-1)),'YYYYMMDD'); --  上月末 8位字符串类型
    SET THIS_YEAR_BEGIN_8 = TO_CHAR(TRUNC(TO_DATE(IN_TX_DATE,'YYYYMMDD'),'YYYY'),'YYYYMMDD');  --  当年初 8位字符串类型
    SET LAST_YEAR_END_8 = TO_CHAR(TRUNC(TO_DATE(IN_TX_DATE,'YYYYMMDD'),'YYYY') - 1,'YYYYMMDD');  --  上年末 8位字符串类型
    SET NULL_DATE_8 = '00010101'; --  空日期 8位字符串类型00010101
    SET ILL_DATE_8 = '00010102'; --  错误日期 8位字符串类型00010102
    SET MAX_DATE_8 = '99991231'; --  拉链最大日期 8位字符串类型99991231
    SET INIT_DATE_8 = '19000101'; --  初始日期 8位字符串类型19000101
    set v_data_last_month=to_date(IN_TX_DATE,'yyyymmdd') - 30;     
    SET OUT_FLAG =  @RTC;
	     
    SET @SQL_STR = 'DELETE FROM ITF.nrt_t_lj_business_duebill;';
CALL etl.PR_EXEC_SQL(@RTC,'',ETL_SQL_UNIT,ETL_STEP_NO,@SQL_STR,IN_TX_DATE);
SET OUT_FLAG = @RTC;
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;    

SET @SQL_STR = '
    insert into  itf.nrt_t_lj_business_duebill (
    datano,
    agmt_id, --  借据流水号
    draw_no, --  信贷提用编号
    contr_agmt_id, --  合同流水号
    be_abs_ind,--  借据出表状态
    line_Cate_Cd,--  条线标识
    cust_id,--  客户编号
    prod_id, --  业务品种
    in_bal_off_bal_ind,--  表内外标识
    distr_dt,--  借据起始日
    agmt_dt, --   到期日 
    finali_dt, --   终结日 
    Repay_Freq_Cd, --   还款频率 
    base_int_rate_cate_cd, --   基准利率类型 
    base_int_rate, --   基准年利率(%) 
    int_rate_flt_mod_cd, --   浮动类型 
    int_rate_flt,--   利率浮动值(%) 
    distr_int_rate,--   借据执行年利率(%) 
    int_rate_adj_mod_cd, --   借据利率调整方式
    loan_usg,--   贷款用途 
    cur_cd,--   币种   
    exchg_rate,--   汇率
    dubil_amt, --   借据金额（原币） 
    bal, --   借据余额（原币）
    fiv_cls_cd,--   当月五级分类 
    agmt_grade_rest_cd,--   当月十二级分类 
    ovrd_days, --   逾期天数 
    core_org_num,--   借据核心机构号 
    oprr_id, --   经办人 
    opr_org_id,--   经办机构 
    term_mth ,
    Dubil_Stat_Cd
    )
    select 
    
    '''||TX_DATE||''' ,
    substr(t1.agmt_id,4), --  借据流水号
    substr(t1.Crdt_Use_ID,4), --  信贷提用编号
    substr(t1.contr_agmt_id,4), --  合同流水号
    t3.be_abs_ind,--  借据出表状态
    t4.line_Cate_Cd AS line_Cate_Cd, --  条线标识
    t1.cust_id,--  客户编号
    substr(t1.prod_id,4), --  业务品种
    t8.inout_bal_ind,--  表内外标识
    t1.distr_dt,--  借据起始日
    t5.agmt_dt, --   到期日 
    case when  t1.finali_dt in  (DATE''0001-01-01'',DATE''0001-01-02'') then null  else  t1.finali_dt end as  finali_dt, --   终结日 
    t1.Repay_Freq_Cd, --   还款频率 
           case
         when T1.base_int_rate_cate_cd = ''100'' then
          ''101''
         when T1.base_int_rate_cate_cd = ''060'' then
          ''103''
         when T1.base_int_rate_cate_cd = ''010'' then
          ''202''
         when T1.base_int_rate_cate_cd = ''030'' then
          ''203''
         when T1.base_int_rate_cate_cd = ''020'' then
          ''204''
         when T1.base_int_rate_cate_cd = ''090'' then
          ''205''
         when T1.base_int_rate_cate_cd = ''080'' then
          ''301''
         when T1.base_int_rate_cate_cd = ''040'' then
          ''302''
         when T1.base_int_rate_cate_cd = ''050'' then
          ''303''
         when T1.base_int_rate_cate_cd = ''110'' then
          ''304''
         when T1.base_int_rate_cate_cd = ''070'' then
          ''305''
         ELSE
          t1.base_int_rate_cate_cd
       end AS Base_Int_Rate_Cate_Cd, --  基准利率类型 
       t1.base_int_rate, --   基准年利率(%) 
        t1.Int_Rate_Flt_Mod_Cd, --  利率浮动方式 
    t1.int_rate_flt,--   利率浮动值(%) 
    t1.distr_int_rate,--   借据执行年利率(%) 
    t1.int_rate_adj_mod_cd, --   借据利率调整方式 
    t4.Loan_Usg_Cd,--   贷款用途 
    t1.cur_cd,--   币种   
    nvl(t2.Mdl_Prc,1),--   汇率
    t1.dubil_amt, --   借据金额（原币） 
    t7.bal, --   借据余额（原币）
    t1.fiv_cls_cd  AS Fiv_Cls_Cd, --   当月五级分类 
    t1.Ten_Sec_Cls_Cd AS Agmt_Grade_Rest_Cd, --   当月十二级分类
    t1.ovrd_days, --   逾期天数 
    t3.core_org_num,--   借据核心机构号 
    t3.oprr_id, --   经办人 
    t3.opr_org_id,--   经办机构 
    t1.term_mth,
    t1.Dubil_Stat_Cd  

    from pdm.t03_loan_dubil_h t1
     left join    pdm.t02_exchg_rate_quot_form        t2 ON   t2.Efft_Dt = t1.distr_dt   
     and t2.Trg_Cur=''CNY''  and Exchg_Rate_Cate_Cd=''CTR'' and t2.Statt_Dt='''||TX_DATE||'''  and  T2.efft_tm=''000000''
    left join pdm.t03_loan_dubil_oth_info_h t3 on t1.agmt_id=t3.agmt_id  and '''||TX_DATE||''' between T3.Start_Dt and T3.End_Dt 
    left join pdm.t03_loan_contr_h t4 on  t4.agmt_id=t1.contr_agmt_id    and '''||TX_DATE||''' between T4.Start_Dt and T4.End_Dt
    left join pdm.t03_agmt_dt_ncm_h t5 on t1.agmt_id=t5.agmt_id and '''||TX_DATE||''' between T5.Start_Dt and T5.End_Dt
    and t5.Agmt_Cate_Cd=''0101''  and t5.Agmt_Dt_Typ_Cd=''0004''
    left join pdm.t03_agmt_grade_ncm_h t6 on t1.agmt_id=t6.agmt_id  and '''||TX_DATE||''' between T6.Start_Dt and T6.End_Dt
             and T6.Agmt_Cate_Cd=''0101''   and T6.Agmt_Grade_Typ_Cd=''02''
    left join pdm.t03_agmt_bal_ncm_h t7 on  t1.agmt_id=t7.agmt_id and '''||TX_DATE||''' between T7.Start_Dt and T7.End_Dt
    and T7.Agmt_Bal_Typ_Cd=''02''  
    left join  pdm.t02_loan_prod_h t8
      on  t1.prod_id=t8.prod_id 
      and t8.data_src_cd=''NCM''
      and '''||TX_DATE||''' between T8.Start_Dt and T8.End_Dt
    where t7.bal > 0
      and t1.data_src_cd=''NCM''
      and T1.Agmt_Cate_Cd=''0101''
      and '''||TX_DATE||''' between T1.Start_Dt and T1.End_Dt  ;';
  CALL etl.PR_EXEC_SQL(@RTC,'',ETL_SQL_UNIT,ETL_STEP_NO,@SQL_STR,IN_TX_DATE);
SET OUT_FLAG = @RTC;
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;    
      
    SET @SQL_STR = 'DELETE FROM ITF.nrt_t_lj_business_duebill_NEW;';
CALL etl.PR_EXEC_SQL(@RTC,'',ETL_SQL_UNIT,ETL_STEP_NO,@SQL_STR,IN_TX_DATE);
SET OUT_FLAG =  @RTC;
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;     



SET @SQL_STR = '
    insert into  itf.nrt_t_lj_business_duebill_NEW (
     datano
	,agmt_id --  借据流水号
	,Contr_Agmt_Id --  合同流水号
	,Be_Abs_Ind --  借据出表状态
	,line_Cate_Cd --  条线标识
	,Cust_Id --  客户编号w
    ,ECIF_Cust_Id --  ECIF客户编号
	,Prod_Id --  业务品种
	,In_Bal_Off_Bal_Ind --  表内外标识
	,Distr_Dt --  借据起始日
	,Agmt_Dt --   到期日
	,Finali_Dt --   终结日
	,Repay_Freq_Cd --   还款频率
	,BASE_INT_RATE_CATE_CD  --  基准利率类型
	,Base_Int_Rate --   基准年利率(%)
	,Int_Rate_Flt_Mod_Cd --  利率浮动方式
	,Int_Rate_Flt --   利率浮动值(%)
	,Distr_Int_Rate --   借据执行年利率(%)
	,Int_Rate_Adj_Mod_Cd --   借据利率调整方式
	,Loan_Usg --   贷款用途
	,Cur_Cd --   币种
	,Exchg_Rate --   汇率
	,Dubil_Amt  --  
	,BAL --  
	,Fiv_Cls_Cd --   当月五级分类
    ,Last_Fiv_Cls_Cd  -- 上月五级分类
	,Agmt_Grade_Rest_Cd --   当月十二级分类
    ,Last_Agmt_Grade_Rest_Cd  -- 上月十二级分类
	,Ovrd_Days --   逾期天数
	,Core_Org_Num --   借据核心机构号
	,Oprr_Id --   经办人
	,Opr_Org_Id --   经办机构
	,Term_Mth --   期限
	,Dubil_Stat_Cd -- 借据状态代码
	,Inds_Drct_Cd --  a
	,Repay_Mod_Cd --  还款方式
	,County_CoDe -- 行政区划
	,  Green_Pct   -- 绿色占比
	,PBC_Std    -- 人行标准
	,bp_Std  -- 银监标准
	,Is_Green_Loan_Cd   -- 是否绿色贷款
	,line_Ind_2022   -- 条线标识2022
	,Draw_ID   -- 提用编号
    )
    select    
    '''||TX_DATE||''' 
    ,T1.SERIALNO AS Agmt_Id --  借据流水号  525245
      ,
       T1.RELATIVESERIALNO2 AS Contr_Agmt_Id --  合同流水号
      ,
    case T1.ISABSASSET when 
           ''1'' then ''04''
	  when 
           ''2'' then ''01''
	 when 
           ''3'' then ''05''
	  when 
          ''99'' then ''02''
	  when 
           ''4'' then ''03''
	  when 
           ''5'' then ''06''
    else ''01''
    end AS  Be_Abs_Ind --  借据出表状态 
  ,
       case
         when t1.LINETYPE = ''0010'' then
          ''0''
         when t1.LINETYPE = ''0020'' then
          ''1''
         when t1.LINETYPE = ''0030'' then
          ''2''
         when t1.LINETYPE = ''0040'' then
          ''3''
         else
          t1.LINETYPE
       end AS line_Cate_Cd --  条线标识                             -- 
      ,
       T1.Customerid AS Cust_Id --  客户编号w

      ,ci.MFCUSTOMERID as ECIF_Cust_Id --    ECIF客户编号
      ,
       T1.BUSINESSTYPE AS Prod_Id --  业务品种
      ,
       T4.offsheetflag AS In_Bal_Off_Bal_Ind --  表内外标识
      ,
       T1.PUTOUTDATE AS Distr_Dt --  借据起始日
      ,
       T1.MATURITY AS Agmt_Dt --   到期日
      ,
       CASE
         WHEN T1.FINISHDATE IS NULL OR LENGTH(T1.FINISHDATE) = 10 THEN
          T1.FINISHDATE
         ELSE
          ''2099/12/31''
       END AS Finali_Dt --   终结日
      ,
       case
         when t1.paycyc = ''0'' then
          ''01''
         when t1.paycyc = ''1'' then
          ''02''
         when t1.paycyc = ''2'' then
          ''12''
         when t1.paycyc = ''3'' then
          ''30''
         when t1.paycyc = ''4'' then
          ''25''
         when t1.paycyc = ''5'' then
          ''26''
         when t1.paycyc = ''7'' then
          ''27''
         when t1.paycyc = ''8'' then
          ''28''
         when t1.paycyc = ''9'' then
          ''29''
         when t1.paycyc = ''D0'' then
          ''03''
         when t1.paycyc = ''D3'' then
          ''04''
         when t1.paycyc = ''D5'' then
          ''05''
         when t1.paycyc = ''D7'' then
          ''06''
         when t1.paycyc = ''M2'' then
          ''07''
         when t1.paycyc = ''MF'' then
          ''08''
         when t1.paycyc = ''QL'' then
          ''09''
         when t1.paycyc = ''T1'' then
          ''10''
         when t1.paycyc = ''T2'' then
          ''11''
         when t1.paycyc = ''T3'' then
          ''13''
         when t1.paycyc = ''T4'' then
          ''14''
         when t1.paycyc = ''T5'' then
          ''15''
         when t1.paycyc = ''T6'' then
          ''16''
         when t1.paycyc = ''T7'' then
          ''17''
         when t1.paycyc = ''W3'' then
          ''18''
         when t1.paycyc = ''X1'' then
          ''19''
         when t1.paycyc = ''Y2'' then
          ''20''
         when t1.paycyc = ''Y3'' then
          ''21''
         when t1.paycyc = ''Y4'' then
          ''22''
         when t1.paycyc = ''Y5'' then
          ''23''
         when t1.paycyc = ''ZD'' then
          ''24''
         else
          null
       end AS Repay_Freq_Cd --   还款频率
      ,
       T3.BaseRateType --  基准利率类型
      ,
       T3.BaseRate AS Base_Int_Rate --   基准年利率(%)
      ,
       CASE
         WHEN T6.RATEFLOATTYPE = ''0'' THEN
          ''2''
         WHEN T6.RATEFLOATTYPE = ''1'' THEN
          ''1''
         ELSE
          T6.RATEFLOATTYPE
       END AS Int_Rate_Flt_Mod_Cd --  利率浮动方式
      ,
        CASE
         WHEN T6.RATEFLOAT = ''0'' THEN
		 null
		 ELSE
		 T6.RATEFLOAT		
		end  AS Int_Rate_Flt --   利率浮动值(%)  --
      , 
	   case
         when T1.ACTUALBUSINESSRATE = ''0''  THEN
        null
		  ELSE  T1.ACTUALBUSINESSRATE		
		end
	   AS Distr_Int_Rate --   借据执行年利率(%) -- 
      ,
       T1.ADJUSTRATETYPE AS Int_Rate_Adj_Mod_Cd --   借据利率调整方式
      ,
       t3.PURPOSE AS Loan_Usg --   贷款用途
      ,
       T99.Des_Curr_Cd  AS Cur_Cd --   币种
      ,
       T2.EXRATE AS Exchg_Rate --   汇率
      ,
       T1.BUSINESSSUM AS Dubil_Amt  --  
      ,
       T1.BALANCE AS BAL  --  
      ,
       case
         when CR1.FINALLYRESULT_5 like ''A%'' then
          ''1''
         when CR1.FINALLYRESULT_5 like ''B%'' then
          ''2''
         when CR1.FINALLYRESULT_5 like ''C%'' then
          ''3''
         when CR1.FINALLYRESULT_5 like ''D%'' then
          ''4''
         when CR1.FINALLYRESULT_5 like ''E%'' then
          ''5''
       end AS Fiv_Cls_Cd --   当月五级分类
     ,
      case
         when CR.FINALLYRESULT_5 like ''A%'' then
          ''1''
         when CR.FINALLYRESULT_5 like ''B%'' then
          ''2''
         when CR.FINALLYRESULT_5 like ''C%'' then
          ''3''
         when CR.FINALLYRESULT_5 like ''D%'' then
          ''4''
         when CR.FINALLYRESULT_5 like ''E%'' then
          ''5''
       end AS Last_Fiv_Cls_Cd --   上月五级分类
      ,
       case
         when CR1.FINALLYRESULT_12 = ''A1'' then
          ''01''
         when CR1.FINALLYRESULT_12 = ''A2'' then
          ''02''
         when CR1.FINALLYRESULT_12 = ''A3'' then
          ''03''
         when CR1.FINALLYRESULT_12 = ''A4'' then
          ''04''
         when CR1.FINALLYRESULT_12 = ''B1'' then
          ''06''
         when CR1.FINALLYRESULT_12 = ''B2'' then
          ''07''
         when CR1.FINALLYRESULT_12 = ''B3'' then
          ''08''
         when CR1.FINALLYRESULT_12 = ''C1'' then
          ''09''
         when CR1.FINALLYRESULT_12 = ''C2'' then
          ''10''
         when CR1.FINALLYRESULT_12 IN (''D1'',''D2'') then
          ''11''
         when CR1.FINALLYRESULT_12 = ''E'' then
          ''12''
       END AS Agmt_Grade_Rest_Cd --   当月十二级分类
      ,
       case
         when CR.FINALLYRESULT_12 = ''A1'' then
          ''01''
         when CR.FINALLYRESULT_12 = ''A2'' then
          ''02''
         when CR.FINALLYRESULT_12 = ''A3'' then
          ''03''
         when CR.FINALLYRESULT_12 = ''A4'' then
          ''04''
         when CR.FINALLYRESULT_12 = ''B1'' then
          ''06''
         when CR.FINALLYRESULT_12 = ''B2'' then
          ''07''
         when CR.FINALLYRESULT_12 = ''B3'' then
          ''08''
         when CR.FINALLYRESULT_12 = ''C1'' then
          ''09''
         when CR.FINALLYRESULT_12 = ''C2'' then
          ''10''
         when CR.FINALLYRESULT_12 IN (''D1'',''D2'') then
          ''11''
         when CR.FINALLYRESULT_12 = ''E'' then
          ''12''
       END AS Last_Agmt_Grade_Rest_Cd --   上月十二级分类
      ,
       T1.OVERDUEDAYS AS Ovrd_Days --   逾期天数
      ,
       T1.MFORGID AS Core_Org_Num --   借据核心机构号
      ,
       T1.OPERATEUSERID AS Oprr_Id --   经办人
      ,
       T1.OPERATEORGID AS Opr_Org_Id --   经办机构
      ,
       T1.ACTUALTERMMONTH AS Term_Mth --   期限
      ,
       T1.businessstatus as Dubil_Stat_Cd -- 借据状态代码
      ,
       nvl(t3.direction,t5.direction)  as Inds_Drct_Cd -- 行业投向
      ,
       t1.CORPUSPAYMETHOD as Repay_Mod_Cd  --  还款方式
      ,
	   t7.RegionCode as County_CD -- 行政区划
	   ,
	   case
         when nvl(BH2.ATTRIBUTE4, '' '') = ''1'' then
          ''100''
         else
          (case
            when T1.ISGREENLOANS = ''30'' then
             T1.GREENPROPORTION
            else
             (case
               when BH5.ISGREENLOANS = ''30'' then
                BH5.GREENPROPORTION
               else
                ''100''
             end)
          end)
       end as   Green_Pct   -- 绿色占比
	   ,
       case
         when nvl(BH2.ATTRIBUTE4, '' '') = ''1'' then
          nvl(BH4.GREENCREDITTYPE, ''999'')
         else
          (case
            when T1.ISGREENLOANS = ''30'' then
             nvl(T1.GREENCREDITTYPE, ''999'')
            else
             (case
               when BH5.ISGREENLOANS = ''30'' then
                nvl(BH5.GREENCREDITTYPE, ''999'')
               else
                nvl(BH4.GREENCREDITTYPE, ''999'')
             end)
          end)
       end as PBC_Std    -- 人行标准
	   ,
       case
         when nvl(BH2.ATTRIBUTE4, '''') = ''1'' then
          BH4.GREENFINANCINGLOANTYPE
         else
          (case
            when T1.ISGREENLOANS = ''30'' then
             T1.GREENFINANCINGLOANTYPE
            else
             (case
               when BH5.ISGREENLOANS = ''30'' then
                BH5.GREENFINANCINGLOANTYPE
               else
                BH4.GREENFINANCINGLOANTYPE
             end)
          end)
       end as bp_Std  -- 银监标准
	   ,
       case
         when nvl(BH2.ATTRIBUTE4, '' '') = ''1'' then
          BH4.LISTBUSINESSTYPE
         else
          NVL(T1.ISGREENLOANS, BH5.ISGREENLOANS)
       end as Is_Green_Loan_Cd   -- 是否绿色贷款
	   ,
       T1.linetype as line_Ind_2022   -- 条线标识2022
	   ,
       T1.RELATIVESERIALNO1 as Draw_ID   -- 提用编号
  FROM ODS.NCM_BUSINESS_DUEBILL T1
  LEFT JOIN ODS.NCM_BUSINESS_HISTORY T2
    ON T2.SERIALNO = T1.SERIALNO
  --  AND T2.INPUTDATE = ''2023/02/01''
   and '''||TX_DATE||''' BETWEEN T2.SDATE AND T2.EDATE
  left join ODS.NCM_BUSINESS_CONTRACT t3
    on t1.RELATIVESERIALNO2 = t3.SERIALNO
   and '''||TX_DATE||''' BETWEEN T3.SDATE AND T3.EDATE
  LEFT JOIN ODS.NCM_BUSINESS_TYPE T4
    ON T1.BUSINESSTYPE = T4.TYPENO
   and '''||TX_DATE||''' BETWEEN T4.SDATE AND T4.EDATE
  left join ODS.NCM_putout_scheme t5
    on t1.relativeserialno2 = t5.objectno
   and t1.businesstype = t5.businesstype
   and t5.objecttype = ''BusinessContract''
   and '''||TX_DATE||''' BETWEEN T5.SDATE AND T5.EDATE
   left join pdm.t99_curr_cd  T99
    ON  T1.BUSINESSCURRENCY = T99.Src_Curr_Cd 
    AND T99.Data_Src_Cd = ''NCM''
  LEFT JOIN ODS.NCM_BUSINESS_PUTOUT t6
  ON (t6.Serialno=t1.relativeserialno1)
and '''||TX_DATE||''' BETWEEN T6.SDATE AND T6.EDATE
  left join ODS.NCM_ENT_INFO T7
  ON t7.customerid=t1.customerid
and '''||TX_DATE||''' BETWEEN T7.SDATE AND T7.EDATE
INNER  JOIN ODS.ncm_business_history BH1
ON T1.SERIALNO = BH1.SERIALNO
and '''||TX_DATE||''' BETWEEN BH1.SDATE AND BH1.EDATE


INNER  JOIN ODS.NCM_BUSINESS_TYPE BH2
ON T1.BusinessType = BH2.TypeNo
and '''||TX_DATE||''' BETWEEN BH2.SDATE AND BH2.EDATE
INNER  JOIN ODS.NCM_BUSINESS_CONTRACT BH3
ON BH3.SERIALNO = T1.relativeSerialno2
and '''||TX_DATE||''' BETWEEN BH3.SDATE AND BH3.EDATE
INNER  JOIN ODS.NCM_BUSINESS_CONTRACT_EXTRA BH4
ON BH4.bcserialno = BH3.serialno
and '''||TX_DATE||''' BETWEEN BH4.SDATE AND BH4.EDATE
INNER  JOIN ODS.NCM_BUSINESS_PUTOUT BH5
ON   BH5.Serialno = T1.relativeserialno1    
and '''||TX_DATE||''' BETWEEN BH5.SDATE AND BH5.EDATE



left join ODS.NCM_customer_info ci
    on t1.customerid = ci.customerid
   and '''||TX_DATE||''' BETWEEN ci.SDATE AND ci.EDATE
left join (
 select a.ObjectnO,a.FINALLYRESULT_12 ,a.FINALLYRESULT_5
                from (
                        select 
                           ObjectnO,
                           row_number() over(partition by ObjectnO order by FINALLYRESULT  desc, substr(FINALLYRESULT,1,1) desc ) rownum,
                           FINALLYRESULT FINALLYRESULT_12,
                           substr(FINALLYRESULT,1,1)  FINALLYRESULT_5 
                         from ods.ncm_classify_record cr 
                         where ACCOUNTMONTH=  substring(comm.get_last_month_first_date('''||TX_DATE||'''),1,6)
                           and '''||TX_DATE||''' BETWEEN cr.SDATE AND cr.EDATE
                           and OBJECTTYPE in (''TwelveClassify'',''DTwelveClassify'')
                           and ISWORK=''1''
                 ) a where a.rownum=''1''
 )CR
on CR.OBJECTNO=T1.RELATIVESERIALNO2
left join (
 select b.ISWORK, b.ObjectnO,b.FINALLYRESULT_12 ,b.FINALLYRESULT_5
                from (
                        select ISWORK,
                           ObjectnO,ACCOUNTMONTH,
                           row_number() over(partition by ObjectnO order by FINALLYRESULT  desc, substr(FINALLYRESULT,1,1) desc ) rownum,
                           FINALLYRESULT FINALLYRESULT_12,
                           substr(FINALLYRESULT,1,1)  FINALLYRESULT_5 
                         from ods.ncm_classify_record cr 
                         where  
                         ACCOUNTMONTH=  substring(comm.get_month_first_date('''||TX_DATE||'''),1,6)
                           and '''||TX_DATE||''' BETWEEN cr.SDATE AND cr.EDATE
                           and OBJECTTYPE in (''TwelveClassify'',''DTwelveClassify'')
                           and ISWORK=''1''
                 ) b where b.rownum=''1''
 )CR1
on CR1.OBJECTNO=T1.RELATIVESERIALNO2
 WHERE (T1.FINISHDATE is null  or BH1.balance >0)
    -- OR T1.FINISHDATE > ''2022/03/10''
	 and '''||TX_DATE||''' BETWEEN T1.SDATE AND T1.EDATE
;';
         
CALL etl.PR_EXEC_SQL(@RTC,'',ETL_SQL_UNIT,ETL_STEP_NO,@SQL_STR,IN_TX_DATE);
SET OUT_FLAG = @RTC;
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

END |