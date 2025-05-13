CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_EXPOSURE_0808(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_HG_EXPOSURE_0808
    实现功能:核心系统-回购-信用风险暴露(从数据源核心系统将业务相关信息全量导入RWA回购接口表风险暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISHIYONG
    编写时间:2017-04-07
    单  位  :上海安硕信息技术股份有限公司
    
    源  表1 :RWA.CODE_LIBRARY|RWA代码表
    源	 表2 :RWA.ORG_INFO|机构信息表
    源  表3 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
    源  表4 :RWA_DEV.IRS_CR_CUSTOMER_RATE|非零售客户评级信息表
    源  表5 :RWA_DEV.NCM_BREAKDEFINEDREMARK|信贷违约记录表
    源  表6 :RWA_DEV.BRD_BILL_REPO|票据回购
    源  表7 :RWA_DEV.BRD_REPO|债券回购
    
    目标表  :RWA_DEV.RWA_HG_EXPOSURE|核心系统回购类信用风险暴露表
    变更记录(修改人|修改时间|修改内容):
    pxl 2019/04/15 去除老核心系统相关表，去除债券、票据补录数据
    chengang 2019/04/23 更新RWA_DEV.BRD_BILL_REPO和RWA_DEV.BRD_REPO的机构号
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_EXPOSURE_0808';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  


  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_EXPOSURE';
     

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 债券业务-买入返售债券回购-质押式、买入返售债券回购-买断式、卖出回购债券回购-买断式
    INSERT INTO RWA_DEV.RWA_HG_EXPOSURE(
                 DataDate           																					--数据日期
								,DataNo                                 											--数据流水号
								,ExposureID                             											--风险暴露ID
								,DueID                                  											--债项ID
								,SSysID                                 											--源系统ID
								,ContractID                             											--合同ID
								,ClientID                               											--参与主体ID
								,SOrgID                                 											--源机构ID
								,SOrgName                               											--源机构名称
								,OrgSortNo                              											--所属机构排序号
								,OrgID                                  											--所属机构ID
								,OrgName                                											--所属机构名称
								,AccOrgID                               											--账务机构ID
								,AccOrgName                             											--账务机构名称
								,IndustryID                             											--所属行业代码
								,IndustryName                           											--所属行业名称
								,BusinessLine                           											--业务条线
								,AssetType                              											--资产大类
								,AssetSubType                           											--资产小类
								,BusinessTypeID                         											--业务品种代码
								,BusinessTypeName                       											--业务品种名称
								,CreditRiskDataType                     											--信用风险数据类型
								,AssetTypeOfHaircuts                    											--折扣系数对应资产类别
								,BusinessTypeSTD                        											--权重法业务类型
								,ExpoClassSTD                           											--权重法暴露大类
								,ExpoSubClassSTD                        											--权重法暴露小类
								,ExpoClassIRB                           											--内评法暴露大类
								,ExpoSubClassIRB                        											--内评法暴露小类
								,ExpoBelong                             											--暴露所属标识
								,BookType                               											--账户类别
								,ReguTranType                           											--监管交易类型
								,RepoTranFlag                           											--回购交易标识
								,RevaFrequency                          											--重估频率
								,Currency                               											--币种
								,NormalPrincipal                        											--正常本金余额
								,OverdueBalance                         											--逾期余额
								,NonAccrualBalance                      											--非应计余额
								,OnSheetBalance                         											--表内余额
								,NormalInterest                         											--正常利息
								,OnDebitInterest                        											--表内欠息
								,OffDebitInterest                       											--表外欠息
								,ExpenseReceivable                      											--应收费用
								,AssetBalance                           											--资产余额
								,AccSubject1                            											--科目一
								,AccSubject2                            											--科目二
								,AccSubject3                            											--科目三
								,StartDate                              											--起始日期
								,DueDate                                											--到期日期
								,OriginalMaturity                       											--原始期限
								,ResidualM                              											--剩余期限
								,RiskClassify                           											--风险分类
								,ExposureStatus                         											--风险暴露状态
								,OverdueDays                            											--逾期天数
								,SpecialProvision                       											--专项准备金
								,GeneralProvision                       											--一般准备金
								,EspecialProvision                      											--特别准备金
								,WrittenOffAmount                       											--已核销金额
								,OffExpoSource                          											--表外暴露来源
								,OffBusinessType                        											--表外业务类型
								,OffBusinessSdvsSTD                     											--权重法表外业务类型细分
								,UncondCancelFlag                       											--是否可随时无条件撤销
								,CCFLevel                               											--信用转换系数级别
								,CCFAIRB                                											--高级法信用转换系数
								,ClaimsLevel                            											--债权级别
								,BondFlag                               											--是否为债券
								,BondIssueIntent                        											--债券发行目的
								,NSURealPropertyFlag                    											--是否非自用不动产
								,RepAssetTermType                       											--抵债资产期限类型
								,DependOnFPOBFlag                       											--是否依赖于银行未来盈利
								,IRating                                											--内部评级
								,PD                                     											--违约概率
								,LGDLevel                               											--违约损失率级别
								,LGDAIRB                                											--高级法违约损失率
								,MAIRB                                  											--高级法有效期限
								,EADAIRB                                											--高级法违约风险暴露
								,DefaultFlag                            											--违约标识
								,BEEL                                   											--已违约暴露预期损失比率
								,DefaultLGD                             											--已违约暴露违约损失率
								,EquityExpoFlag                         											--股权暴露标识
								,EquityInvestType                       											--股权投资对象类型
								,EquityInvestCause          																	--股权投资形成原因
								,SLFlag                                 											--专业贷款标识
								,SLType                               												--专业贷款类型
								,PFPhase                                											--项目融资阶段
								,ReguRating                             											--监管评级
								,CBRCMPRatingFlag                       											--银监会认定评级是否更为审慎
								,LargeFlucFlag                          											--是否波动性较大
								,LiquExpoFlag                           											--是否清算过程中风险暴露
								,PaymentDealFlag                        											--是否货款对付模式
								,DelayTradingDays                       											--延迟交易天数
								,SecuritiesFlag                         											--有价证券标识
								,SecuIssuerID                           											--证券发行人ID
								,RatingDurationType                     											--评级期限类型
								,SecuIssueRating                        											--证券发行等级
								,SecuResidualM                          											--证券剩余期限
								,SecuRevaFrequency                      											--证券重估频率
								,CCPTranFlag                            											--是否中央交易对手相关交易
								,CCPID                                  											--中央交易对手ID
								,QualCCPFlag                         													--是否合格中央交易对手
								,BankRole                               											--银行角色
								,ClearingMethod                        												--清算方式
								,BankAssetFlag                          											--是否银行提交资产
								,MatchConditions    																					--符合条件情况
								,SFTFlag                                											--证券融资交易标识
								,MasterNetAgreeFlag                     											--净额结算主协议标识
								,MasterNetAgreeID                       											--净额结算主协议ID
								,SFTType                                											--证券融资交易类型
								,SecuOwnerTransFlag                     											--证券所有权是否转移
								,OTCFlag                                 											--场外衍生工具标识
								,ValidNettingFlag                       											--有效净额结算协议标识
								,ValidNetAgreementID                    											--有效净额结算协议ID
								,OTCType                                											--场外衍生工具类型
								,DepositRiskPeriod                      											--保证金风险期间
								,MTM                                    											--重置成本
								,MTMCurrency                            											--重置成本币种
								,BuyerOrSeller                          											--买方卖方
								,QualROFlag                             											--合格参照资产标识
								,ROIssuerPerformFlag                    											--参照资产发行人是否能履约
								,BuyerInsolvencyFlag                    											--信用保护买方是否破产
								,NonpaymentFees                         											--尚未支付费用
								,RetailExpoFlag                         											--零售暴露标识
								,RetailClaimType                        											--零售债权类型
								,MortgageType                           											--住房抵押贷款类型
								,ExpoNumber                             											--风险暴露个数
								,LTV                                    											--贷款价值比
								,Aging                                  											--账龄
								,NewDefaultDebtFlag                     											--新增违约债项标识
								,PDPoolModelID                          											--PD分池模型ID
								,LGDPoolModelID                         											--LGD分池模型ID
								,CCFPoolModelID                         											--CCF分池模型ID
								,PDPoolID           																					--所属PD池ID
								,LGDPoolID             																				--所属LGD池ID
								,CCFPoolID                                      							--所属CCF池ID
								,ABSUAFlag           																					--资产证券化基础资产标识
								,ABSPoolID                                                    --证券化资产池ID
								,GroupID                                                      --分组编号
								,DefaultDate                                                  --违约时点
								,ABSPROPORTION																								--资产证券化比重
								,DEBTORNUMBER																									--借款人个数
    )
    SELECT 
        TO_DATE(p_data_dt_str,'YYYYMMDD') , --数据日期
        p_data_dt_str , --数据流水号
        T1.ACCT_NO  , --风险暴露ID
        T1.ACCT_NO  , --债项ID
        'HG'  , --源系统ID
        'MRFSZQ' || T1.ACCT_NO  , --合同ID
        T1.CUST_NO  , --参与主体ID
        T1.ORG_CD , --源机构ID
        T2.ORGNAME  , --源机构名称
        T2.SORTNO , --所属机构排序号
        T1.ORG_CD , --所属机构ID
        T2.ORGNAME  , --所属机构名称
        T1.ORG_CD , --账务机构ID
        T2.ORGNAME  , --账务机构名称
        T3.INDUSTRYTYPE , --所属行业代码
        T4.ITEMNAME , --所属行业名称
        '0401'  , --业务条线
        ''  , --资产大类
        ''  , --资产小类
        CASE
           WHEN T1.REPO_TYPE IN ('4', 'RB') THEN
            '1040102010' --逆回购 债券买入返售质押式
           WHEN T1.REPO_TYPE IN ('2', 'RS') THEN
            '1040102010' --正回购 债券卖出回购质押式 
           ELSE
            '未知'
         END , --业务品种代码 
        /*  数据问题 由于源系统  T1.CLIENT_PROPRIETARY 是否可以再质押字段全部为空  结合行内尚无买断式业务  简化业务逻辑
        
        CASE
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '1040102020' --逆回购 债券买入返售买断式
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '1040102010' --逆回购 债券买入返售质押式
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '1040102020' --正回购 债券卖出回购买断式
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '1040102010' --正回购 债券卖出回购质押式 
           ELSE
            '未知'
         END , --业务品种代码  */
        
        CASE
           WHEN T1.REPO_TYPE IN ('4', 'RB') THEN
            '债券质押式回购' --逆回购 债券买入返售质押式
           WHEN T1.REPO_TYPE IN ('2', 'RS') THEN
            '债券质押式回购' --正回购 债券卖出回购质押式
           ELSE
            '未知'
         END, --业务品种名称
         
        /* 同上
        CASE
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '债券买断式回购' --逆回购 债券买入返售买断式
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '债券质押式回购' --逆回购 债券买入返售质押式
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '债券买断式回购' --正回购 债券卖出回购买断式
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '债券质押式回购' --正回购 债券卖出回购质押式
           ELSE
            '未知'
         END , --业务品种名称*/
        
        CASE
           WHEN T1.REPO_TYPE IN ('4', 'RB') THEN
            '01' --逆回购 债券买入返售质押式
           WHEN T1.REPO_TYPE IN ('2', 'RS') THEN
            '01' --正回购 债券卖出回购质押式
           ELSE
            '未知'
         END ,  --信用风险数据类型      
        /* 同上        
        CASE
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '06' --逆回购 债券买入返售买断式
           WHEN (T1.REPO_TYPE = '4' OR T1.PROD_TYPE = 'RB') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '01' --逆回购 债券买入返售质押式
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'T' THEN
            '06' --正回购 债券卖出回购买断式
           WHEN (T1.REPO_TYPE = '2' OR T1.PROD_TYPE = 'RS') AND T1.CLIENT_PROPRIETARY = 'F' THEN
            '01' --正回购 债券卖出回购质押式
           ELSE
            '未知'
         END , --信用风险数据类型 */
        '01'  , --折扣系数对应资产类别
        ''  , --权重法业务类型
        ''  , --权重法暴露大类
        ''  , --权重法暴露小类
        ''  , --内评法暴露大类
        ''  , --内评法暴露小类
        CASE WHEN T1.CLIENT_PROPRIETARY = 'T' THEN  '03' ELSE '01' END    , --暴露所属标识
        '01'  , --账户类别
        '01'  , --监管交易类型
        '1' , --回购交易标识
        1   , --重估频率
        T1.CASH_CCY_CD  , --币种
        NVL(T1.CASH_NOMINAL, 0) , --正常本金余额
        0 , --逾期余额
        0 , --非应计余额
        NVL(T1.CASH_NOMINAL, 0) , --表内余额
        NVL(T1.ACCRUAL, 0)  , --正常利息
        0 , --表内欠息
        0 , --表外欠息
        0 , --应收费用
        NVL(T1.CASH_NOMINAL, 0) , --资产余额
        T1.PRINCIPAL_GLNO , --科目一
        ''  , --科目二
        ''  , --科目三
        T1.START_DT , --起始日期
        T1.MATU_DT  , --到期日期
        CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                              THEN 0
                              ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                        END  , --原始期限
        CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                END  , --剩余期限
        '01'  , --风险分类
        '01'  , --风险暴露状态
        T1.ODUE_DT  , --逾期天数
        0 , --专项准备金
        0 , --一般准备金
        0 , --特别准备金
        0 , --已核销金额
        ''  , --表外暴露来源
        ''  , --表外业务类型
        ''  , --权重法表外业务类型细分
        ''  , --是否可随时无条件撤销
        ''  , --信用转换系数级别
        NULL  , --高级法信用转换系数
        '01'  , --债权级别
        '0' , --是否为债券
        '02'  , --债券发行目的
        '0' , --是否非自用不动产
        ''  , --抵债资产期限类型
        '0' , --是否依赖于银行未来盈利
        T5.PDADJLEVEL , --内部评级
        T5.PD , --违约概率
        NULL  , --违约损失率级别
        NULL  , --高级法违约损失率
        NULL  , --高级法有效期限
        NULL  , --高级法违约风险暴露
        CASE WHEN T6.BREAKDATE IS NOT NULL THEN '1' ELSE '0' END  , --违约标识
        0.45  , --已违约暴露预期损失比率
        0.45  , --已违约暴露违约损失率
        '0' , --股权暴露标识
        ''  , --股权投资对象类型
        ''  , --股权投资形成原因
        '0' , --专业贷款标识
        ''  , --专业贷款类型
        ''  , --项目融资阶段
        '01'  , --监管评级
        ''  , --银监会认定评级是否更为审慎
        ''  , --是否波动性较大
        '0' , --是否清算过程中风险暴露
        '0' , --是否货款对付模式
        NULL  , --延迟交易天数
        '0' , --有价证券标识
        ''  , --证券发行人ID
        ''  , --评级期限类型
        ''  , --证券发行等级
        NULL  , --证券剩余期限
        1 , --证券重估频率
        '0' , --是否中央交易对手相关交易
        ''  , --中央交易对手ID
        ''  , --是否合格中央交易对手
        ''  , --银行角色
        ''  , --清算方式
        '0' , --是否银行提交资产
        ''  , --符合条件情况
        CASE WHEN T1.CLIENT_PROPRIETARY = 'T' THEN  '1' ELSE '0' END  , --证券融资交易标识
        '0' , --净额结算主协议标识
        ''  , --净额结算主协议ID
        CASE WHEN  T1.REPO_TYPE = '4' THEN '02' ELSE '01' END , --证券融资交易类型
        CASE WHEN T1.CLIENT_PROPRIETARY = 'T' THEN  '1' ELSE '0'  END , --证券所有权是否转移
        '0' , --场外衍生工具标识
        '0' , --有效净额结算协议标识
        ''  , --有效净额结算协议ID
        ''  , --场外衍生工具类型
        ''  , --保证金风险期间
        NULL  , --重置成本
        NULL  , --重置成本币种
        ''  , --买方卖方
        '0' , --合格参照资产标识
        ''  , --参照资产发行人是否能履约
        ''  , --信用保护买方是否破产
        NULL  , --尚未支付费用
        '0' , --零售暴露标识
        ''  , --零售债权类型
        ''  , --住房抵押贷款类型
        1 , --风险暴露个数
        0.8 , --贷款价值比
        NULL  , --账龄
        ''  , --新增违约债项标识
        ''  , --PD分池模型ID
        ''  , --LGD分池模型ID
        ''  , --CCF分池模型ID
        ''  , --所属PD池ID
        ''  , --所属LGD池ID
        ''  , --所属CCF池ID
        '0' , --资产证券化基础资产标识
        ''  , --证券化资产池ID
        ''  , --分组编号
        CASE WHEN T5.PDADJLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END , --违约时点  需要关联rwa.CODE_LIBARY
        NULL  , --资产证券化比重
        NULL   --借款人个数

    FROM BRD_REPO      T1      
    LEFT JOIN RWA.ORG_INFO      T2  ON T1.ORG_CD = T2.ORGID AND T2.STATUS = '1'     
    LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO     T3  ON T1.CUST_NO = T3.CUSTOMERID AND T3.CUSTOMERTYPE NOT LIKE '03%' --对公客户   
    LEFT JOIN RWA.CODE_LIBRARY      T4  ON T3.INDUSTRYTYPE = T4.ITEMNO AND T4.CODENO = 'IndustryType'   
    LEFT JOIN RWA_DEV.IRS_CR_CUSTOMER_RATE      T5  ON T1.CUST_NO = T5.T_IT_CUSTOMER_ID   
    LEFT JOIN RWA_DEV.NCM_BREAKDEFINEDREMARK      T6  ON T1.CUST_NO = T6.CUSTOMERID   
    WHERE T1.CASH_NOMINAL<> 0 
      AND T1.PRINCIPAL_GLNO IS NOT NULL
      ;

    COMMIT;

    --2.2 票据业务-买入返售票据回购
    INSERT INTO RWA_DEV.RWA_HG_EXPOSURE(
                DataDate           																						--数据日期
								,DataNo                                 											--数据流水号
								,ExposureID                             											--风险暴露ID
								,DueID                                  											--债项ID
								,SSysID                                 											--源系统ID
								,ContractID                             											--合同ID
								,ClientID                               											--参与主体ID
								,SOrgID                                 											--源机构ID
								,SOrgName                               											--源机构名称
								,OrgSortNo                              											--所属机构排序号
								,OrgID                                  											--所属机构ID
								,OrgName                                											--所属机构名称
								,AccOrgID                               											--账务机构ID
								,AccOrgName                             											--账务机构名称
								,IndustryID                             											--所属行业代码
								,IndustryName                           											--所属行业名称
								,BusinessLine                           											--业务条线
								,AssetType                              											--资产大类
								,AssetSubType                           											--资产小类
								,BusinessTypeID                         											--业务品种代码
								,BusinessTypeName                       											--业务品种名称
								,CreditRiskDataType                     											--信用风险数据类型
								,AssetTypeOfHaircuts                    											--折扣系数对应资产类别
								,BusinessTypeSTD                        											--权重法业务类型
								,ExpoClassSTD                           											--权重法暴露大类
								,ExpoSubClassSTD                        											--权重法暴露小类
								,ExpoClassIRB                           											--内评法暴露大类
								,ExpoSubClassIRB                        											--内评法暴露小类
								,ExpoBelong                             											--暴露所属标识
								,BookType                               											--账户类别
								,ReguTranType                           											--监管交易类型
								,RepoTranFlag                           											--回购交易标识
								,RevaFrequency                          											--重估频率
								,Currency                               											--币种
								,NormalPrincipal                        											--正常本金余额
								,OverdueBalance                         											--逾期余额
								,NonAccrualBalance                      											--非应计余额
								,OnSheetBalance                         											--表内余额
								,NormalInterest                         											--正常利息
								,OnDebitInterest                        											--表内欠息
								,OffDebitInterest                       											--表外欠息
								,ExpenseReceivable                      											--应收费用
								,AssetBalance                           											--资产余额
								,AccSubject1                            											--科目一
								,AccSubject2                            											--科目二
								,AccSubject3                            											--科目三
								,StartDate                              											--起始日期
								,DueDate                                											--到期日期
								,OriginalMaturity                       											--原始期限
								,ResidualM                              											--剩余期限
								,RiskClassify                           											--风险分类
								,ExposureStatus                         											--风险暴露状态
								,OverdueDays                            											--逾期天数
								,SpecialProvision                       											--专项准备金
								,GeneralProvision                       											--一般准备金
								,EspecialProvision                      											--特别准备金
								,WrittenOffAmount                       											--已核销金额
								,OffExpoSource                          											--表外暴露来源
								,OffBusinessType                        											--表外业务类型
								,OffBusinessSdvsSTD                     											--权重法表外业务类型细分
								,UncondCancelFlag                       											--是否可随时无条件撤销
								,CCFLevel                               											--信用转换系数级别
								,CCFAIRB                                											--高级法信用转换系数
								,ClaimsLevel                            											--债权级别
								,BondFlag                               											--是否为债券
								,BondIssueIntent                        											--债券发行目的
								,NSURealPropertyFlag                    											--是否非自用不动产
								,RepAssetTermType                       											--抵债资产期限类型
								,DependOnFPOBFlag                       											--是否依赖于银行未来盈利
								,IRating                                											--内部评级
								,PD                                     											--违约概率
								,LGDLevel                               											--违约损失率级别
								,LGDAIRB                                											--高级法违约损失率
								,MAIRB                                  											--高级法有效期限
								,EADAIRB                                											--高级法违约风险暴露
								,DefaultFlag                            											--违约标识
								,BEEL                                   											--已违约暴露预期损失比率
								,DefaultLGD                             											--已违约暴露违约损失率
								,EquityExpoFlag                         											--股权暴露标识
								,EquityInvestType                       											--股权投资对象类型
								,EquityInvestCause          																	--股权投资形成原因
								,SLFlag                                 											--专业贷款标识
								,SLType                               												--专业贷款类型
								,PFPhase                                											--项目融资阶段
								,ReguRating                             											--监管评级
								,CBRCMPRatingFlag                       											--银监会认定评级是否更为审慎
								,LargeFlucFlag                          											--是否波动性较大
								,LiquExpoFlag                           											--是否清算过程中风险暴露
								,PaymentDealFlag                        											--是否货款对付模式
								,DelayTradingDays                       											--延迟交易天数
								,SecuritiesFlag                         											--有价证券标识
								,SecuIssuerID                           											--证券发行人ID
								,RatingDurationType                     											--评级期限类型
								,SecuIssueRating                        											--证券发行等级
								,SecuResidualM                          											--证券剩余期限
								,SecuRevaFrequency                      											--证券重估频率
								,CCPTranFlag                            											--是否中央交易对手相关交易
								,CCPID                                  											--中央交易对手ID
								,QualCCPFlag                         													--是否合格中央交易对手
								,BankRole                               											--银行角色
								,ClearingMethod                        												--清算方式
								,BankAssetFlag                          											--是否银行提交资产
								,MatchConditions    																					--符合条件情况
								,SFTFlag                                											--证券融资交易标识
								,MasterNetAgreeFlag                     											--净额结算主协议标识
								,MasterNetAgreeID                       											--净额结算主协议ID
								,SFTType                                											--证券融资交易类型
								,SecuOwnerTransFlag                     											--证券所有权是否转移
								,OTCFlag                                 											--场外衍生工具标识
								,ValidNettingFlag                       											--有效净额结算协议标识
								,ValidNetAgreementID                    											--有效净额结算协议ID
								,OTCType                                											--场外衍生工具类型
								,DepositRiskPeriod                      											--保证金风险期间
								,MTM                                    											--重置成本
								,MTMCurrency                            											--重置成本币种
								,BuyerOrSeller                          											--买方卖方
								,QualROFlag                             											--合格参照资产标识
								,ROIssuerPerformFlag                    											--参照资产发行人是否能履约
								,BuyerInsolvencyFlag                    											--信用保护买方是否破产
								,NonpaymentFees                         											--尚未支付费用
								,RetailExpoFlag                         											--零售暴露标识
								,RetailClaimType                        											--零售债权类型
								,MortgageType                           											--住房抵押贷款类型
								,ExpoNumber                             											--风险暴露个数
								,LTV                                    											--贷款价值比
								,Aging                                  											--账龄
								,NewDefaultDebtFlag                     											--新增违约债项标识
								,PDPoolModelID                          											--PD分池模型ID
								,LGDPoolModelID                         											--LGD分池模型ID
								,CCFPoolModelID                         											--CCF分池模型ID
								,PDPoolID           																					--所属PD池ID
								,LGDPoolID             																				--所属LGD池ID
								,CCFPoolID                                      							--所属CCF池ID
								,ABSUAFlag           																					--资产证券化基础资产标识
								,ABSPoolID                                                    --证券化资产池ID
								,GroupID                                                      --分组编号
								,DefaultDate                                                  --违约时点
								,ABSPROPORTION																								--资产证券化比重
								,DEBTORNUMBER																									--借款人个数
    )
    SELECT
            TO_DATE(p_data_dt_str,'YYYYMMDD')   , --数据日期
            p_data_dt_str   , --数据流水号
            T1.ACCT_NO    , --风险暴露ID
            T1.ACCT_NO    , --债项ID
            'HG'    , --源系统ID
            'MRFSPJ' || T1.ACCT_NO    , --合同ID
            T1.CUST_NO    , --参与主体ID
            T1.ORG_CD   , --源机构ID
            T2.ORGNAME    , --源机构名称
            T2.SORTNO   , --所属机构排序号
            T1.ORG_CD   , --所属机构ID
            T2.ORGNAME    , --所属机构名称
            T1.ORG_CD   , --账务机构ID
            T2.ORGNAME    , --账务机构名称
            T3.INDUSTRYTYPE   , --所属行业代码
            T4.ITEMNAME   , --所属行业名称
            '0401'    , --业务条线
            ''    , --资产大类
            ''    , --资产小类
            '10303010'    , --业务品种代码
            '汇票转贴现业务'   , --业务品种名称
            '01'    , --信用风险数据类型
            '01'    , --折扣系数对应资产类别
            ''    , --权重法业务类型
            ''    , --权重法暴露大类
            ''    , --权重法暴露小类
            ''    , --内评法暴露大类
            ''    , --内评法暴露小类
            '01'    , --暴露所属标识
            '01'    , --账户类别
            '01'    , --监管交易类型
            '1'   , --回购交易标识
            1     , --重估频率
            T1.CASH_CCY_CD    , --币种
            T1.CASH_NOMINAL   , --正常本金余额
            0   , --逾期余额
            0   , --非应计余额
            T1.CASH_NOMINAL   , --表内余额
            T1.ACCRUAL    , --正常利息
            0   , --表内欠息
            0   , --表外欠息
            0   , --应收费用
            T1.CASH_NOMINAL   , --资产余额
            T1.PRINCIPAL_GLNO   , --科目一
            ''    , --科目二
            ''    , --科目三
            T1.START_DT   , --起始日期
            T1.MATU_DT    , --到期日期
            CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(T1.START_DT,'YYYY-MM-DD')) / 365
                            END    , --原始期限
            CASE WHEN (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                          THEN 0
                          ELSE (TO_DATE(T1.MATU_DT,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                    END    , --剩余期限
            '01'    , --风险分类
            '01'    , --风险暴露状态
            T1.ODUE_DT    , --逾期天数
            0   , --专项准备金
            0   , --一般准备金
            0   , --特别准备金
            0   , --已核销金额
            ''   , --表外暴露来源
            ''   , --表外业务类型
            ''   , --权重法表外业务类型细分
            ''   , --是否可随时无条件撤销
            ''   , --信用转换系数级别
            NULL    , --高级法信用转换系数
            '01'    , --债权级别
            '0'   , --是否为债券
            '02'    , --债券发行目的
            '0'   , --是否非自用不动产
            ''    , --抵债资产期限类型
            '0'   , --是否依赖于银行未来盈利
            T5.PDADJLEVEL  , --内部评级
            T5.PD   , --违约概率
            NULL    , --违约损失率级别
            NULL    , --高级法违约损失率
            NULL    , --高级法有效期限
            NULL    , --高级法违约风险暴露
            CASE WHEN T6.BREAKDATE IS NOT NULL THEN '1' ELSE '0' END    , --违约标识
            0.45    , --已违约暴露预期损失比率
            0.45    , --已违约暴露违约损失率
            '0'   , --股权暴露标识
            ''    , --股权投资对象类型
            ''    , --股权投资形成原因
            '0'   , --专业贷款标识
            ''    , --专业贷款类型
            ''    , --项目融资阶段
            '01'    , --监管评级
            ''    , --银监会认定评级是否更为审慎
            ''    , --是否波动性较大
            '0'   , --是否清算过程中风险暴露
            '0'   , --是否货款对付模式
            NULL    , --延迟交易天数
            '0'   , --有价证券标识
            ''    , --证券发行人ID
            ''    , --评级期限类型
            ''    , --证券发行等级
            NULL    , --证券剩余期限
            1   , --证券重估频率
            '0'   , --是否中央交易对手相关交易
            ''    , --中央交易对手ID
            ''    , --是否合格中央交易对手
            ''    , --银行角色
            ''    , --清算方式
            '0'   , --是否银行提交资产
            ''    , --符合条件情况
            '0'   , --证券融资交易标识
            '0'   , --净额结算主协议标识
            ''    , --净额结算主协议ID
            ''    , --证券融资交易类型
            '0'   , --证券所有权是否转移
            '0'   , --场外衍生工具标识
            '0'   , --有效净额结算协议标识
            ''    , --有效净额结算协议ID
            ''    , --场外衍生工具类型
            ''    , --保证金风险期间
            ''    , --重置成本
            ''    , --重置成本币种
            ''    , --买方卖方
            '0'   , --合格参照资产标识
            ''    , --参照资产发行人是否能履约
            ''    , --信用保护买方是否破产
            ''    , --尚未支付费用
            '0'   , --零售暴露标识
            ''    , --零售债权类型
            ''    , --住房抵押贷款类型
            1   , --风险暴露个数
            0.8   , --贷款价值比
            NULL    , --账龄
            ''    , --新增违约债项标识
            ''    , --PD分池模型ID
            ''    , --LGD分池模型ID
            ''    , --CCF分池模型ID
            ''    , --所属PD池ID
            ''    , --所属LGD池ID
            ''    , --所属CCF池ID
            '0'   , --资产证券化基础资产标识
            ''    , --证券化资产池ID
            ''    , --分组编号
            CASE WHEN T5.PDADJLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
                     ELSE NULL
                     END   , --违约时点
            ''    , --资产证券化比重
            ''      --借款人个数
    FROM  RWA_DEV.BRD_BILL_REPO     T1  --票据回购    
    LEFT JOIN RWA.ORG_INFO      T2  ON T1.ORG_CD = T2.ORGID AND T2.STATUS = '1'           
    LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO     T3  ON T1.CUST_NO = T3.CUSTOMERID AND T3.CUSTOMERTYPE NOT LIKE '01%' --对公客户         
    LEFT JOIN RWA.CODE_LIBRARY      T4  ON T3.INDUSTRYTYPE = T4.ITEMNO AND T4.CODENO = 'IndustryType'         
    LEFT JOIN RWA_DEV.IRS_CR_CUSTOMER_RATE      T5  ON T1.CUST_NO = T5.T_IT_CUSTOMER_ID         
    LEFT JOIN RWA_DEV.NCM_BREAKDEFINEDREMARK      T6  ON T1.CUST_NO = T6.CUSTOMERID         
     WHERE T1.CASH_NOMINAL <> 0 --过滤无效数据
       AND T1.PRINCIPAL_GLNO IS NOT NULL --经ALM集市反馈  科目为空的数据不计帐为历史数据
       AND SUBSTR(T1.PRINCIPAL_GLNO, 1, 6) = '111102' --买入返售金融资产-买入返售票据
       AND (T1.CLIENT_PROPRIETARY <> 'N' OR T1.CLIENT_PROPRIETARY IS NULL )--是否可以再质押 N为买断式  非N质押式
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_EXPOSURE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_EXPOSURE;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_EXPOSURE表当前插入的核心系统-买断式回购数据记录为: ' || (v_count1 - v_count) || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '信用风险暴露('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_EXPOSURE_0808;
/

