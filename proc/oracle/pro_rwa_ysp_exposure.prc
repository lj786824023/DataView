CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_EXPOSURE(P_DATA_DT_STR IN VARCHAR2, --数据日期 yyyyMMdd
                                                 P_PO_RTNCODE  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                                 P_PO_RTNMSG   OUT VARCHAR2 --返回描述
                                                 )
/*
  存储过程名称:RWA_DEV.PRO_RWA_YSP_EXPOSURE
  实现功能:财务系统-衍生品业务-信用风险表
  数据口径:全量
  跑批频率:月初运行
  版  本  :V1.0.0
  编写人  :
  编写时间:2019-04-17
  单  位  :上海安硕信息技术股份有限公司
  源  表1 :RWA_DEV.BRD_SWAP|互换表
  源  表2 :RWA.ORG_INFO|机构信息表
  源  表3 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
  源  表4 :RWA.CODE_LIBRARY|代码库表
  源  表5 :RWA_DEV.IRS_CR_CUSTOMER_RATE|客户评级表
  源  表6 :RWA_DEV.NCM_BREAKDEFINEDREMARK|标识违约定义表
  源  表6 :RWA_DEV.OPI_SWDT\源系统的互换表
  变更记录(修改人|修改时间|修改内容):
  
  */
 AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_YSP_EXPOSURE';
  --定义异常变量
  V_RAISE EXCEPTION;
  --定义当前插入的记录数
  V_COUNT INTEGER;

BEGIN
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*如果是全量数据加载需清空目标表*/
  --1.清除目标表中的原有记录
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_EXPOSURE';

  --2.将满足条件的数据从源表插入到目标表中
  
  --利率互换  货币互换业务

  INSERT INTO RWA_DEV.RWA_YSP_EXPOSURE
    (DATADATE, --数据日期
     DATANO, --数据流水号
     EXPOSUREID, --风险暴露ID
     DUEID, --债项ID
     SSYSID, --源系统ID
     CONTRACTID, --合同ID
     CLIENTID, --参与主体ID
     SORGID, --源机构ID
     SORGNAME, --源机构名称
     ORGSORTNO, --所属机构排序号
     ORGID, --所属机构ID
     ORGNAME, --所属机构名称
     ACCORGID, --账务机构ID
     ACCORGNAME, --账务机构名称
     INDUSTRYID, --所属行业代码
     INDUSTRYNAME, --所属行业名称
     BUSINESSLINE, --业务条线
     ASSETTYPE, --资产大类
     ASSETSUBTYPE, --资产小类
     BUSINESSTYPEID, --业务品种代码
     BUSINESSTYPENAME, --业务品种名称
     CREDITRISKDATATYPE, --信用风险数据类型
     ASSETTYPEOFHAIRCUTS, --折扣系数对应资产类别
     BUSINESSTYPESTD, --权重法业务类型
     EXPOCLASSSTD, --权重法暴露大类
     EXPOSUBCLASSSTD, --权重法暴露小类
     EXPOCLASSIRB, --内评法暴露大类
     EXPOSUBCLASSIRB, --内评法暴露小类
     EXPOBELONG, --暴露所属标识
     BOOKTYPE, --账户类别
     REGUTRANTYPE, --监管交易类型
     REPOTRANFLAG, --回购交易标识
     REVAFREQUENCY, --重估频率
     CURRENCY, --币种
     NORMALPRINCIPAL, --正常本金余额
     OVERDUEBALANCE, --逾期余额
     NONACCRUALBALANCE, --非应计余额
     ONSHEETBALANCE, --表内余额
     NORMALINTEREST, --正常利息
     ONDEBITINTEREST, --表内欠息
     OFFDEBITINTEREST, --表外欠息
     EXPENSERECEIVABLE, --应收费用
     ASSETBALANCE, --资产余额
     ACCSUBJECT1, --科目一
     ACCSUBJECT2, --科目二
     ACCSUBJECT3, --科目三
     STARTDATE, --起始日期
     DUEDATE, --到期日期
     ORIGINALMATURITY, --原始期限
     RESIDUALM, --剩余期限
     RISKCLASSIFY, --风险分类
     EXPOSURESTATUS, --风险暴露状态
     OVERDUEDAYS, --逾期天数
     SPECIALPROVISION, --专项准备金
     GENERALPROVISION, --一般准备金
     ESPECIALPROVISION, --特别准备金
     WRITTENOFFAMOUNT, --已核销金额
     OFFEXPOSOURCE, --表外暴露来源
     OFFBUSINESSTYPE, --表外业务类型
     OFFBUSINESSSDVSSTD, --权重法表外业务类型细分
     UNCONDCANCELFLAG, --是否可随时无条件撤销
     CCFLEVEL, --信用转换系数级别
     CCFAIRB, --高级法信用转换系数
     CLAIMSLEVEL, --债权级别
     BONDFLAG, --是否为债券
     BONDISSUEINTENT, --债券发行目的
     NSUREALPROPERTYFLAG, --是否非自用不动产
     REPASSETTERMTYPE, --抵债资产期限类型
     DEPENDONFPOBFLAG, --是否依赖于银行未来盈利
     IRATING, --内部评级
     PD, --违约概率
     LGDLEVEL, --违约损失率级别
     LGDAIRB, --高级法违约损失率
     MAIRB, --高级法有效期限
     EADAIRB, --高级法违约风险暴露
     DEFAULTFLAG, --违约标识
     BEEL, --已违约暴露预期损失比率
     DEFAULTLGD, --已违约暴露违约损失率
     EQUITYEXPOFLAG, --股权暴露标识
     EQUITYINVESTTYPE, --股权投资对象类型
     EQUITYINVESTCAUSE, --股权投资形成原因
     SLFLAG, --专业贷款标识
     SLTYPE, --专业贷款类型
     PFPHASE, --项目融资阶段
     REGURATING, --监管评级
     CBRCMPRATINGFLAG, --银监会认定评级是否更为审慎
     LARGEFLUCFLAG, --是否波动性较大
     LIQUEXPOFLAG, --是否清算过程中风险暴露
     PAYMENTDEALFLAG, --是否货款对付模式
     DELAYTRADINGDAYS, --延迟交易天数
     SECURITIESFLAG, --有价证券标识
     SECUISSUERID, --证券发行人ID
     RATINGDURATIONTYPE, --评级期限类型
     SECUISSUERATING, --证券发行等级
     SECURESIDUALM, --证券剩余期限
     SECUREVAFREQUENCY, --证券重估频率
     CCPTRANFLAG, --是否中央交易对手相关交易
     CCPID, --中央交易对手ID
     QUALCCPFLAG, --是否合格中央交易对手
     BANKROLE, --银行角色
     CLEARINGMETHOD, --清算方式
     BANKASSETFLAG, --是否银行提交资产
     MATCHCONDITIONS, --符合条件情况
     SFTFLAG, --证券融资交易标识
     MASTERNETAGREEFLAG, --净额结算主协议标识
     MASTERNETAGREEID, --净额结算主协议ID
     SFTTYPE, --证券融资交易类型
     SECUOWNERTRANSFLAG, --证券所有权是否转移
     OTCFLAG, --场外衍生工具标识
     VALIDNETTINGFLAG, --有效净额结算协议标识
     VALIDNETAGREEMENTID, --有效净额结算协议ID
     OTCTYPE, --场外衍生工具类型
     DEPOSITRISKPERIOD, --保证金风险期间
     MTM, --重置成本
     MTMCURRENCY, --重置成本币种
     BUYERORSELLER, --买方卖方
     QUALROFLAG, --合格参照资产标识
     ROISSUERPERFORMFLAG, --参照资产发行人是否能履约
     BUYERINSOLVENCYFLAG, --信用保护买方是否破产
     NONPAYMENTFEES, --尚未支付费用
     RETAILEXPOFLAG, --零售暴露标识
     RETAILCLAIMTYPE, --零售债权类型
     MORTGAGETYPE, --住房抵押贷款类型
     EXPONUMBER, --风险暴露个数
     LTV, --贷款价值比
     AGING, --账龄
     NEWDEFAULTDEBTFLAG, --新增违约债项标识
     PDPOOLMODELID, --PD分池模型ID
     LGDPOOLMODELID, --LGD分池模型ID
     CCFPOOLMODELID, --CCF分池模型ID
     PDPOOLID, --所属PD池ID
     LGDPOOLID, --所属LGD池ID
     CCFPOOLID, --所属CCF池ID
     ABSUAFLAG, --资产证券化基础资产标识
     ABSPOOLID, --证券化资产池ID
     GROUPID, --分组编号
     DEFAULTDATE, --违约时点
     ABSPROPORTION, --资产证券化比重
     DEBTORNUMBER --借款人个数
     )
  
        SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --01数据日期
           p_data_dt_str, --02数据流水号
           T1.DEALNO || T1.SEQ || T1.PAYRECIND, --风险暴露ID
           T1.DEALNO || T1.SEQ || T1.PAYRECIND, --债项ID
           'YSP', --源系统ID
           T1.DEALNO || T1.SEQ || T1.PAYRECIND, --合同ID
           'OPI' || TRIM(T2.CNO), --参与主体ID
           '6001', --源机构ID
           '重庆银行股份有限公司国际业务部', --源机构名称
           '1290', --所属机构排序号
           '6001', --所属机构ID
           '重庆银行股份有限公司国际业务部', --所属机构名称
           '6001', --账务机构ID
           '重庆银行股份有限公司国际业务部', --账务机构名称
           'J6621', --所属行业代码
           '商业银行服务', --所属行业名称
           '0102',  --业务条线
           '223',   --资产大类
           '22301', --资产小类
           CASE
             WHEN substr(T2.Cost,6,1) = '1' THEN
              '404000001'
             WHEN substr(T2.Cost,6,1) in( '2','3') THEN
              '404000002'
           END, --业务品种代码
           CASE
             WHEN substr(T2.Cost,6,1) = '1' THEN
              '利率互换'
             WHEN substr(T2.Cost,6,1) in( '2','3') THEN
              '货币互换'
           END, --业务品种名称
           '05', --信用风险数据类型
           '01', --折扣系数对应资产类别
           '01', --权重法业务类型
           '', --权重法暴露大类
           '', --权重法暴露小类
           '', --内评法暴露大类
           '', --内评法暴露小类
           '03', --暴露所属标识
           CASE
             WHEN SUBSTR(T2.COST,4,1) = '3' THEN
              '01'
             ELSE
              '02'
           END, --账户类别
           '02', --监管交易类型
           '0', --回购交易标识
           '1', --重估频率
           T1.NOTCCY, --币种
           ABS(T1.NOTCCYAMT), --正常本金余额
           '0', --逾期余额
           '0', --非应计余额
           ABS(T1.NOTCCYAMT), --表内余额
           '0', --正常利息
           '0', --表内欠息
           '0', --表外欠息
           '0', --应收费用
           ABS(T1.NOTCCYAMT), --资产余额
           '70120000', --科目一
           '', --科目二
           '', --科目三
           T2.STARTDATE, --起始日期
            CASE
               --浮动利率时
               WHEN T1.FIXFLOATIND = 'L' AND T3.RATEREVDTE IS NOT NULL THEN TO_CHAR(T3.RATEREVDTE,'YYYYMMDD')
               ELSE T2.MATDATE
            END, --到期日期
           CASE
               --浮动利率时
               WHEN T1.FIXFLOATIND = 'L' AND T3.RATEREVDTE IS NOT NULL THEN 
                     CASE
                         WHEN (T3.RATEREVDTE - TO_DATE(T2.STARTDATE, 'YYYY/MM/DD')) / 365 < 0 THEN
                          0
                         ELSE
                          (T3.RATEREVDTE - TO_DATE(T2.STARTDATE, 'YYYY/MM/DD')) / 365
                     END
               --其他情况
               ELSE
                     CASE
                         WHEN (TO_DATE(T2.MATDATE, 'YYYY/MM/DD') - TO_DATE(T2.STARTDATE, 'YYYY/MM/DD')) / 365 < 0 THEN
                          0
                         ELSE
                              (TO_DATE(T2.MATDATE, 'YYYY/MM/DD') - TO_DATE(T2.STARTDATE, 'YYYY/MM/DD')) / 365
                     END
           END, --原始期限   
           CASE
         --浮动利率时
         WHEN T1.FIXFLOATIND = 'L' AND T3.RATEREVDTE IS NOT NULL THEN 
               CASE
                   WHEN (T3.RATEREVDTE - TO_DATE(p_data_dt_str, 'YYYY/MM/DD')) / 365 < 0 THEN
                    0
                   ELSE
                    (T3.RATEREVDTE - TO_DATE(p_data_dt_str, 'YYYY/MM/DD')) / 365
               END         
          --其他情况
          ELSE
               CASE
                   WHEN (TO_DATE(T2.MATDATE, 'YYYY/MM/DD') - TO_DATE(p_data_dt_str, 'YYYY/MM/DD')) / 365 < 0 THEN
                    0
                   ELSE
                        (TO_DATE(T2.MATDATE, 'YYYY/MM/DD') - TO_DATE(p_data_dt_str, 'YYYY/MM/DD')) / 365
               END      
           END, --剩余期限  
           '01', --风险分类
           '01', --风险暴露状态
           '0', --逾期天数
           '0', --专项准备金
           '0', --一般准备金
           '0', --特别准备金
           '0', --已核销金额
           '', --表外暴露来源
           '', --表外业务类型
           '', --权重法表外业务类型细分
           '0', --是否可随时无条件撤销
           '', --信用转换系数级别
           '', --高级法信用转换系数
           '01', --债权级别
           '0', --是否为债券
           '', --债券发行目的
           '0', --是否非自用不动产
           '', --抵债资产期限类型
           '0', --是否依赖于银行未来盈利
           '', --内部评级
           '', --违约概率
           '', --违约损失率级别
           '', --高级法违约损失率
           '', --高级法有效期限
           '', --高级法违约风险暴露
           '', --违约标识
           '0.45', --已违约暴露预期损失比率
           '0.45', --已违约暴露违约损失率
           '0', --股权暴露标识
           '', --股权投资对象类型
           '', --股权投资形成原因
           '0', --专业贷款标识
           '', --专业贷款类型
           '', --项目融资阶段
           '', --监管评级
           '0', --银监会认定评级是否更为审慎
           '0', --是否波动性较大
           '0', --是否清算过程中风险暴露
           '0', --是否货款对付模式
           '0', --延迟交易天数
           '0', --有价证券标识
           '', --证券发行人ID
           '', --评级期限类型
           '', --证券发行等级
           '', --证券剩余期限
           '', --证券重估频率
           '0', --是否中央交易对手相关交易
           '', --中央交易对手ID0
           '0', --是否合格中央交易对手
           '', --银行角色
           '', --清算方式
           '0', --是否银行提交资产
           '', --符合条件情况
           '0', --证券融资交易标识
           '0', --净额结算主协议标识
           '', --净额结算主协议ID
           '', --证券融资交易类型
           '0', --证券所有权是否转移
           '1', --场外衍生工具标识
           '0', --有效净额结算协议标识
           '', --有效净额结算协议ID
           '01', --场外衍生工具类型
           '', --保证金风险期间
           ABS(T2.NPVBAMT), --重置成本（盯市价值）
           T1.NOTCCY, --重置成本币种
           '', --买方卖方
           '0', --合格参照资产标识
           '0', --参照资产发行人是否能履约
           '0', --信用保护买方是否破产
           '0', --尚未支付费用
           '0', --零售暴露标识
           '', --零售债权类型
           '', --住房抵押贷款类型
           '1', --风险暴露个数
           '0.8', --贷款价值比
           '', --账龄
           '', --新增违约债项标识
           '', --PD分池模型ID
           '', --LGD分池模型ID
           '', --CCF分池模型ID
           '', --所属PD池ID
           '', --所属LGD池ID
           '', --所属CCF池ID
           '0', --资产证券化基础资产标识
           '', --证券化资产池ID
           '', --分组编号
           '', -- BREAKDATE--违约时点 
           '', --资产证券化比重
           '' --借款人个数
      FROM OPI_SWDT T1--互换交易
      INNER JOIN OPI_SWDH T2--互换报头 
      ON T1.DEALNO = T2.DEALNO
      AND T2.DATANO = p_data_dt_str
      AND T2.PORT<>'SWDK'    --排除结构性存款业务
      LEFT JOIN (
            SELECT SWDS.DEALNO, SWDS.SEQ,PAYRECIND, TO_DATE(MAX(SWDS.RATEREVDTE)) AS   RATEREVDTE 
              FROM OPI_SWDS SWDS  --利息信息表   获取重定价日期              
            WHERE SWDS.DATANO = p_data_dt_str
              AND SWDS.RATEREVDTE IS NOT NULL 
            GROUP BY SWDS.DEALNO, SWDS.SEQ,PAYRECIND
      )T3
        ON T3.DEALNO = T1.DEALNO 
       AND T3.SEQ = T1.SEQ
       AND T1.PAYRECIND=T3.PAYRECIND
      LEFT JOIN OPI_CUST T4
             ON T2.CNO = T4.CNO         
            AND T4.DATANO = p_data_dt_str
    WHERE T1.DATANO = p_data_dt_str
      AND T1.PAYRECIND = 'R' --交易对手只考虑对手风险     
      AND SUBSTR(T2.COST, 1, 1) = '3' --第一位=3  --数据为利率/货币掉期业务
      AND SUBSTR(T2.COST, 6, 1) IN ('1', '2','3') --第六位=1\2  --利率掉期\货币掉期,远期
      AND T2.VERIND = 1 
      AND TRIM(T2.REVDATE) IS NULL
      AND TO_DATE(T1.MATDATE,'YYYYMMDD')>=TO_DATE(p_data_dt_str,'YYYYMMDD')  --到期日不能小于当前日期       
    ;
    
  COMMIT;
 
  --外汇掉期、远期 、即期
  
  INSERT INTO RWA_DEV.RWA_YSP_EXPOSURE
    (DATADATE, --数据日期
     DATANO, --数据流水号
     EXPOSUREID, --风险暴露ID
     DUEID, --债项ID
     SSYSID, --源系统ID
     CONTRACTID, --合同ID
     CLIENTID, --参与主体ID
     SORGID, --源机构ID
     SORGNAME, --源机构名称
     ORGSORTNO, --所属机构排序号
     ORGID, --所属机构ID
     ORGNAME, --所属机构名称
     ACCORGID, --账务机构ID
     ACCORGNAME, --账务机构名称
     INDUSTRYID, --所属行业代码
     INDUSTRYNAME, --所属行业名称
     BUSINESSLINE, --业务条线
     ASSETTYPE, --资产大类
     ASSETSUBTYPE, --资产小类
     BUSINESSTYPEID, --业务品种代码
     BUSINESSTYPENAME, --业务品种名称
     CREDITRISKDATATYPE, --信用风险数据类型
     ASSETTYPEOFHAIRCUTS, --折扣系数对应资产类别
     BUSINESSTYPESTD, --权重法业务类型
     EXPOCLASSSTD, --权重法暴露大类
     EXPOSUBCLASSSTD, --权重法暴露小类
     EXPOCLASSIRB, --内评法暴露大类
     EXPOSUBCLASSIRB, --内评法暴露小类
     EXPOBELONG, --暴露所属标识
     BOOKTYPE, --账户类别
     REGUTRANTYPE, --监管交易类型
     REPOTRANFLAG, --回购交易标识
     REVAFREQUENCY, --重估频率
     CURRENCY, --币种
     NORMALPRINCIPAL, --正常本金余额
     OVERDUEBALANCE, --逾期余额
     NONACCRUALBALANCE, --非应计余额
     ONSHEETBALANCE, --表内余额
     NORMALINTEREST, --正常利息
     ONDEBITINTEREST, --表内欠息
     OFFDEBITINTEREST, --表外欠息
     EXPENSERECEIVABLE, --应收费用
     ASSETBALANCE, --资产余额
     ACCSUBJECT1, --科目一
     ACCSUBJECT2, --科目二
     ACCSUBJECT3, --科目三
     STARTDATE, --起始日期
     DUEDATE, --到期日期
     ORIGINALMATURITY, --原始期限
     RESIDUALM, --剩余期限
     RISKCLASSIFY, --风险分类
     EXPOSURESTATUS, --风险暴露状态
     OVERDUEDAYS, --逾期天数
     SPECIALPROVISION, --专项准备金
     GENERALPROVISION, --一般准备金
     ESPECIALPROVISION, --特别准备金
     WRITTENOFFAMOUNT, --已核销金额
     OFFEXPOSOURCE, --表外暴露来源
     OFFBUSINESSTYPE, --表外业务类型
     OFFBUSINESSSDVSSTD, --权重法表外业务类型细分
     UNCONDCANCELFLAG, --是否可随时无条件撤销
     CCFLEVEL, --信用转换系数级别
     CCFAIRB, --高级法信用转换系数
     CLAIMSLEVEL, --债权级别
     BONDFLAG, --是否为债券
     BONDISSUEINTENT, --债券发行目的
     NSUREALPROPERTYFLAG, --是否非自用不动产
     REPASSETTERMTYPE, --抵债资产期限类型
     DEPENDONFPOBFLAG, --是否依赖于银行未来盈利
     IRATING, --内部评级
     PD, --违约概率
     LGDLEVEL, --违约损失率级别
     LGDAIRB, --高级法违约损失率
     MAIRB, --高级法有效期限
     EADAIRB, --高级法违约风险暴露
     DEFAULTFLAG, --违约标识
     BEEL, --已违约暴露预期损失比率
     DEFAULTLGD, --已违约暴露违约损失率
     EQUITYEXPOFLAG, --股权暴露标识
     EQUITYINVESTTYPE, --股权投资对象类型
     EQUITYINVESTCAUSE, --股权投资形成原因
     SLFLAG, --专业贷款标识
     SLTYPE, --专业贷款类型
     PFPHASE, --项目融资阶段
     REGURATING, --监管评级
     CBRCMPRATINGFLAG, --银监会认定评级是否更为审慎
     LARGEFLUCFLAG, --是否波动性较大
     LIQUEXPOFLAG, --是否清算过程中风险暴露
     PAYMENTDEALFLAG, --是否货款对付模式
     DELAYTRADINGDAYS, --延迟交易天数
     SECURITIESFLAG, --有价证券标识
     SECUISSUERID, --证券发行人ID
     RATINGDURATIONTYPE, --评级期限类型
     SECUISSUERATING, --证券发行等级
     SECURESIDUALM, --证券剩余期限
     SECUREVAFREQUENCY, --证券重估频率
     CCPTRANFLAG, --是否中央交易对手相关交易
     CCPID, --中央交易对手ID
     QUALCCPFLAG, --是否合格中央交易对手
     BANKROLE, --银行角色
     CLEARINGMETHOD, --清算方式
     BANKASSETFLAG, --是否银行提交资产
     MATCHCONDITIONS, --符合条件情况
     SFTFLAG, --证券融资交易标识
     MASTERNETAGREEFLAG, --净额结算主协议标识
     MASTERNETAGREEID, --净额结算主协议ID
     SFTTYPE, --证券融资交易类型
     SECUOWNERTRANSFLAG, --证券所有权是否转移
     OTCFLAG, --场外衍生工具标识
     VALIDNETTINGFLAG, --有效净额结算协议标识
     VALIDNETAGREEMENTID, --有效净额结算协议ID
     OTCTYPE, --场外衍生工具类型
     DEPOSITRISKPERIOD, --保证金风险期间
     MTM, --重置成本
     MTMCURRENCY, --重置成本币种
     BUYERORSELLER, --买方卖方
     QUALROFLAG, --合格参照资产标识
     ROISSUERPERFORMFLAG, --参照资产发行人是否能履约
     BUYERINSOLVENCYFLAG, --信用保护买方是否破产
     NONPAYMENTFEES, --尚未支付费用
     RETAILEXPOFLAG, --零售暴露标识
     RETAILCLAIMTYPE, --零售债权类型
     MORTGAGETYPE, --住房抵押贷款类型
     EXPONUMBER, --风险暴露个数
     LTV, --贷款价值比
     AGING, --账龄
     NEWDEFAULTDEBTFLAG, --新增违约债项标识
     PDPOOLMODELID, --PD分池模型ID
     LGDPOOLMODELID, --LGD分池模型ID
     CCFPOOLMODELID, --CCF分池模型ID
     PDPOOLID, --所属PD池ID
     LGDPOOLID, --所属LGD池ID
     CCFPOOLID, --所属CCF池ID
     ABSUAFLAG, --资产证券化基础资产标识
     ABSPOOLID, --证券化资产池ID
     GROUPID, --分组编号
     DEFAULTDATE, --违约时点
     ABSPROPORTION, --资产证券化比重
     DEBTORNUMBER --借款人个数
     )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --01数据日期
       p_data_dt_str, --02数据流水号
       DEALNO || T1.SEQ || T1.PS, --风险暴露ID
       DEALNO || T1.SEQ || T1.PS, --债项ID
       'YSP', --源系统ID
       DEALNO || T1.SEQ || T1.PS, --合同ID
       'OPI'|| TRIM(T1.CUST), --参与主体ID
       '6001', --源机构ID
       '重庆银行股份有限公司国际业务部', --源机构名称
       '1290', --所属机构排序号
       '6001', --所属机构ID
       '重庆银行股份有限公司国际业务部', --所属机构名称
       '6001', --账务机构ID
       '重庆银行股份有限公司国际业务部', --账务机构名称
       'J6621', --所属行业代码
       '商业银行服务', --所属行业名称
       '0102', --业务条线
       '223', --资产大类
       '22301', --资产小类
        CASE  WHEN  SUBSTR(T1.COST,6,1)=1
         THEN '404000003'
           WHEN  SUBSTR(T1.COST,6,1)=2
         THEN '404000004'
           WHEN  SUBSTR(T1.COST,6,1)=3
         THEN '404000005'
        END, --业务品种代码
       CASE  WHEN  SUBSTR(T1.COST,6,1)=1
         THEN '外汇即期'
           WHEN  SUBSTR(T1.COST,6,1)=2
         THEN '外汇远期'
           WHEN  SUBSTR(T1.COST,6,1)=3
         THEN '外汇掉期'
        END, --业务品种名称
       '01', --信用风险数据类型
       '01', --折扣系数对应资产类别
       '01', --权重法业务类型
       '', --权重法暴露大类
       '', --权重法暴露小类
       '', --内评法暴露大类
       '', --内评法暴露小类
       '02', --暴露所属标识
       CASE
         WHEN SUBSTR(COST, 1， 4) = 'E' THEN
          '01'
         ELSE
          '02'
       END, --账户类别
       '02', --监管交易类型
       '0', --回购交易标识
       '1', --重估频率
       CASE
         WHEN T1.PS = 'P' THEN T1.CTRCCY  --当支付方式为P时  是我行买入  到期卖出计卖出端风险
         WHEN T1.PS = 'S' THEN T1.CCY     --当支付方式为S时  是我行卖出  到期买入计卖出端风险
         ELSE T1.CCY
       END , --币种
       CASE
         WHEN T1.PS = 'P' THEN ABS(T1.CTRAMT)  --当支付方式为P时  是我行买入  到期卖出计卖出端风险
         WHEN T1.PS = 'S' THEN ABS(T1.CCYAMT)     --当支付方式为S时  是我行卖出  到期买入计卖出端风险
         ELSE ABS(T1.CCYAMT)
       END, --正常本金余额
       '0', --逾期余额
       '0', --非应计余额
       CASE
         WHEN T1.PS = 'P' THEN ABS(T1.CTRAMT)  --当支付方式为P时  是我行买入  到期卖出计卖出端风险
         WHEN T1.PS = 'S' THEN ABS(T1.CCYAMT)     --当支付方式为S时  是我行卖出  到期买入计卖出端风险
         ELSE ABS(T1.CCYAMT)
       END, --表内余额
       '0', --正常利息
       '0', --表内欠息
       '0', --表外欠息
       '0', --应收费用
       CASE
         WHEN T1.PS = 'P' THEN ABS(T1.CTRAMT)  --当支付方式为P时  是我行买入  到期卖出计卖出端风险
         WHEN T1.PS = 'S' THEN ABS(T1.CCYAMT)     --当支付方式为S时  是我行卖出  到期买入计卖出端风险
         ELSE ABS(T1.CCYAMT)
       END, --资产余额 
       '70270400', --科目一 应收掉期结售汇-套保应收掉期结售汇
       '', --科目二
       '', --科目三
       T1.DEALDATE, --起始日期
       T1.VDATE, --到期日期
       CASE
         WHEN (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
              TO_DATE(T1.DEALDATE, 'YYYY-MM-DD')) / 365 < 0 THEN
          0
         ELSE
          (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
          TO_DATE(T1.DEALDATE, 'YYYY-MM-DD')) / 365
       END, --原始期限 
       CASE
         WHEN (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
          0
         ELSE
          (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
          TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
       END, --剩余期限
       '01', --风险分类
       '01', --风险暴露状态
       '0', --逾期天数
       '0', --专项准备金
       '0', --一般准备金
       '0', --特别准备金
       '0', --已核销金额
       '', --表外暴露来源
       '', --表外业务类型
       '', --权重法表外业务类型细分
       '0', --是否可随时无条件撤销
       '', --信用转换系数级别
       '', --高级法信用转换系数
       '01', --债权级别
       '0', --是否为债券
       '', --债券发行目的
       '0', --是否非自用不动产
       '', --抵债资产期限类型
       '0', --是否依赖于银行未来盈利
       '', --内部评级
       '', --违约概率
       '', --违约损失率级别
       '', --高级法违约损失率
       '', --高级法有效期限
       '', --高级法违约风险暴露
       '', --违约标识
       '0.45', --已违约暴露预期损失比率
       '0.45', --已违约暴露违约损失率
       '0', --股权暴露标识
       '', --股权投资对象类型
       '', --股权投资形成原因
       '0', --专业贷款标识
       '', --专业贷款类型
       '', --项目融资阶段
       '', --监管评级
       '0', --银监会认定评级是否更为审慎
       '0', --是否波动性较大
       '0', --是否清算过程中风险暴露
       '0', --是否货款对付模式
       '0', --延迟交易天数
       '0', --有价证券标识
       '', --证券发行人ID
       '', --评级期限类型
       '', --证券发行等级
       '', --证券剩余期限
       '', --证券重估频率
       '0', --是否中央交易对手相关交易
       '', --中央交易对手ID
       '0', --是否合格中央交易对手
       '', --银行角色
       '', --清算方式
       '0', --是否银行提交资产
       '', --符合条件情况
       '0', --证券融资交易标识
       '0', --净额结算主协议标识
       '', --净额结算主协议ID
       '', --证券融资交易类型
       '0', --证券所有权是否转移
       '1', --场外衍生工具标识
       '0', --有效净额结算协议标识
       '', --有效净额结算协议ID
       '02', --场外衍生工具类型
       '', --保证金风险期间
       T1.CCYNPVAMT + T1.CTRNPVAMT, --重置成本= MAX(盯市价值, 0)
       CASE
         WHEN T1.PS = 'P' THEN T1.CTRCCY  --当支付方式为P时  是我行买入  到期卖出计卖出端风险
         WHEN T1.PS = 'S' THEN T1.CCY     --当支付方式为S时  是我行卖出  到期买入计卖出端风险
         ELSE T1.CCY
       END, --重置成本币种
       '', --买方卖方
       '0', --合格参照资产标识
       '0', --参照资产发行人是否能履约
       '0', --信用保护买方是否破产
       '0', --尚未支付费用
       '0', --零售暴露标识
       '', --零售债权类型
       '', --住房抵押贷款类型
       '1', --风险暴露个数
       '0.8', --贷款价值比
       '', --账龄
       '', --新增违约债项标识
       '', --PD分池模型ID
       '', --LGD分池模型ID
       '', --CCF分池模型ID
       '', --所属PD池ID
       '', --所属LGD池ID
       '', --所属CCF池ID
       '0', --资产证券化基础资产标识
       '', --证券化资产池ID
       '', --分组编号
       '', -- BREAKDATE, --违约时点 
       '', --资产证券化比重
       '' --借款人个数
  FROM OPI_FXDH T1
  LEFT JOIN OPI_CUST T2
    ON T1.DATANO = T2.DATANO
   AND T1.CUST = T2.CNO
   AND T1.DATANO = p_data_dt_str              
  WHERE T1.DATANO = p_data_dt_str
   AND T1.VDATE >= p_data_dt_str     
   AND SUBSTR(T1.COST, 1, 1) = '2' --第一位=2  --数据为外汇
   AND SUBSTR(T1.COST, 6, 1)='2' --第六位=2  --远期
   AND T1.VERIND = 1
   AND TRIM(T1.REVDATE) IS NULL
   ;
   
   COMMIT;
   
  

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_YSP_EXPOSURE',
                                CASCADE => TRUE);

  /*目标表数据统计*/
  --统计插入的记录数
  SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_YSP_EXPOSURE;

  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT表当前插入的财务系统-应收款投资数据记录为: ' || (v_count1 - v_count) || ' 条');
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  P_PO_RTNCODE := '1';
  P_PO_RTNMSG  := '成功' || '-' || V_COUNT;
  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '合同信息(' || V_PRO_NAME || ')ETL转换失败！' || SQLERRM ||
                    ';错误行数为:' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    RETURN;
END PRO_RWA_YSP_EXPOSURE;
/

