CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_EXPOSURE(p_data_dt_str IN VARCHAR2, --数据日期
                                                p_po_rtncode  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                                p_po_rtnmsg   OUT VARCHAR2 --返回描述
                                                )
/*
  存储过程名称:RWA_DEV.PRO_RWA_EI_EXPOSURE
  实现功能:汇总风险暴露表,插入所有风险暴露表信息
  数据口径:全量
  跑批频率:月初
  版  本  :V1.0.0
  编写人  :SHUXD
  编写时间:2016-06-01
  单  位  :上海安硕信息技术股份有限公司
  源  表1 :RWA_DEV.RWA_XD_EXPOSURE|信贷风险暴露表
  源  表2 :RWA_DEV.RWA_HG_EXPOSURE|回购风险暴露表
  源  表3 :RWA_DEV.RWA_LC_EXPOSURE|理财风险暴露表
  源  表4 :RWA_DEV.RWA_PJ_EXPOSURE|票据风险暴露表
  源  表5 :RWA_DEV.RWA_TY_EXPOSURE|同业风险暴露表
  源  表6 :RWA_DEV.RWA_TZ_EXPOSURE|投资风险暴露表
  源  表7 :RWA_DEV.RWA_XYK_EXPOSURE|信用卡风险暴露表
  源  表8 :RWA_DEV.RWA_GQ_EXPOSURE|股权风险暴露表
  源  表9 :RWA_DEV.RWA_ABS_ISSURE_EXPOSURE|资产证券化发行机构风险暴露表
  源  表10:RWA_DEV.RWA_DZ_EXPOSURE|抵债资产风险暴露表
  源  表11:RWA_DEV.RWA_ZX_EXPOSURE|直销银行风险暴露表
  源  表12:RWA_DEV.RWA_YSP_EXPOSURE|衍生品业务信用风险暴露表
  
  目标表  :RWA_DEV.RWA_EI_EXPOSURE|风险暴露汇总表
  辅助表  :无
  变更记录(修改人|修改时间|修改内容)：
  
  pxl  2019/05/08 增加资金系统衍生品业务暴露信息到计算引擎接口
  pxl  2019/05/29 增加消费金融个人业务暴露信息到计算引擎接口
  
  */
 AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_EXPOSURE';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

BEGIN
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_EXPOSURE DROP PARTITION EXPOSURE' ||
                      p_data_dt_str;
  
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      IF (SQLCODE <> '-2149') THEN
        --首次分区truncate会出现2149异常
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '汇总信用风险暴露表(' || v_pro_name || ')ETL转换失败！' ||
                        sqlerrm || ';错误行数为:' ||
                        dbms_utility.format_error_backtrace;
        RETURN;
      END IF;
  END;

  --新增一个当前日期下的分区
  EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_EXPOSURE ADD PARTITION EXPOSURE' ||
                    p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str ||
                    ',''YYYYMMDD''))';

  COMMIT;

  /*插入信贷的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE AS DATADATE -- 数据日期
          ,
           DATANO AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID AS DUEID -- 债项ID
          ,
           SSYSID AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID AS CLIENTID -- 参与主体ID
          ,
           SORGID AS SORGID -- 源机构ID
          ,
           SORGNAME AS SORGNAME -- 源机构名称
          ,
           ORGID AS ORGID -- 所属机构ID
          ,
           ORGNAME AS ORGNAME -- 所属机构名称
          ,
           ACCORGID AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           CASE
             WHEN CREDITRISKDATATYPE <> '02' AND INDUSTRYID IS NULL THEN
              'J66'
             ELSE
              INDUSTRYID
           END AS INDUSTRYID --所属行业代码
          ,
           CASE
             WHEN CREDITRISKDATATYPE <> '02' AND INDUSTRYNAME IS NULL THEN
              '货币金融服务'
             ELSE
              NVL(INDUSTRYNAME,'未知' ） END AS INDUSTRYNAME --所属行业名称
          , BUSINESSLINE AS BUSINESSLINE -- 条线
          , ASSETTYPE AS ASSETTYPE -- 资产大类
          , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
          , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
          , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
          , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
          , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
          , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
          , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
          , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
          , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
          , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
          , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
          , BOOKTYPE AS BOOKTYPE -- 账户类别
          , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
          , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
          , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
          , CURRENCY AS CURRENCY -- 币种
          , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
          , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
          , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
          , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
          , NORMALINTEREST AS NORMALINTEREST -- 正常利息
          , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
          , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
          , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
          , ASSETBALANCE AS ASSETBALANCE -- 资产余额
          , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
          , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
          , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
          , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'), 'YYYY-MM-DD') AS STARTDATE -- 起始日期
          , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'), 'YYYY-MM-DD') AS DUEDATE -- 到期日期
          , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
          , RESIDUALM AS RESIDUALM -- 剩余期限
          , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
          , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
          , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
          , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
          , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
          , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
          , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
          , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
          , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
          , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
          , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
          , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
          , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
          , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
          , BONDFLAG AS BONDFLAG -- 是否为债券
          , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
          , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
          , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
          , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
          , IRATING AS IRATING -- 内部评级
          , PD AS PD -- 违约概率
          , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
          , LGDAIRB AS LGDAIRB -- 高级法违约损失率
          , MAIRB AS MAIRB -- 高级法有效期限
          , EADAIRB AS EADAIRB -- 高级法违约风险暴露
          , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
          , BEEL AS BEEL -- 已违约暴露预期损失比率
          , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
          , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
          , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
          , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
          , SLFLAG AS SLFLAG -- 专业贷款标识
          , SLTYPE AS SLTYPE -- 专业贷款类型
          , PFPHASE AS PFPHASE -- 项目融资阶段
          , REGURATING AS REGURATING -- 监管评级
          , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
          , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
          , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
          , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
          , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
          , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
          , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
          , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
          , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
          , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
          , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
          , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
          , CCPID AS CCPID -- 中央交易对手ID
          , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
          , BANKROLE AS BANKROLE -- 银行角色
          , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
          , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
          , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
          , SFTFLAG AS SFTFLAG -- 证券融资交易标识
          , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
          , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
          , SFTTYPE AS SFTTYPE -- 证券融资交易类型
          , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
          , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
          , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
          , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
          , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
          , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
          , MTM AS MTM -- 重置成本
          , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
          , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
          , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
          , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
          , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
          , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
          , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
          , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
          , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
          , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
          , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
          , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
          , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
          , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
          , PDPOOLID AS PDPOOLID -- 所属PD池ID
          , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
          , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
          , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
          , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
          , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
          , GROUPID AS GROUPID -- 分组编号
          , ORGSORTNO AS ORGSORTNO --所属机构排序号
          , LTV AS LTV --贷款价值比
          , AGING AS AGING --账龄
          , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
          , DEFAULTDATE AS DEFAULTDATE --违约时点
           FROM RWA_DEV.RWA_XD_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') and accsubject1 <> '70230101' ---20191014 by wzb
           AND ACCSUBJECT1 NOT LIKE '@%' --20191123 BY YSJ
           AND ASSETBALANCE > 0 ----20191024 BY WZB
           ;
  COMMIT;

  /*插入回购的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_HG_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入理财的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
           --,decode(accsubject1,'15030201',nvl(SBJT_VAL4,'0')+NORMALPRINCIPAL,NORMALPRINCIPAL)           AS NORMALPRINCIPAL         -- 正常本金余额
                  , NORMALPRINCIPAL, OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
           --,decode(accsubject1,'15030201',nvl(SBJT_VAL4,'0')+ASSETBALANCE,ASSETBALANCE)                                             AS ASSETBALANCE            -- 资产余额
                  , NORMALPRINCIPAL, ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_LC_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入票据的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_PJ_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入同业的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_TY_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入信用卡的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_XYK_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入投资的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  ,CASE ACCSUBJECT1
             WHEN '11010101' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) + NVL(SBJT_VAL5, 0) --本金+公允价值变动+应计利息+应收利息
             WHEN '11010301' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010302', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --本金+应计利息+公允价值变动
             WHEN '15010101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15010103', NVL(SBJT_VAL3, 0), 0) --本金+利息调整+应计利息
             WHEN '15030101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15030103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --本金+利息调整+应计利息+公允价值变动
             ELSE
              NORMALPRINCIPAL
           END        AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  ,CASE ACCSUBJECT1
             WHEN '11010101' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) + NVL(SBJT_VAL5, 0) --本金+公允价值变动+应计利息+应收利息
             WHEN '11010301' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010302', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --本金+应计利息+公允价值变动
             WHEN '15010101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15010103', NVL(SBJT_VAL3, 0), 0) --本金+利息调整+应计利息
             WHEN '15030101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15030103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --本金+利息调整+应计利息+公允价值变动
             ELSE
              NORMALPRINCIPAL
           END    AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  ,CASE ACCSUBJECT1
             WHEN '11010101' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) + NVL(SBJT_VAL5, 0) --本金+公允价值变动+应计利息+应收利息
             WHEN '11010301' THEN
              NORMALPRINCIPAL +
              DECODE(SBJT3, '11010302', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --本金+应计利息+公允价值变动
             WHEN '15010101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15010103', NVL(SBJT_VAL3, 0), 0) --本金+利息调整+应计利息
             WHEN '15030101' THEN
              NORMALPRINCIPAL + NVL(SBJT_VAL2, 0) +
              DECODE(SBJT3, '15030103', NVL(SBJT_VAL3, 0), 0) +
              NVL(SBJT_VAL4, 0) --本金+利息调整+应计利息+公允价值变动
             ELSE
              ASSETBALANCE
           END        AS ASSETBALANCE -- 资产余额 BY WZB 20190910 加上公允价值变动 利息调整，应记应收
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_TZ_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       ) AND (NVL(ACCSUBJECT1, '00000000'       ) <> '11010101'        or EXPOSUBCLASSSTD = '010407'       ) and exposureid not in('B201803296435',
                                                                                                                                                                                                                                     'B201712285095')       ; ---20190910 by wzb 人民币债券11010101科目是交易性金融资产（只计市场风险）
  COMMIT;

  /*插入股权的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_GQ_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入抵债资产的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_DZ_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入直销银行的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_ZX_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入资产证券化发行机构的风险暴露信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_ABS_ISSURE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入衍生品业务信用风险暴露表信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_YSP_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  /*插入消费金融业务信用风险暴露表信息*/
  INSERT /*+ APPEND */
  INTO RWA_DEV.RWA_EI_EXPOSURE
    (DATADATE -- 数据日期
    ,
     DATANO -- 数据流水号
    ,
     EXPOSUREID -- 风险暴露ID
    ,
     DUEID -- 债项ID
    ,
     SSYSID -- 源系统ID
    ,
     CONTRACTID -- 合同ID
    ,
     CLIENTID -- 参与主体ID
    ,
     SORGID -- 源机构ID
    ,
     SORGNAME -- 源机构名称
    ,
     ORGID -- 所属机构ID
    ,
     ORGNAME -- 所属机构名称
    ,
     ACCORGID -- 账务机构ID
    ,
     ACCORGNAME -- 账务机构名称
    ,
     INDUSTRYID -- 所属行业代码
    ,
     INDUSTRYNAME -- 所属行业名称
    ,
     BUSINESSLINE -- 条线
    ,
     ASSETTYPE -- 资产大类
    ,
     ASSETSUBTYPE -- 资产小类
    ,
     BUSINESSTYPEID -- 业务品种代码
    ,
     BUSINESSTYPENAME -- 业务品种名称
    ,
     CREDITRISKDATATYPE -- 信用风险数据类型
    ,
     ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
    ,
     BUSINESSTYPESTD -- 权重法业务类型
    ,
     EXPOCLASSSTD -- 权重法暴露大类
    ,
     EXPOSUBCLASSSTD -- 权重法暴露小类
    ,
     EXPOCLASSIRB -- 内评法暴露大类
    ,
     EXPOSUBCLASSIRB -- 内评法暴露小类
    ,
     EXPOBELONG -- 暴露所属标识
    ,
     BOOKTYPE -- 账户类别
    ,
     REGUTRANTYPE -- 监管交易类型
    ,
     REPOTRANFLAG -- 回购交易标识
    ,
     REVAFREQUENCY -- 重估频率
    ,
     CURRENCY -- 币种
    ,
     NORMALPRINCIPAL -- 正常本金余额
    ,
     OVERDUEBALANCE -- 逾期余额
    ,
     NONACCRUALBALANCE -- 非应计余额
    ,
     ONSHEETBALANCE -- 表内余额
    ,
     NORMALINTEREST -- 正常利息
    ,
     ONDEBITINTEREST -- 表内欠息
    ,
     OFFDEBITINTEREST -- 表外欠息
    ,
     EXPENSERECEIVABLE -- 应收费用
    ,
     ASSETBALANCE -- 资产余额
    ,
     ACCSUBJECT1 -- 科目一
    ,
     ACCSUBJECT2 -- 科目二
    ,
     ACCSUBJECT3 -- 科目三
    ,
     STARTDATE -- 起始日期
    ,
     DUEDATE -- 到期日期
    ,
     ORIGINALMATURITY -- 原始期限
    ,
     RESIDUALM -- 剩余期限
    ,
     RISKCLASSIFY -- 风险分类
    ,
     EXPOSURESTATUS -- 风险暴露状态
    ,
     OVERDUEDAYS -- 逾期天数
    ,
     SPECIALPROVISION -- 专项准备金
    ,
     GENERALPROVISION -- 一般准备金
    ,
     ESPECIALPROVISION -- 特别准备金
    ,
     WRITTENOFFAMOUNT -- 已核销金额
    ,
     OFFEXPOSOURCE -- 表外暴露来源
    ,
     OFFBUSINESSTYPE -- 表外业务类型
    ,
     OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
    ,
     UNCONDCANCELFLAG -- 是否可随时无条件撤销
    ,
     CCFLEVEL -- 信用转换系数级别
    ,
     CCFAIRB -- 高级法信用转换系数
    ,
     CLAIMSLEVEL -- 债权级别
    ,
     BONDFLAG -- 是否为债券
    ,
     BONDISSUEINTENT -- 债券发行目的
    ,
     NSUREALPROPERTYFLAG -- 是否非自用不动产
    ,
     REPASSETTERMTYPE -- 抵债资产期限类型
    ,
     DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
    ,
     IRATING -- 内部评级
    ,
     PD -- 违约概率
    ,
     LGDLEVEL -- 违约损失率级别
    ,
     LGDAIRB -- 高级法违约损失率
    ,
     MAIRB -- 高级法有效期限
    ,
     EADAIRB -- 高级法违约风险暴露
    ,
     DEFAULTFLAG -- 违约标识
    ,
     BEEL -- 已违约暴露预期损失比率
    ,
     DEFAULTLGD -- 已违约暴露违约损失率
    ,
     EQUITYEXPOFLAG -- 股权暴露标识
    ,
     EQUITYINVESTTYPE -- 股权投资对象类型
    ,
     EQUITYINVESTCAUSE -- 股权投资形成原因
    ,
     SLFLAG -- 专业贷款标识
    ,
     SLTYPE -- 专业贷款类型
    ,
     PFPHASE -- 项目融资阶段
    ,
     REGURATING -- 监管评级
    ,
     CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
    ,
     LARGEFLUCFLAG -- 是否波动性较大
    ,
     LIQUEXPOFLAG -- 是否清算过程中风险暴露
    ,
     PAYMENTDEALFLAG -- 是否货款对付模式
    ,
     DELAYTRADINGDAYS -- 延迟交易天数
    ,
     SECURITIESFLAG -- 有价证券标识
    ,
     SECUISSUERID -- 证券发行人ID
    ,
     RATINGDURATIONTYPE -- 评级期限类型
    ,
     SECUISSUERATING -- 证券发行等级
    ,
     SECURESIDUALM -- 证券剩余期限
    ,
     SECUREVAFREQUENCY -- 证券重估频率
    ,
     CCPTRANFLAG -- 是否中央交易对手相关交易
    ,
     CCPID -- 中央交易对手ID
    ,
     QUALCCPFLAG -- 是否合格中央交易对手
    ,
     BANKROLE -- 银行角色
    ,
     CLEARINGMETHOD -- 清算方式
    ,
     BANKASSETFLAG -- 是否银行提交资产
    ,
     MATCHCONDITIONS -- 符合条件情况
    ,
     SFTFLAG -- 证券融资交易标识
    ,
     MASTERNETAGREEFLAG -- 净额结算主协议标识
    ,
     MASTERNETAGREEID -- 净额结算主协议ID
    ,
     SFTTYPE -- 证券融资交易类型
    ,
     SECUOWNERTRANSFLAG -- 证券所有权是否转移
    ,
     OTCFLAG -- 场外衍生工具标识
    ,
     VALIDNETTINGFLAG -- 有效净额结算协议标识
    ,
     VALIDNETAGREEMENTID -- 有效净额结算协议ID
    ,
     OTCTYPE -- 场外衍生工具类型
    ,
     DEPOSITRISKPERIOD -- 保证金风险期间
    ,
     MTM -- 重置成本
    ,
     MTMCURRENCY -- 重置成本币种
    ,
     BUYERORSELLER -- 买方卖方
    ,
     QUALROFLAG -- 合格参照资产标识
    ,
     ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
    ,
     BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
    ,
     NONPAYMENTFEES -- 尚未支付费用
    ,
     RETAILEXPOFLAG -- 零售暴露标识
    ,
     RETAILCLAIMTYPE -- 零售债权类型
    ,
     MORTGAGETYPE -- 住房抵押贷款类型
    ,
     DEBTORNUMBER -- 借款人个数
    ,
     EXPONUMBER -- 风险暴露个数
    ,
     PDPOOLMODELID -- PD分池模型ID
    ,
     LGDPOOLMODELID -- LGD分池模型ID
    ,
     CCFPOOLMODELID -- CCF分池模型ID
    ,
     PDPOOLID -- 所属PD池ID
    ,
     LGDPOOLID -- 所属LGD池ID
    ,
     CCFPOOLID -- 所属CCF池ID
    ,
     ABSUAFLAG -- 资产证券化基础资产标识
    ,
     ABSPOOLID -- 证券化资产池ID
    ,
     ABSPROPORTION -- 资产证券化比重
    ,
     GROUPID -- 分组编号
    ,
     ORGSORTNO --所属机构排序号
    ,
     LTV --贷款价值比
    ,
     AGING --账龄
    ,
     NEWDEFAULTDEBTFLAG --新增违约债项标识
    ,
     DEFAULTDATE --违约时点
     )
    SELECT DATADATE   AS DATADATE -- 数据日期
          ,
           DATANO     AS DATANO -- 数据流水号
          ,
           EXPOSUREID AS EXPOSUREID -- 风险暴露ID
          ,
           DUEID      AS DUEID -- 债项ID
          ,
           SSYSID     AS SSYSID -- 源系统ID
          ,
           CONTRACTID AS CONTRACTID -- 合同ID
          ,
           CLIENTID   AS CLIENTID -- 参与主体ID
          ,
           SORGID     AS SORGID -- 源机构ID
          ,
           SORGNAME   AS SORGNAME -- 源机构名称
          ,
           ORGID      AS ORGID -- 所属机构ID
          ,
           ORGNAME    AS ORGNAME -- 所属机构名称
          ,
           ACCORGID   AS ACCORGID -- 账务机构ID
          ,
           ACCORGNAME AS ACCORGNAME -- 账务机构名称
          ,
           INDUSTRYID AS INDUSTRYID --所属行业代码
          ,
           NVL       (INDUSTRYNAME, '未知'        ） AS INDUSTRYNAME --所属行业名称
                  , BUSINESSLINE AS BUSINESSLINE -- 条线
                  , ASSETTYPE AS ASSETTYPE -- 资产大类
                  , ASSETSUBTYPE AS ASSETSUBTYPE -- 资产小类
                  , BUSINESSTYPEID AS BUSINESSTYPEID -- 业务品种代码
                  , BUSINESSTYPENAME AS BUSINESSTYPENAME -- 业务品种名称
                  , CREDITRISKDATATYPE AS CREDITRISKDATATYPE -- 信用风险数据类型
                  , ASSETTYPEOFHAIRCUTS AS ASSETTYPEOFHAIRCUTS -- 折扣系数对应资产类别
                  , BUSINESSTYPESTD AS BUSINESSTYPESTD -- 权重法业务类型
                  , EXPOCLASSSTD AS EXPOCLASSSTD -- 权重法暴露大类
                  , EXPOSUBCLASSSTD AS EXPOSUBCLASSSTD -- 权重法暴露小类
                  , EXPOCLASSIRB AS EXPOCLASSIRB -- 内评法暴露大类
                  , EXPOSUBCLASSIRB AS EXPOSUBCLASSIRB -- 内评法暴露小类
                  , EXPOBELONG AS EXPOBELONG -- 暴露所属标识
                  , BOOKTYPE AS BOOKTYPE -- 账户类别
                  , REGUTRANTYPE AS REGUTRANTYPE -- 监管交易类型
                  , REPOTRANFLAG AS REPOTRANFLAG -- 回购交易标识
                  , REVAFREQUENCY AS REVAFREQUENCY -- 重估频率
                  , CURRENCY AS CURRENCY -- 币种
                  , NORMALPRINCIPAL AS NORMALPRINCIPAL -- 正常本金余额
                  , OVERDUEBALANCE AS OVERDUEBALANCE -- 逾期余额
                  , NONACCRUALBALANCE AS NONACCRUALBALANCE -- 非应计余额
                  , ONSHEETBALANCE AS ONSHEETBALANCE -- 表内余额
                  , NORMALINTEREST AS NORMALINTEREST -- 正常利息
                  , ONDEBITINTEREST AS ONDEBITINTEREST -- 表内欠息
                  , OFFDEBITINTEREST AS OFFDEBITINTEREST -- 表外欠息
                  , EXPENSERECEIVABLE AS EXPENSERECEIVABLE -- 应收费用
                  , ASSETBALANCE AS ASSETBALANCE -- 资产余额
                  , ACCSUBJECT1 AS ACCSUBJECT1 -- 科目一
                  , ACCSUBJECT2 AS ACCSUBJECT2 -- 科目二
                  , ACCSUBJECT3 AS ACCSUBJECT3 -- 科目三
                  , TO_CHAR(TO_DATE(STARTDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS STARTDATE -- 起始日期
                  , TO_CHAR(TO_DATE(DUEDATE, 'YYYY-MM-DD'       ), 'YYYY-MM-DD'       ) AS DUEDATE -- 到期日期
                  , ORIGINALMATURITY AS ORIGINALMATURITY -- 原始期限
                  , RESIDUALM AS RESIDUALM -- 剩余期限
                  , RISKCLASSIFY AS RISKCLASSIFY -- 风险分类
                  , EXPOSURESTATUS AS EXPOSURESTATUS -- 风险暴露状态
                  , OVERDUEDAYS AS OVERDUEDAYS -- 逾期天数
                  , SPECIALPROVISION AS SPECIALPROVISION -- 专项准备金
                  , GENERALPROVISION AS GENERALPROVISION -- 一般准备金
                  , ESPECIALPROVISION AS ESPECIALPROVISION -- 特别准备金
                  , WRITTENOFFAMOUNT AS WRITTENOFFAMOUNT -- 已核销金额
                  , OFFEXPOSOURCE AS OFFEXPOSOURCE -- 表外暴露来源
                  , OFFBUSINESSTYPE AS OFFBUSINESSTYPE -- 表外业务类型
                  , OFFBUSINESSSDVSSTD AS OFFBUSINESSSDVSSTD -- 权重法表外业务类型细分
                  , UNCONDCANCELFLAG AS UNCONDCANCELFLAG -- 是否可随时无条件撤销
                  , CCFLEVEL AS CCFLEVEL -- 信用转换系数级别
                  , CCFAIRB AS CCFAIRB -- 高级法信用转换系数
                  , CLAIMSLEVEL AS CLAIMSLEVEL -- 债权级别
                  , BONDFLAG AS BONDFLAG -- 是否为债券
                  , BONDISSUEINTENT AS BONDISSUEINTENT -- 债券发行目的
                  , NSUREALPROPERTYFLAG AS NSUREALPROPERTYFLAG -- 是否非自用不动产
                  , REPASSETTERMTYPE AS REPASSETTERMTYPE -- 抵债资产期限类型
                  , DEPENDONFPOBFLAG AS DEPENDONFPOBFLAG -- 是否依赖于银行未来盈利
                  , IRATING AS IRATING -- 内部评级
                  , PD AS PD -- 违约概率
                  , LGDLEVEL AS LGDLEVEL -- 违约损失率级别
                  , LGDAIRB AS LGDAIRB -- 高级法违约损失率
                  , MAIRB AS MAIRB -- 高级法有效期限
                  , EADAIRB AS EADAIRB -- 高级法违约风险暴露
                  , DEFAULTFLAG AS DEFAULTFLAG -- 违约标识
                  , BEEL AS BEEL -- 已违约暴露预期损失比率
                  , DEFAULTLGD AS DEFAULTLGD -- 已违约暴露违约损失率
                  , EQUITYEXPOFLAG AS EQUITYEXPOFLAG -- 股权暴露标识
                  , EQUITYINVESTTYPE AS EQUITYINVESTTYPE -- 股权投资对象类型
                  , EQUITYINVESTCAUSE AS EQUITYINVESTCAUSE -- 股权投资形成原因
                  , SLFLAG AS SLFLAG -- 专业贷款标识
                  , SLTYPE AS SLTYPE -- 专业贷款类型
                  , PFPHASE AS PFPHASE -- 项目融资阶段
                  , REGURATING AS REGURATING -- 监管评级
                  , CBRCMPRATINGFLAG AS CBRCMPRATINGFLAG -- 银监会认定评级是否更为审慎
                  , LARGEFLUCFLAG AS LARGEFLUCFLAG -- 是否波动性较大
                  , LIQUEXPOFLAG AS LIQUEXPOFLAG -- 是否清算过程中风险暴露
                  , PAYMENTDEALFLAG AS PAYMENTDEALFLAG -- 是否货款对付模式
                  , DELAYTRADINGDAYS AS DELAYTRADINGDAYS -- 延迟交易天数
                  , SECURITIESFLAG AS SECURITIESFLAG -- 有价证券标识
                  , SECUISSUERID AS SECUISSUERID -- 证券发行人ID
                  , RATINGDURATIONTYPE AS RATINGDURATIONTYPE -- 评级期限类型
                  , SECUISSUERATING AS SECUISSUERATING -- 证券发行等级
                  , SECURESIDUALM AS SECURESIDUALM -- 证券剩余期限
                  , SECUREVAFREQUENCY AS SECUREVAFREQUENCY -- 证券重估频率
                  , CCPTRANFLAG AS CCPTRANFLAG -- 是否中央交易对手相关交易
                  , CCPID AS CCPID -- 中央交易对手ID
                  , QUALCCPFLAG AS QUALCCPFLAG -- 是否合格中央交易对手
                  , BANKROLE AS BANKROLE -- 银行角色
                  , CLEARINGMETHOD AS CLEARINGMETHOD -- 清算方式
                  , BANKASSETFLAG AS BANKASSETFLAG -- 是否银行提交资产
                  , MATCHCONDITIONS AS MATCHCONDITIONS -- 符合条件情况
                  , SFTFLAG AS SFTFLAG -- 证券融资交易标识
                  , MASTERNETAGREEFLAG AS MASTERNETAGREEFLAG -- 净额结算主协议标识
                  , MASTERNETAGREEID AS MASTERNETAGREEID -- 净额结算主协议ID
                  , SFTTYPE AS SFTTYPE -- 证券融资交易类型
                  , SECUOWNERTRANSFLAG AS SECUOWNERTRANSFLAG -- 证券所有权是否转移
                  , OTCFLAG AS OTCFLAG -- 场外衍生工具标识
                  , VALIDNETTINGFLAG AS VALIDNETTINGFLAG -- 有效净额结算协议标识
                  , VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID -- 有效净额结算协议ID
                  , OTCTYPE AS OTCTYPE -- 场外衍生工具类型
                  , DEPOSITRISKPERIOD AS DEPOSITRISKPERIOD -- 保证金风险期间
                  , MTM AS MTM -- 重置成本
                  , MTMCURRENCY AS MTMCURRENCY -- 重置成本币种
                  , BUYERORSELLER AS BUYERORSELLER -- 买方卖方
                  , QUALROFLAG AS QUALROFLAG -- 合格参照资产标识
                  , ROISSUERPERFORMFLAG AS ROISSUERPERFORMFLAG -- 参照资产发行人是否能履约
                  , BUYERINSOLVENCYFLAG AS BUYERINSOLVENCYFLAG -- 信用保护买方是否破产
                  , NONPAYMENTFEES AS NONPAYMENTFEES -- 尚未支付费用
                  , RETAILEXPOFLAG AS RETAILEXPOFLAG -- 零售暴露标识
                  , RETAILCLAIMTYPE AS RETAILCLAIMTYPE -- 零售债权类型
                  , MORTGAGETYPE AS MORTGAGETYPE -- 住房抵押贷款类型
                  , DEBTORNUMBER AS DEBTORNUMBER -- 借款人个数
                  , EXPONUMBER AS EXPONUMBER -- 风险暴露个数
                  , PDPOOLMODELID AS PDPOOLMODELID -- PD分池模型ID
                  , LGDPOOLMODELID AS LGDPOOLMODELID -- LGD分池模型ID
                  , CCFPOOLMODELID AS CCFPOOLMODELID -- CCF分池模型ID
                  , PDPOOLID AS PDPOOLID -- 所属PD池ID
                  , LGDPOOLID AS LGDPOOLID -- 所属LGD池ID
                  , CCFPOOLID AS CCFPOOLID -- 所属CCF池ID
                  , ABSUAFLAG AS ABSUAFLAG -- 资产证券化基础资产标识
                  , ABSPOOLID AS ABSPOOLID -- 证券化资产池ID
                  , ABSPROPORTION AS ABSPROPORTION -- 资产证券化比重
                  , GROUPID AS GROUPID -- 分组编号
                  , ORGSORTNO AS ORGSORTNO --所属机构排序号
                  , LTV AS LTV --贷款价值比
                  , AGING AS AGING --账龄
                  , NEWDEFAULTDEBTFLAG AS NEWDEFAULTDEBTFLAG --新增违约债项标识
                  , DEFAULTDATE AS DEFAULTDATE --违约时点
                   FROM RWA_DEV.RWA_XF_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'       );
  COMMIT;

  --关联减值信息
  /*UPDATE RWA_EI_EXPOSURE T1
     SET T1.GENERALPROVISION =
         (SELECT FINAL_ECL
            FROM SYS_IFRS9_RESULT T2
           WHERE T1.DATANO = T2.DATANO
             AND DECODE(T1.SSYSID, 'XYK', T1.EXPOSUREID, T1.DUEID) =
                 DECODE(T2.ITEM_CODE,
                        '信用卡表外',
                        'BW_' || T2.CONTRACT_REFERENCE,
                        T2.CONTRACT_REFERENCE))
   WHERE T1.DATANO = p_data_dt_str
     AND EXISTS (SELECT 1
            FROM SYS_IFRS9_RESULT I
           WHERE I.DATANO = T1.DATANO
             AND I.CONTRACT_REFERENCE = T1.DUEID);
  COMMIT;*/

  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_EI_EXPOSURE_TMP';
  INSERT /*+ APPEND */
  INTO RWA_EI_EXPOSURE_TMP
    SELECT T1.DATADATE,
           T1.DATANO,
           T1.EXPOSUREID,
           T1.DUEID,
           T1.SSYSID,
           T1.CONTRACTID,
           T1.CLIENTID,
           T1.SORGID,
           T1.SORGNAME,
           T1.ORGSORTNO,
           T1.ORGID,
           T1.ORGNAME,
           T1.ACCORGID,
           T1.ACCORGNAME,
           T1.INDUSTRYID,
           T1.INDUSTRYNAME,
           T1.BUSINESSLINE,
           T1.ASSETTYPE,
           T1.ASSETSUBTYPE,
           T1.BUSINESSTYPEID,
           T1.BUSINESSTYPENAME,
           T1.CREDITRISKDATATYPE,
           T1.ASSETTYPEOFHAIRCUTS,
           T1.BUSINESSTYPESTD,
           T1.EXPOCLASSSTD,
           T1.EXPOSUBCLASSSTD,
           T1.EXPOCLASSIRB,
           T1.EXPOSUBCLASSIRB,
           T1.EXPOBELONG,
           T1.BOOKTYPE,
           T1.REGUTRANTYPE,
           T1.REPOTRANFLAG,
           T1.REVAFREQUENCY,
           T1.CURRENCY,
           T1.NORMALPRINCIPAL,
           T1.OVERDUEBALANCE,
           T1.NONACCRUALBALANCE,
           T1.ONSHEETBALANCE,
           T1.NORMALINTEREST,
           T1.ONDEBITINTEREST,
           T1.OFFDEBITINTEREST,
           T1.EXPENSERECEIVABLE,
           T1.ASSETBALANCE,
           T1.ACCSUBJECT1,
           T1.ACCSUBJECT2,
           T1.ACCSUBJECT3,
           T1.STARTDATE,
           T1.DUEDATE,
           T1.ORIGINALMATURITY,
           T1.RESIDUALM,
           T1.RISKCLASSIFY,
           T1.EXPOSURESTATUS,
           T1.OVERDUEDAYS,
           T1.SPECIALPROVISION,
           CASE
             when ssysid = 'BL' or accsubject1 like '1503%' OR accsubject1 like '1301%'   --这两种资产都不计减值，经宋科确认 modify by YSJ
               THEN
                 NVL(T1.GENERALPROVISION, 0)
               ELSE
                 NVL(T2.FINAL_ECL, 0)
           END, --一般准备金 
           T1.ESPECIALPROVISION,
           T1.WRITTENOFFAMOUNT,
           T1.OFFEXPOSOURCE,
           T1.OFFBUSINESSTYPE,
           T1.OFFBUSINESSSDVSSTD,
           T1.UNCONDCANCELFLAG,
           T1.CCFLEVEL,
           T1.CCFAIRB,
           T1.CLAIMSLEVEL,
           T1.BONDFLAG,
           T1.BONDISSUEINTENT,
           T1.NSUREALPROPERTYFLAG,
           T1.REPASSETTERMTYPE,
           T1.DEPENDONFPOBFLAG,
           T1.IRATING,
           T1.PD,
           T1.LGDLEVEL,
           T1.LGDAIRB,
           T1.MAIRB,
           T1.EADAIRB,
           T1.DEFAULTFLAG,
           T1.BEEL,
           T1.DEFAULTLGD,
           T1.EQUITYEXPOFLAG,
           T1.EQUITYINVESTTYPE,
           T1.EQUITYINVESTCAUSE,
           T1.SLFLAG,
           T1.SLTYPE,
           T1.PFPHASE,
           T1.REGURATING,
           T1.CBRCMPRATINGFLAG,
           T1.LARGEFLUCFLAG,
           T1.LIQUEXPOFLAG,
           T1.PAYMENTDEALFLAG,
           T1.DELAYTRADINGDAYS,
           T1.SECURITIESFLAG,
           T1.SECUISSUERID,
           T1.RATINGDURATIONTYPE,
           T1.SECUISSUERATING,
           T1.SECURESIDUALM,
           T1.SECUREVAFREQUENCY,
           T1.CCPTRANFLAG,
           T1.CCPID,
           T1.QUALCCPFLAG,
           T1.BANKROLE,
           T1.CLEARINGMETHOD,
           T1.BANKASSETFLAG,
           T1.MATCHCONDITIONS,
           T1.SFTFLAG,
           T1.MASTERNETAGREEFLAG,
           T1.MASTERNETAGREEID,
           T1.SFTTYPE,
           T1.SECUOWNERTRANSFLAG,
           T1.OTCFLAG,
           T1.VALIDNETTINGFLAG,
           T1.VALIDNETAGREEMENTID,
           T1.OTCTYPE,
           T1.DEPOSITRISKPERIOD,
           T1.MTM,
           T1.MTMCURRENCY,
           T1.BUYERORSELLER,
           T1.QUALROFLAG,
           T1.ROISSUERPERFORMFLAG,
           T1.BUYERINSOLVENCYFLAG,
           T1.NONPAYMENTFEES,
           T1.RETAILEXPOFLAG,
           T1.RETAILCLAIMTYPE,
           T1.MORTGAGETYPE,
           T1.DEBTORNUMBER,
           T1.EXPONUMBER,
           T1.PDPOOLMODELID,
           T1.LGDPOOLMODELID,
           T1.CCFPOOLMODELID,
           T1.PDPOOLID,
           T1.LGDPOOLID,
           T1.CCFPOOLID,
           T1.ABSUAFLAG,
           T1.ABSPOOLID,
           T1.ABSPROPORTION,
           T1.GROUPID,
           T1.AGING,
           T1.LTV,
           T1.NEWDEFAULTDEBTFLAG,
           T1.DEFAULTDATE
      FROM RWA_EI_EXPOSURE T1
      LEFT JOIN SYS_IFRS9_RESULT T2
        ON T1.DATANO = T2.DATANO
       AND DECODE(T1.SSYSID, 'XYK', T1.EXPOSUREID, T1.DUEID) =
           DECODE(T2.ITEM_CODE,
                  '信用卡表外',
                  'BW_' || T2.CONTRACT_REFERENCE,
                  T2.CONTRACT_REFERENCE)
     WHERE T1.DATANO = P_DATA_DT_STR;

  COMMIT;

  EXECUTE IMMEDIATE 'ALTER TABLE RWA_EI_EXPOSURE truncate PARTITION EXPOSURE' ||
                    P_DATA_DT_STR;
  INSERT /*+ APPEND */
  INTO RWA_EI_EXPOSURE
    SELECT * FROM RWA_EI_EXPOSURE_TMP;

  COMMIT;

  /*  将机构排序号暂时先改为1  
      BY WANGZEBO 
      20191119 为了防止机构不能全部覆盖，必须的把机构排序号改了
  */
  update rwa_ei_exposure
     set orgsortno = '1',
         ORGID     = '9998',
         SORGID    = '9998',
         ACCORGID  = '9998',
         ORGNAME   = '特殊处理'
   where ORGID IS NULL
     AND datano = P_DATA_DT_STR;
  COMMIT;

  --整理表信息
  dbms_stats.gather_table_stats(ownname => 'RWA_DEV',
                                tabname => 'RWA_EI_EXPOSURE',
                                cascade => true);

  /* --暂时置空利息，利息虚拟  分系统已处理，不用在这里更新
  UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.NORMALINTEREST = 0,T.ONDEBITINTEREST = 0,T.OFFDEBITINTEREST =0,T.EXPENSERECEIVABLE = 0,T.ASSETBALANCE = T.ONSHEETBALANCE WHERE T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
  
  COMMIT;
  
  --更新EADIRB，排除利息，表外业务要乘以ccf
  UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.EADAIRB = T.ONSHEETBALANCE * (CASE WHEN T.EXPOBELONG = '01' THEN 1 ELSE NVL(T.CCFAIRB,1) END) WHERE T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND T.CREDITRISKDATATYPE = '02';
  
  COMMIT; */

  /*目标表数据统计*/
  --统计插入的记录
  SELECT COUNT(1)
    INTO v_count
    FROM RWA_DEV.RWA_EI_EXPOSURE
   WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD');

  p_po_rtncode := '1';
  p_po_rtnmsg  := '成功' || '-' || v_count;
  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '汇总风险暴露表(PRO_RWA_EI_EXPOSURE)ETL转换失败！' || sqlerrm ||
                    ';错误行数为:' || dbms_utility.format_error_backtrace;
    RETURN;
END PRO_RWA_EI_EXPOSURE;
/

