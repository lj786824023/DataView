CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XF_EXPOSURE(
                            p_data_dt_str  IN   VARCHAR2, --数据日期
                            p_po_rtncode   OUT  VARCHAR2, --返回编号
                            p_po_rtnmsg    OUT  VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_XF_EXPOSURE
    实现功能:消费金融-消费金融-信用风险暴露(从数据源消费金融系统将业务相关信息全量导入RWA消金接口表风险暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :xlpang
    编写时间:2019-05-28
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RMPS_CQ_LOAN|借据信息
    源  表2 :RWA_DEV.RWA_CD_PAYTODW_ORG|机构统一信息
    源  表3 :RWA_DEV.RMPS_CQ_CONTRACT|合同信息
    源  表4 :
    源  表5 :

    目标表1 :RWA_DEV.RWA_XF_EXPOSURE|RWA信用风险暴露信息表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XF_EXPOSURE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;
  --v_count2 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XF_EXPOSURE';

    
    INSERT INTO RWA_DEV.RWA_XF_EXPOSURE(
                DATADATE                        --数据日期
               ,DATANO                          --数据流水号
               ,EXPOSUREID                      --风险暴露ID
               ,DUEID                           --债项ID
               ,SSYSID                          --源系统ID
               ,CONTRACTID                      --合同ID
               ,CLIENTID                        --参与主体ID
               ,SORGID                          --源机构ID
               ,SORGNAME                        --源机构名称
               ,ORGSORTNO                       --所属机构排序号
               ,ORGID                           --所属机构ID
               ,ORGNAME                         --所属机构名称
               ,ACCORGID                        --账务机构ID
               ,ACCORGNAME                      --账务机构名称
               ,INDUSTRYID                      --所属行业代码
               ,INDUSTRYNAME                    --所属行业名称
               ,BUSINESSLINE                    --业务条线
               ,ASSETTYPE                       --资产大类
               ,ASSETSUBTYPE                    --资产小类
               ,BUSINESSTYPEID                  --业务品种代码
               ,BUSINESSTYPENAME                --业务品种名称
               ,CREDITRISKDATATYPE              --信用风险数据类型
               ,ASSETTYPEOFHAIRCUTS             --折扣系数对应资产类别
               ,BUSINESSTYPESTD                 --权重法业务类型
               ,EXPOCLASSSTD                    --权重法暴露大类
               ,EXPOSUBCLASSSTD                 --权重法暴露小类
               ,EXPOCLASSIRB                    --内评法暴露大类
               ,EXPOSUBCLASSIRB                 --内评法暴露小类
               ,EXPOBELONG                      --暴露所属标识
               ,BOOKTYPE                        --账户类别
               ,REGUTRANTYPE                    --监管交易类型
               ,REPOTRANFLAG                    --回购交易标识
               ,REVAFREQUENCY                   --重估频率
               ,CURRENCY                        --币种
               ,NORMALPRINCIPAL                 --正常本金余额
               ,OVERDUEBALANCE                  --逾期余额
               ,NONACCRUALBALANCE               --非应计余额
               ,ONSHEETBALANCE                  --表内余额
               ,NORMALINTEREST                  --正常利息
               ,ONDEBITINTEREST                 --表内欠息
               ,OFFDEBITINTEREST                --表外欠息
               ,EXPENSERECEIVABLE               --应收费用
               ,ASSETBALANCE                    --资产余额
               ,ACCSUBJECT1                     --科目一
               ,ACCSUBJECT2                     --科目二
               ,ACCSUBJECT3                     --科目三
               ,STARTDATE                       --起始日期
               ,DUEDATE                         --到期日期
               ,ORIGINALMATURITY                --原始期限
               ,RESIDUALM                       --剩余期限
               ,RISKCLASSIFY                    --风险分类
               ,EXPOSURESTATUS                  --风险暴露状态
               ,OVERDUEDAYS                     --逾期天数
               ,SPECIALPROVISION                --专项准备金
               ,GENERALPROVISION                --一般准备金
               ,ESPECIALPROVISION               --特别准备金
               ,WRITTENOFFAMOUNT                --已核销金额
               ,OFFEXPOSOURCE                   --表外暴露来源
               ,OFFBUSINESSTYPE                 --表外业务类型
               ,OFFBUSINESSSDVSSTD              --权重法表外业务类型细分
               ,UNCONDCANCELFLAG                --是否可随时无条件撤销
               ,CCFLEVEL                        --信用转换系数级别
               ,CCFAIRB                         --高级法信用转换系数
               ,CLAIMSLEVEL                     --债权级别
               ,BONDFLAG                        --是否为债券
               ,BONDISSUEINTENT                 --债券发行目的
               ,NSUREALPROPERTYFLAG             --是否非自用不动产
               ,REPASSETTERMTYPE                --抵债资产期限类型
               ,DEPENDONFPOBFLAG                --是否依赖于银行未来盈利
               ,IRATING                         --内部评级
               ,PD                              --违约概率
               ,LGDLEVEL                        --违约损失率级别
               ,LGDAIRB                         --高级法违约损失率
               ,MAIRB                           --高级法有效期限
               ,EADAIRB                         --高级法违约风险暴露
               ,DEFAULTFLAG                     --违约标识
               ,BEEL                            --已违约暴露预期损失比率
               ,DEFAULTLGD                      --已违约暴露违约损失率
               ,EQUITYEXPOFLAG                  --股权暴露标识
               ,EQUITYINVESTTYPE                --股权投资对象类型
               ,EQUITYINVESTCAUSE               --股权投资形成原因
               ,SLFLAG                          --专业贷款标识
               ,SLTYPE                          --专业贷款类型
               ,PFPHASE                         --项目融资阶段
               ,REGURATING                      --监管评级
               ,CBRCMPRATINGFLAG                --银监会认定评级是否更为审慎
               ,LARGEFLUCFLAG                   --是否波动性较大
               ,LIQUEXPOFLAG                    --是否清算过程中风险暴露
               ,PAYMENTDEALFLAG                 --是否货款对付模式
               ,DELAYTRADINGDAYS                --延迟交易天数
               ,SECURITIESFLAG                  --有价证券标识
               ,SECUISSUERID                    --证券发行人ID
               ,RATINGDURATIONTYPE              --评级期限类型
               ,SECUISSUERATING                 --证券发行等级
               ,SECURESIDUALM                   --证券剩余期限
               ,SECUREVAFREQUENCY               --证券重估频率
               ,CCPTRANFLAG                     --是否中央交易对手相关交易
               ,CCPID                           --中央交易对手ID
               ,QUALCCPFLAG                     --是否合格中央交易对手
               ,BANKROLE                        --银行角色
               ,CLEARINGMETHOD                  --清算方式
               ,BANKASSETFLAG                   --是否银行提交资产
               ,MATCHCONDITIONS                 --符合条件情况
               ,SFTFLAG                         --证券融资交易标识
               ,MASTERNETAGREEFLAG              --净额结算主协议标识
               ,MASTERNETAGREEID                --净额结算主协议ID
               ,SFTTYPE                         --证券融资交易类型
               ,SECUOWNERTRANSFLAG              --证券所有权是否转移
               ,OTCFLAG                         --场外衍生工具标识
               ,VALIDNETTINGFLAG                --有效净额结算协议标识
               ,VALIDNETAGREEMENTID             --有效净额结算协议ID
               ,OTCTYPE                         --场外衍生工具类型
               ,DEPOSITRISKPERIOD               --保证金风险期间
               ,MTM                             --重置成本
               ,MTMCURRENCY                     --重置成本币种
               ,BUYERORSELLER                   --买方卖方
               ,QUALROFLAG                      --合格参照资产标识
               ,ROISSUERPERFORMFLAG             --参照资产发行人是否能履约
               ,BUYERINSOLVENCYFLAG             --信用保护买方是否破产
               ,NONPAYMENTFEES                  --尚未支付费用
               ,RETAILEXPOFLAG                  --零售暴露标识
               ,RETAILCLAIMTYPE                 --零售债权类型
               ,MORTGAGETYPE                    --住房抵押贷款类型
               ,EXPONUMBER                      --风险暴露个数
               ,LTV                             --贷款价值比
               ,AGING                           --账龄
               ,NEWDEFAULTDEBTFLAG              --新增违约债项标识
               ,PDPOOLMODELID                   --PD分池模型ID
               ,LGDPOOLMODELID                  --LGD分池模型ID
               ,CCFPOOLMODELID                  --CCF分池模型ID
               ,PDPOOLID                        --所属PD池ID
               ,LGDPOOLID                       --所属LGD池ID
               ,CCFPOOLID                       --所属CCF池ID
               ,ABSUAFLAG                       --资产证券化基础资产标识
               ,ABSPOOLID                       --证券化资产池ID
               ,GROUPID                         --分组编号
    )
    SELECT TO_DATE(T1.DATANO, 'YYYYMMDD'), --  数据日期
           T1.DATANO, --  数据流水号
           T1.LOAN_ID, --  风险暴露ID
           T1.LOAN_ID, --  债项ID
           'XJ', --  源系统ID
           T1.CONTRACT_NBR, --  合同ID
           T1.CUST_ID, --  参与主体ID
           T1.CORE_ACCT_ORG, --  源机构ID
           T2.ORGNAME, --  源机构名称
           T2.SORTNO, --  所属机构排序号
           T1.CORE_ACCT_ORG, --  所属机构ID
           T2.ORGNAME, --  所属机构名称
           T1.CORE_ACCT_ORG, --  账务机构ID
           T2.ORGNAME, --  账务机构名称
           NULL, --  所属行业代码
           NULL, --  所属行业名称
           '0301', --  业务条线 默认 个人
           NULL, --  资产大类
           NULL, --  资产小类
           '11103038', --  业务品种代码
           '捷e贷', --  业务品种名称
           '02', --  信用风险数据类型
           '01', --  折扣系数对应资产类别
           NULL, --  权重法业务类型
           NULL, --  权重法暴露大类
           NULL, --  权重法暴露小类
           NULL, --  内评法暴露大类
           NULL, --  内评法暴露小类
           '01', --  暴露所属标识
           '01', --  账户类别
           '03', --  监管交易类型
           '0', --  回购交易标识
           1, --  重估频率
           T1.CURRENCY, --  币种
           T1.PRIN_BAL, --  正常本金余额
           0, --  逾期余额
           0, --  非应计余额
           T1.PRIN_BAL, --  表内余额
           0, --  正常利息
           T1.IN_INTEREST_BAL, --  表内欠息
           T1.OUT_INTEREST_BAL, --  表外欠息
           0, --  应收费用
           T1.PRIN_BAL, --  资产余额
           T1.AccSubject, --  科目一
           NULL, --  科目二
           NULL, --  科目三
           T1.PAY_FINISH_DATE, --  起始日期
           T1.DD_EXPIR_DAY, --  到期日期
           CASE
             WHEN (TO_DATE(T1.DD_EXPIR_DAY, 'YYYYMMDD') -
                  TO_DATE(T1.PAY_FINISH_DATE, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.DD_EXPIR_DAY, 'YYYYMMDD') -
              TO_DATE(T1.PAY_FINISH_DATE, 'YYYYMMDD')) / 365
           END AS ORIGINALMATURITY, -- 原始期限
           CASE
             WHEN (TO_DATE(T1.DD_EXPIR_DAY, 'YYYYMMDD') -
                  TO_DATE(T1.DATANO, 'YYYYMMDD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T1.DD_EXPIR_DAY, 'YYYYMMDD') -
              TO_DATE(T1.DATANO, 'YYYYMMDD')) / 365
           END AS RESIDUALM, -- 剩余期限
           CASE
             WHEN T1.FIVE_CLASS = 'A' THEN
              '01' --正常
             WHEN T1.FIVE_CLASS = 'B' THEN
              '02' --关注
             WHEN T1.FIVE_CLASS = 'C' THEN
              '03' --次级
             WHEN T1.FIVE_CLASS = 'D' THEN
              '04' --可疑
             WHEN T1.FIVE_CLASS = 'E' THEN
              '05' --损失级
             ELSE
              '@' || T1.FIVE_CLASS
           END, --  风险分类
           '01', --  风险暴露状态 默认正常
           T1.OVERDUE_DAYS, --  逾期天数
           0, --  专项准备金
           0, --  一般准备金
           0, --  特别准备金
           0, --  已核销金额
           NULL, --  表外暴露来源
           NULL, --  表外业务类型
           NULL, --  权重法表外业务类型细分
           '1', --  是否可随时无条件撤销
           NULL, --  信用转换系数级别
           0, --  高级法信用转换系数
           '01', --  债权级别 默认 高级债
           '0', --  是否为债券
           NULL, --  债券发行目的
           '0', --  是否非自用不动产
           NULL, --  抵债资产期限类型
           '0', --  是否依赖于银行未来盈利
           NULL, --  内部评级
           NULL, --  违约概率
           NULL, --  违约损失率级别
           NULL, --  高级法违约损失率
           NULL, --  高级法有效期限
           T1.PRIN_BAL, --  高级法违约风险暴露
           NULL, --  违约标识
           0.45, --  已违约暴露预期损失比率
           0.45, --  已违约暴露违约损失率
           '0', --  股权暴露标识
           NULL, --  股权投资对象类型
           NULL, --  股权投资形成原因
           '0', --  专业贷款标识
           NULL, --  专业贷款类型
           NULL, --  项目融资阶段
           NULL, --  监管评级
           '0', --  银监会认定评级是否更为审慎
           '0', --  是否波动性较大
           '0', --  是否清算过程中风险暴露
           '0', --  是否货款对付模式
           NULL, --  延迟交易天数
           '0', --  有价证券标识
           NULL, --  证券发行人ID
           NULL, --  评级期限类型
           NULL, --  证券发行等级
           NULL, --  证券剩余期限
           NULL, --  证券重估频率
           '0', --  是否中央交易对手相关交易
           NULL, --  中央交易对手ID
           NULL, --  是否合格中央交易对手
           NULL, --  银行角色
           NULL, --  清算方式
           NULL, --  是否银行提交资产
           NULL, --  符合条件情况
           '0', --  证券融资交易标识
           NULL, --  净额结算主协议标识
           NULL, --  净额结算主协议ID
           NULL, --  证券融资交易类型
           '0', --  证券所有权是否转移
           NULL, --  场外衍生工具标识
           NULL, --  有效净额结算协议标识
           NULL, --  有效净额结算协议ID
           NULL, --  场外衍生工具类型
           NULL, --  保证金风险期间
           NULL, --  重置成本
           NULL, --  重置成本币种
           NULL, --  买方卖方
           '0', --  合格参照资产标识
           '0', --  参照资产发行人是否能履约
           '0', --  信用保护买方是否破产
           NULL, --  尚未支付费用
           '1', --  零售暴露标识
           '020403', --  零售债权类型
           NULL, --  住房抵押贷款类型
           1, --  风险暴露个数
           NULL, --  贷款价值比
           NULL, --  账龄
           NULL, --  新增违约债项标识
           NULL, --  PD分池模型ID
           NULL, --  LGD分池模型ID
           NULL, --  CCF分池模型ID
           NULL, --  所属PD池ID
           NULL, --  所属LGD池ID
           NULL, --  所属CCF池ID
           NULL, --  资产证券化基础资产标识
           NULL, --  证券化资产池ID
           NULL --  分组编号
      FROM RWA_DEV.RMPS_CQ_LOAN T1 --借据信息
      LEFT JOIN RWA.ORG_INFO T2
        ON T1.CORE_ACCT_ORG = T2.ORGID
     WHERE T1.DATANO = p_data_dt_str --终结日期不为空
       AND T1.PRIN_BAL <> 0 --本金余额不等于零
       AND T1.TERMIN_DATE IS NOT NULL; --借据终结日期不为空

     COMMIT;
    
    

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XF_EXPOSURE',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XF_EXPOSURE;

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count1;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '消费金融系统信用风险暴露('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

      RETURN;
END PRO_RWA_XF_EXPOSURE;
/

