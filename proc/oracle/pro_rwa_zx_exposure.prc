CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZX_EXPOSURE(
                            p_data_dt_str IN  VARCHAR2,   --数据日期
                            p_po_rtncode  OUT VARCHAR2,   --返回编号
                            p_po_rtnmsg   OUT VARCHAR2    --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZX_EXPOSURE
    实现功能:核心系统-直销银行垫款-信用风险暴露
    数据口径:全量
    跑批频率:月末运行
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-10-18
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.CBS_IAC|通用分户帐
    源  表2 :RWA.ORG_INFO|机构表
    源  表3 :RWA.RWA_WS_DSBANK_ADV|直销银行垫款补录表
    源  表4 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表1 :RWA_DEV.RWA_ZX_EXPOSURE|直销银行暴露表
    辅助表 :无
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZX_EXPOSURE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;
  --v_count2 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZX_EXPOSURE';

    --2.将满足条件的数据从源表插入到目标表中
    /*插入 暴露表—直销银行垫款业务 到目标表*/
    INSERT INTO RWA_DEV.RWA_ZX_EXPOSURE(
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
               ,DefaultDate                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
    )
     SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                         AS DATADATE                -- 数据日期
                ,T1.DATANO                                                             AS DATANO                  -- 数据流水号
                ,T1.IACAC_NO                                                           AS EXPOSUREID              -- 风险暴露ID
                ,T1.IACAC_NO                                                           AS DUEID                   -- 债项ID
                ,'ZX'                                                                  AS SSYSID                  -- 源系统ID
                ,T1.IACAC_NO                                                           AS CONTRACTID              -- 合同ID
                ,T2.CUSTID1                                                            AS CLIENTID                -- 参与主体ID   取参与主体表参与主体ID
                ,T1.IACGACBR                                                           AS SORGID                  -- 源机构ID
                ,T3.ORGNAME                                                            AS SORGNAME                -- 源机构名称
                ,T3.SORTNO                                                             AS ORGSORTNO               -- 所属机构排序号
                ,T1.IACGACBR                                                           AS ORGID                   -- 所属机构ID
                ,T3.ORGNAME                                                            AS ORGNAME                 -- 所属机构名称
                ,T1.IACGACBR                                                           AS ACCORGID                -- 账务机构ID
                ,T3.ORGNAME                                                            AS ACCORGNAME              -- 账务机构名称
                ,T2.INDUSTRYID                                                         AS INDUSTRYID              -- 所属行业代码
                ,T4.ITEMNAME                                                           AS INDUSTRYNAME            -- 所属行业名称
                ,'0101'                                                                AS BUSINESSLINE            -- 条线                      默认 01-大中
                ,''                                                                    AS ASSETTYPE               -- 资产大类                  默认“存放同业”11601  改为暴露分类计算
                ,''                                                                    AS ASSETSUBTYPE            -- 资产小类                  默认“存放同业”11601  改为暴露分类计算
                ,'109010'                                                              AS BUSINESSTYPEID          -- 业务品种代码              默认“存放同业”11601
                ,'直销银行垫款'                                                         AS BUSINESSTYPENAME        -- 业务品种名称              默认“存放同业”11601
                ,'01'                                                                  AS CREDITRISKDATATYPE      -- 信用风险数据类型          默认一般非零售 01
                ,'01'                                                                  AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别      默认“现金及现金等价物”
                ,''                                                                    AS BUSINESSTYPESTD         -- 权重法业务类型            默认“一般资产”07
                ,''                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类            RWA 不能为空
                ,''                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类            RWA 不能为空
                ,''                                                                    AS EXPOCLASSIRB            -- 内评法暴露大类            RWA 不能为空
                ,''                                                                    AS EXPOSUBCLASSIRB         -- 内评法暴露小类            RWA 不能为空
                ,'01'                                                                  AS EXPOBELONG              -- 暴露所属标识              默认01 表内
                ,'01'                                                                  AS BOOKTYPE                -- 账户类别                  01-银行账户
                ,'03'                                                                  AS REGUTRANTYPE            -- 监管交易类型              02-其他资本市场交易
                ,'0'                                                                   AS REPOTRANFLAG            -- 回购交易标识              0-否
                ,1                                                                     AS REVAFREQUENCY           -- 重估频率                  默认为1
                ,'CNY'                                                                 AS CURRENCY                -- 币种
                ,ABS(T1.IACCURBAL)                                                     AS NORMALPRINCIPAL         -- 正常本金余额              需取绝对值
                ,0                                                                     AS OVERDUEBALANCE          -- 逾期余额                  默认为0
                ,0                                                                     AS NONACCRUALBALANCE       -- 非应计余额                默认为0
                ,ABS(T1.IACCURBAL)+0+0                                                 AS ONSHEETBALANCE          -- 表内余额                  表内余额=正常本金余额+逾期余额+非应余额计
                ,0                                                                     AS NORMALINTEREST          -- 正常利息
                ,0                                                                     AS ONDEBITINTEREST         -- 表内欠息                  默认为0
                ,0                                                                     AS OFFDEBITINTEREST        -- 表外欠息                  默认为0
                ,0                                                                     AS EXPENSERECEIVABLE       -- 应收费用                  默认为0
                ,nvl(ABS(T1.IACCURBAL),0)                                              AS ASSETBALANCE            -- 资产余额
                ,T1.IACITMNO                                                           AS ACCSUBJECT1             -- 科目一
                ,''                                                                    AS ACCSUBJECT2             -- 科目二
                ,''                                                                    AS ACCSUBJECT3             -- 科目三
                ,NVL(T2.IACCRTDAT,P_DATA_DT_STR)                                       AS STARTDATE           -- 起始日期
                ,NVL(T2.IACDLTDAT,TO_CHAR(TO_DATE(P_DATA_DT_STR,'YYYYMMDD')+30,'YYYYMMDD'))           AS DUEDATE             -- 到期日期
                ,NVL((TO_DATE(T2.IACDLTDAT,'YYYYMMDD')-TO_DATE(T2.IACCRTDAT,'YYYYMMDD'))/365,30/365)  AS ORIGINALMATURITY    --原始期限
                ,NVL((TO_DATE(T2.IACDLTDAT,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365,30/365) AS RESIDUALM           --剩余期限
                ,'01'                                                                  AS RISKCLASSIFY            -- 风险分类                  01-正常
                ,'01'                                                                  AS EXPOSURESTATUS          -- 风险暴露状态              01-正常
                ,0                                                                     AS OVERDUEDAYS             -- 逾期天数                  默认为0
                ,0                                                                     AS SPECIALPROVISION        -- 专项准备金                RWA计算
                ,0                                                                     AS GENERALPROVISION        -- 一般准备金                RWA计算
                ,0                                                                     AS ESPECIALPROVISION       -- 特别准备金                RWA计算
                ,NULL                                                                  AS WRITTENOFFAMOUNT        -- 已核销金额                默认为空
                ,''                                                                    AS OffExpoSource           -- 表外暴露来源              默认为空 可用额度01
                ,''                                                                    AS OffBusinessType         -- 表外业务类型              默认为空 等同于贷款的授信业务01
                ,''                                                                    AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分    默认为空 银行承兑汇票0101
                ,'0'                                                                   AS UncondCancelFlag        -- 是否可随时无条件撤销      默认为空 默认为否0
                ,''                                                                    AS CCFLevel                -- 信用转换系数级别          默认为空 高级法ccf
                ,NULL                                                                  AS CCFAIRB                 -- 高级法信用转换系数        默认为空
                ,'01'                                                                  AS CLAIMSLEVEL             -- 债权级别                  RWA               默认01—高级权债
                ,'0'                                                                   AS BONDFLAG                -- 是否为债券                RWA               默认为否 0
                ,'01'                                                                  AS BONDISSUEINTENT         -- 债券发行目的              RWA               收购国有银行不良贷款01
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- 是否非自用不动产          RWA               默认为否 0
                ,'01'                                                                  AS REPASSETTERMTYPE        -- 抵债资产期限类型          RWA               法律规定处分期限内01
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利    默认否0
                ,''                                                                    AS IRATING                 -- 内部评级                  取参与表的        默认为空
                ,NULL                                                                  AS PD                      -- 违约概率                  取参与表的        默认为0
                ,NULL                                                                  AS LGDLEVEL                -- 违约损失率级别            默认为NULL
                ,NULL                                                                  AS LGDAIRB                 -- 高级法违约损失率          NULL
                ,NULL                                                                  AS MAIRB                   -- 高级法有效期限            NULL
                ,NULL                                                                  AS EADAIRB                 -- 高级法违约风险暴露        NULL
                ,'0'                                                                   AS DEFAULTFLAG             -- 违约标识                  默认为否 0
                ,0.45                                                                  AS BEEL                    -- 已违约暴露预期损失比率    默认为45%
                ,0.45                                                                  AS DEFAULTLGD              -- 已违约暴露违约损失率      默认为45%
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- 股权暴露标识              默认为否 0
                ,''                                                                    AS EQUITYINVESTTYPE        -- 股权投资对象类型          商业银行0202
                ,''                                                                    AS EQUITYINVESTCAUSE       -- 股权投资形成原因          被动持有01
                ,'0'                                                                   AS SLFLAG                  -- 专业贷款标识              默认为否 0
                ,''                                                                    AS SLTYPE                  -- 专业贷款类型              项目融资02030301
                ,''                                                                    AS PFPHASE                 -- 项目融资阶段              建设期01
                ,''                                                                    AS REGURATING              -- 监管评级                  优01
                ,'0'                                                                   AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎  默认为否 0
                ,'0'                                                                   AS LARGEFLUCFLAG           -- 是否波动性较大              默认为否 0
                ,'0'                                                                   AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露      默认为否 0
                ,'0'                                                                   AS PAYMENTDEALFLAG         -- 是否货款对付模式            默认为否 0
                ,0                                                                     AS DELAYTRADINGDAYS        -- 延迟交易天数                默认0
                ,'0'                                                                   AS SECURITIESFLAG          -- 有价证券标识                02
                ,''                                                                    AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- 评级期限类型                长期信用评级01
                ,''                                                                    AS SECUISSUERATING         -- 证券发行等级                AAA
                ,NULL                                                                  AS SECURESIDUALM           -- 证券剩余期限                默认空
                ,1                                                                     AS SECUREVAFREQUENCY       -- 证券重估频率                默认0
                ,'0'                                                                   AS CCPTRANFLAG             -- 是否中央交易对手相关交易    默认为否 0
                ,''                                                                    AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                   AS QUALCCPFLAG             -- 是否合格中央交易对手       默认为否 0
                ,''                                                                    AS BANKROLE                -- 银行角色                   客户-02
                ,''                                                                    AS CLEARINGMETHOD          -- 清算方式                   为客户-02
                ,'0'                                                                   AS BANKASSETFLAG           -- 是否银行提交资产           默认为否-0
                ,''                                                                    AS MATCHCONDITIONS         -- 符合条件情况               完全满足条件01
                ,'0'                                                                   AS SFTFLAG                 -- 证券融资交易标识           默认为否-0
                ,'0'                                                                  AS MASTERNETAGREEFLAG      -- 净额结算主协议标识         利率01
                ,NULL                                                                  AS MASTERNETAGREEID        -- 净额结算主协议ID           不为空，代码表没有
                ,''                                                                    AS SFTTYPE                 -- 证券融资交易类型           正回购01
                ,'0'                                                                   AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移         默认为否-0
                ,'0'                                                                   AS OTCFLAG                 -- 场外衍生工具标识           05
                ,'0'                                                                   AS VALIDNETTINGFLAG        -- 有效净额结算协议标识       默认为否-0
                ,''                                                                    AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID         不为空，代码表没有
                ,''                                                                    AS OTCTYPE                 -- 场外衍生工具类型           利率01
                ,0                                                                     AS DEPOSITRISKPERIOD       -- 保证金风险期间             默认0
                ,0                                                                     AS MTM                     -- 重置成本                   默认0
                ,''                                                                    AS MTMCURRENCY             -- 重置成本币种               01
                ,''                                                                    AS BUYERORSELLER           -- 买方卖方                   信用保护买方01
                ,'0'                                                                   AS QUALROFLAG              -- 合格参照资产标识           默认否0
                ,'0'                                                                   AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约   默认否0
                ,'0'                                                                   AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产       默认否0
                ,0                                                                     AS NONPAYMENTFEES          -- 尚未支付费用               默认0
                ,'0'                                                                   AS RETAILEXPOFLAG          -- 零售暴露标识               默认否0
                ,''                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型               个人住房抵押贷款020401
                ,''                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型           个人住房抵押追加贷款01
                ,1                                                                     AS EXPONUMBER              -- 风险暴露个数
                ,0.8                                                                   AS LTV                      --贷款价值比  统一更新
                ,''                                                                    AS AGING                    --账龄
                ,''                                                                    AS NEWDEFAULTDEBTFLAG       --新增违约债项标识
                ,''                                                                    AS PDPOOLMODELID           -- PD分池模型ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD分池模型ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF分池模型ID
                ,''                                                                    AS PDPOOLID                -- 所属PD池ID
                ,''                                                                    AS LGDPOOLID               -- 所属LGD池ID
                ,''                                                                    AS CCFPOOLID               -- 所属CCF池ID
                ,'0'                                                                   AS ABSUAFLAG               -- 资产证券化基础资产标识     默认否0
                ,''                                                                    AS ABSPOOLID               -- 证券化资产池ID             资产证券化07
                ,''                                                                    AS GROUPID                 -- 分组编号
                ,''                                                                    AS DefaultDate             --违约时点
                ,NULL                                                                  AS ABSPROPORTION           --资产证券化比重
                ,NULL                                                                  AS DEBTORNUMBER            --借款人个数
    FROM        RWA_DEV.CBS_IAC T1                                        --通用分户帐
    LEFT JOIN   (SELECT WDA.IACAC_NO
                       ,WDA.CUSTID1
                       ,WDA.INDUSTRYID
                       ,TO_CHAR(TO_DATE(WDA.IACCRTDAT,'YYYY-MM-DD'),'YYYYMMDD') AS IACCRTDAT
                       ,TO_CHAR(TO_DATE(WDA.IACDLTDAT,'YYYY-MM-DD'),'YYYYMMDD') AS IACDLTDAT
                   FROM RWA.RWA_WS_DSBANK_ADV WDA                         --直销银行垫款数据补录表
             INNER JOIN RWA.RWA_WP_DATASUPPLEMENT T6                      --数据补录表
                     ON WDA.SUPPORGID = T6.ORGID
                    AND T6.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                    AND T6.SUPPTMPLID = 'M-0190'
                    AND T6.SUBMITFLAG = '1'
                  WHERE WDA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                ) T2
    ON          T1.IACAC_NO = T2.IACAC_NO
    LEFT JOIN   RWA.ORG_INFO T3
    ON          T1.IACGACBR = T3.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY  T4                                      --码表，获取行业
    ON          T2.INDUSTRYID = T4.ITEMNO
    AND         T4.CODENO = 'IndustryType'
    WHERE       T1.IACITMNO='13070800'                                    --直销银行垫款
    AND         T1.IACCURBAL <> 0                                         --账户余额不等于0
    AND         T1.DATANO = p_data_dt_str
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZX_EXPOSURE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ZX_EXPOSURE;


    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '信用风险暴露('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZX_EXPOSURE;
/

