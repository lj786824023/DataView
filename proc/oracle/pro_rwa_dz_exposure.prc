CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_DZ_EXPOSURE(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期
                             p_po_rtncode  OUT  VARCHAR2,    --返回编号
                             p_po_rtnmsg    OUT  VARCHAR2    --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_DZ_EXPOSURE
    实现功能:将补录的抵债资产暴露信息插入到暴露表
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-04-15
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :NCM_ASSET_DEBT_INFO |抵债资产信息表
    目标表  :RWA_DEV.RWA_DZ_EXPOSURE|抵债资产暴露表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_DZ_EXPOSURE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_DZ_EXPOSURE';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_DZ_CONTRACT';

    --2.将满足条件的数据从源表插入到目标表中
    /*插入 信贷系统对公借据 到目标表*/
  /*
    INSERT INTO RWA_DEV.RWA_DZ_EXPOSURE(
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
                ,'DZ-' || T1.GUARANTYID                                                AS EXPOSUREID              -- 风险暴露ID
                ,T1.GUARANTYID                                                         AS DUEID                   -- 债项ID
                ,'DZ'                                                                  AS SSYSID                  -- 源系统ID   抵债资产
                ,'DZ-' || T1.GUARANTYID                                                AS CONTRACTID              -- 合同ID
                ,'DZ-' || T1.GUARANTYID                                                AS CLIENTID                -- 参与主体ID
                ,'01050000'	                                                       		 AS SORGID                  -- 源机构ID
                ,'总行资产负债管理部'                                                  AS SORGNAME                -- 源机构名称
                ,'1010050'                                                             AS ORGSORTNO               --所属机构排序号
                ,'01050000'	                                                       		 AS ORGID                   -- 所属机构ID
                ,'总行资产负债管理部'                                                  AS ORGNAME                 -- 所属机构名称
                ,'01050000'	                                                       		 AS ACCORGID                -- 账务机构ID
                ,'总行资产负债管理部'                                                  AS ACCORGNAME              -- 账务机构名称
                ,'999999'                                                              AS INDUSTRYID              -- 所属行业代码
                ,'未知'                                                                AS INDUSTRYNAME            -- 所属行业名称
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '0501'
                			ELSE '0401'
                 END	                                                                 AS BUSINESSLINE            -- 条线         默认 同业
                ,'129'                                                                 AS ASSETTYPE               -- 资产大类
                ,'12901'                                                               AS ASSETSUBTYPE            -- 资产小类
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '109040'
                      ELSE '109050'
                 END                                                                   AS BUSINESSTYPEID      		--业务品种代码
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '抵债资产不动产类'
                      ELSE '抵债资产非不动产类'
                 END                                                                   AS BUSINESSTYPENAME        -- 业务品种名称
                ,'01'                                                                  AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                ,'01'                                                                  AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,'10'                                                                  AS BUSINESSTYPESTD         -- 权重法业务类型
                ,''                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,''                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,''                                                                    AS EXPOCLASSIRB            -- 内评法暴露大类
                ,''                                                                    AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,'01'                                                                  AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                  AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                  AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                   AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                     AS REVAFREQUENCY           -- 重估频率
                ,'CNY'                                                                 AS CURRENCY                -- 币种
                ,T1.ENTRYVALUE                                                         AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                     AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                     AS NONACCRUALBALANCE       -- 非应计余额
                ,T1.ENTRYVALUE                                                         AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                     AS NORMALINTEREST          -- 正常利息
                ,0                                                                     AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                     AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                     AS EXPENSERECEIVABLE       -- 应收费用
                ,T1.ENTRYVALUE	                                                       AS ASSETBALANCE            -- 资产余额
                ,T3.DITEMNO                                                            AS ACCSUBJECT1             -- 科目一
                ,''                                                                    AS ACCSUBJECT2             -- 科目二
                ,''                                                                    AS ACCSUBJECT3             -- 科目三
                ,T1.ACQUIREDATE                                                    		 AS STARTDATE           		--起始日期
                ,T1.DATANO                                 	                       		 AS DUEDATE             		--到期日期
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365
                 END                                                               AS OriginalMaturity    --原始期限
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365
                 END                                                                   AS ResidualM           --剩余期限
                ,CASE WHEN T1.CLASSIFYRESULT IN('B1','B2','B3','B') THEN '02'       --十二级分类转为五级分类
                      WHEN T1.CLASSIFYRESULT IN('C1','C2','C') THEN '03'
                      WHEN T1.CLASSIFYRESULT IN('D1','D2','D') THEN '04'
                      WHEN T1.CLASSIFYRESULT='E' THEN '05'
                      ELSE '01'
                 END                                                                   AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                  AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,0                                                                     AS OVERDUEDAYS             -- 逾期天数
                ,0											                                               AS SPECIALPROVISION        -- 专项准备金
                ,T1.SUBTRACTVALUEPREPARE                                               AS GENERALPROVISION        -- 一般准备金
                ,0                                                                     AS ESPECIALPROVISION       -- 特别准备金
                ,0                                                                     AS WRITTENOFFAMOUNT        -- 已核销金额
                ,''                                                                    AS OffExpoSource           -- 表外暴露来源
                ,''                                                                    AS OffBusinessType         -- 表外业务类型
                ,''                                                                    AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'0'                                                                   AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                    AS CCFLevel                -- 信用转换系数级别
                ,0                                                                     AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                  AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                   AS BONDFLAG                -- 是否为债券
                ,'02'                                                                  AS BONDISSUEINTENT         -- 债券发行目的
                ,\*CASE WHEN T4.TYPEDIVISION='1' AND T1.HELPONESELF='2' THEN '1'
                      ELSE '0'
                 END   *\ ---BY 王泽波
                 1                                                               AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<=2
                      THEN '01'
                      ELSE '02'
                 END                                                                   AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,NULL                                                                  AS IRATING                 -- 内部评级
                ,NULL                                                                  AS PD                      -- 违约概率
                ,''                                                                    AS LGDLEVEL                -- 违约损失率级别
                ,0                                                                     AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                     AS MAIRB                   -- 高级法有效期限
                ,0                                                                     AS EADAIRB                 -- 高级法违约风险暴露
                ,'0'                                                                   AS DEFAULTFLAG             -- 违约标识
                ,0.45                                                                  AS BEEL                    -- 已违约暴露预期损失比率
                ,0.45                                                                  AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                    AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                    AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                   AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                    AS SLTYPE                  -- 专业贷款类型
                ,''                                                                    AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                  AS REGURATING              -- 监管评级
                ,''                                                                    AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                    AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                   AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                    AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                     AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                   AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                    AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                    AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                     AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                     AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                   AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                    AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                   AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                    AS BANKROLE                -- 银行角色
                ,''                                                                    AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                   AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                    AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                   AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                    AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                    AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                    AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                   AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                    AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                    AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                    AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                    AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                     AS MTM                     -- 重置成本
                ,''                                                                    AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                    AS BUYERORSELLER           -- 买方卖方
                ,''                                                                    AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                     AS NONPAYMENTFEES          -- 尚未支付费用
                ,'0'                                                                   AS RETAILEXPOFLAG          -- 零售暴露标识
                ,''                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,''                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
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
                ,'0'                                                                   AS ABSUAFLAG               -- 资产证券化基础资产标识
                ,''                                                                    AS ABSPOOLID               -- 证券化资产池ID
                ,''                                                                    AS GROUPID                 -- 分组编号
                ,''                                                                    AS DefaultDate             --违约时点
		            ,NULL                                                                  AS ABSPROPORTION           --资产证券化比重
                ,NULL                                                                  AS DEBTORNUMBER            --借款人个数
    FROM 				RWA_DEV.NCM_ASSET_DEBT_INFO T1
    LEFT JOIN 	RWA.ORG_INFO T2
    ON 					T1.MANAGEORGID=T2.ORGID
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T3
    ON          T1.GUARANTYTYPEID=T3.SITEMNO
    AND         T3.SCODENO='GuarantySubjectNO'
    LEFT JOIN   RWA_DEV.NCM_COL_PARAM T4
    ON          T1.GUARANTYTYPEID=T4.GUARANTYTYPE
    AND         T4.DATANO=P_DATA_DT_STR
    WHERE  			T1.DATANO=P_DATA_DT_STR
    AND         T1.ENTRYVALUE <> 0
    ;
    COMMIT;*/

---插入抵债资产补录数据
INSERT INTO RWA_DEV.RWA_DZ_EXPOSURE(
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
                TO_DATE(p_data_dt_str,'YYYYMMDD')                                                                   AS DATADATE                -- 数据日期
                ,p_data_dt_str                                                              AS DATANO                  -- 数据流水号
                ,'DZ-' || T1.SUPPSERIALNO                                                AS EXPOSUREID              -- 风险暴露ID
                ,T1.SUPPSERIALNO                                                        AS DUEID                   -- 债项ID
                ,'BL'                                                                 AS SSYSID                  -- 源系统ID   抵债资产
                ,'DZ-' || T1.SUPPSERIALNO                                               AS CONTRACTID              -- 合同ID
                ,'XN-YBGS'                                                           AS CLIENTID                -- 参与主体ID
                 , '9998',                       --源机构ID
                 '重庆银行股份有限公司',              --源机构名称
                 '1',                        --所属机构排序号
                 '9998',                       --所属机构ID
                 '重庆银行股份有限公司',              --所属机构名称
                 '9998',                       --账务机构ID
                 '重庆银行股份有限公司'                 --账务机构名称
               /* ,'01050000'                                                            AS SORGID                  -- 源机构ID
                ,'总行资产负债管理部'                                                  AS SORGNAME                -- 源机构名称
                ,'1010050'                                                             AS ORGSORTNO               --所属机构排序号
                ,'01050000'                                                            AS ORGID                   -- 所属机构ID
                ,'总行资产负债管理部'                                                  AS ORGNAME                 -- 所属机构名称
                ,'01050000'                                                            AS ACCORGID                -- 账务机构ID
                ,'总行资产负债管理部'                                                  AS ACCORGNAME              -- 账务机构名称*/
                ,'999999'                                                              AS INDUSTRYID              -- 所属行业代码
                ,'未知'                                                                AS INDUSTRYNAME            -- 所属行业名称
                ,'0501'                                                                  AS BUSINESSLINE            -- 条线        
                ,'129'                                                                 AS ASSETTYPE               -- 资产大类
                ,'12901'                                                               AS ASSETSUBTYPE            -- 资产小类
                ,CASE WHEN T1.DZLX='不动产' THEN '109040'
                      ELSE '109050'
                 END                                                                   AS BUSINESSTYPEID          --业务品种代码
                ,CASE WHEN T1.DZLX='不动产' THEN '抵债资产不动产类'
                      ELSE '抵债资产非不动产类'
                 END                                                                   AS BUSINESSTYPENAME        -- 业务品种名称
                ,'01'                                                                  AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                ,'01'                                                                  AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,'10'                                                                  AS BUSINESSTYPESTD         -- 权重法业务类型
                ,''                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,''                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,''                                                                    AS EXPOCLASSIRB            -- 内评法暴露大类
                ,''                                                                    AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,'01'                                                                  AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                  AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                  AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                   AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                     AS REVAFREQUENCY           -- 重估频率
                ,'CNY'                                                                 AS CURRENCY                -- 币种
                ,T1.YE                                                                 AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                     AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                     AS NONACCRUALBALANCE       -- 非应计余额
                ,T1.YE                                                                 AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                     AS NORMALINTEREST          -- 正常利息
                ,0                                                                     AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                     AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                     AS EXPENSERECEIVABLE       -- 应收费用
                ,T1.YE                                                                 AS ASSETBALANCE            -- 资产余额
                ,'14410100'                                                            AS ACCSUBJECT1             -- 科目一
                ,''                                                                    AS ACCSUBJECT2             -- 科目二
                ,''                                                                    AS ACCSUBJECT3             -- 科目三
                ,p_data_dt_str                                                                    AS STARTDATE               --起始日期
                ,p_data_dt_str                                                                    AS DUEDATE                 --到期日期
                ,CASE WHEN T1.DZZCMC='一年以内'
                      THEN '0.5'
                      WHEN T1.DZZCMC='一年以上到两年'
                      THEN '1.5'
                      WHEN T1.DZZCMC='两年以上到三年'
                      THEN '2.5'
                      WHEN T1.DZZCMC='三年以上'
                      THEN '3.5'
                 END                                                                   AS OriginalMaturity    --原始期限
                ,'0'                                                                   AS ResidualM           --剩余期限
                ,'02'                                                                  AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                  AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,0                                                                     AS OVERDUEDAYS             -- 逾期天数
                ,0                                                                     AS SPECIALPROVISION        -- 专项准备金
                ,T1.JZ                                                                 AS GENERALPROVISION        -- 一般准备金
                ,0                                                                     AS ESPECIALPROVISION       -- 特别准备金
                ,0                                                                     AS WRITTENOFFAMOUNT        -- 已核销金额
                ,''                                                                    AS OffExpoSource           -- 表外暴露来源
                ,''                                                                    AS OffBusinessType         -- 表外业务类型
                ,''                                                                    AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'0'                                                                   AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                    AS CCFLevel                -- 信用转换系数级别
                ,0                                                                     AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                  AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                   AS BONDFLAG                -- 是否为债券
                ,'02'                                                                  AS BONDISSUEINTENT         -- 债券发行目的
                ,CASE WHEN T1.DZLX='不动产'  THEN '1'
                      ELSE '0'
                 END   
                                                                     AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,/*CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<=2
                      THEN '01'
                      ELSE '02'
                 END*/  
                 '01'                                                                 AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,NULL                                                                  AS IRATING                 -- 内部评级
                ,NULL                                                                  AS PD                      -- 违约概率
                ,''                                                                    AS LGDLEVEL                -- 违约损失率级别
                ,0                                                                     AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                     AS MAIRB                   -- 高级法有效期限
                ,0                                                                     AS EADAIRB                 -- 高级法违约风险暴露
                ,'0'                                                                   AS DEFAULTFLAG             -- 违约标识
                ,0.45                                                                  AS BEEL                    -- 已违约暴露预期损失比率
                ,0.45                                                                  AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                    AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                    AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                   AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                    AS SLTYPE                  -- 专业贷款类型
                ,''                                                                    AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                  AS REGURATING              -- 监管评级
                ,''                                                                    AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                    AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                   AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                    AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                     AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                   AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                    AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                    AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                     AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                     AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                   AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                    AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                   AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                    AS BANKROLE                -- 银行角色
                ,''                                                                    AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                   AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                    AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                   AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                    AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                    AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                    AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                   AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                    AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                    AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                    AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                    AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                     AS MTM                     -- 重置成本
                ,''                                                                    AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                    AS BUYERORSELLER           -- 买方卖方
                ,''                                                                    AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                     AS NONPAYMENTFEES          -- 尚未支付费用
                ,'0'                                                                   AS RETAILEXPOFLAG          -- 零售暴露标识
                ,''                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,''                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
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
                ,'0'                                                                   AS ABSUAFLAG               -- 资产证券化基础资产标识
                ,''                                                                    AS ABSPOOLID               -- 证券化资产池ID
                ,''                                                                    AS GROUPID                 -- 分组编号
                ,''                                                                    AS DefaultDate             --违约时点
                ,NULL                                                                  AS ABSPROPORTION           --资产证券化比重
                ,NULL                                                                  AS DEBTORNUMBER            --借款人个数
    FROM  RWA.RWA_WS_DZH_BL T1
    WHERE T1.DATADATE= TO_DATE(p_data_dt_str,'YYYYMMDD')            
    ;
    
    COMMIT;

---插入有追索权的销售资产（银行商票和非银商票） 
INSERT INTO RWA_DEV.RWA_DZ_EXPOSURE(
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
   TO_DATE(p_data_dt_str,'YYYYMMDD'),  --数据日期
    p_data_dt_str,                    --数据流水号
   'ZHS'||T1.SUPPSERIALNO,            --风险暴露ID
    T1.SUPPSERIALNO,                         --债项ID
   'BL',                             --源系统ID
   'ZHS'||T1.SUPPSERIALNO,             --合同ID
   CASE WHEN T1.ZCMC='已卖断的银行承兑汇票' THEN   'XN-ZGSYYH'
   ELSE
   'XN-YBGS'
    END,                             --参与主体ID
   '9998',                       --源机构ID
   '重庆银行股份有限公司',              --源机构名称
   '1',                        --所属机构排序号
   '9998',                       --所属机构ID
   '重庆银行股份有限公司',              --所属机构名称
   '9998',                       --账务机构ID
   '重庆银行股份有限公司',                  --账务机构名称
   'J',                          --所属行业代码
   '金融业',                 --所属行业名称
   '0401',                           --业务条线
   '213',                            --资产大类
   '21301',                          --资产小类
   '1020301010',                             --业务品种代码
   '有追索权卖方保理',                             --业务品种名称
   '01',                             --信用风险数据类型
   '01',                             --折扣系数对应资产类别
   '07',                             --权重法业务类型
 CASE WHEN T1.ZCMC='已卖断的银行承兑汇票' THEN   '0104'
   ELSE
   '0106'
    END,                           --权重法暴露大类
  CASE WHEN T1.ZCMC='已卖断的银行承兑汇票' THEN   '010406'
   ELSE
   '010601'
    END,                         --权重法暴露小类
   '0203',                           --内评法暴露大类
   '020301',                         --内评法暴露小类
   '02',                             --暴露所属标识
   '01',                             --账户类别
   '03',                             --监管交易类型
   '0',                              --回购交易标识
   1,                                --重估频率
   'CNY',                             --币种
   T1.YE,                       --正常本金余额
   0.000000,                         --逾期余额
   0.000000,                         --非应计余额
   T1.YE,                       --表内余额
   0.000000,                         --正常利息
   0.000000,                         --表内欠息
   0.000000,                         --表外欠息
   0.000000,                         --应收费用
   T1.YE,                          --资产余额
   '',                              --科目一
   null,                             --科目二
   null,                             --科目三
   p_data_dt_str,                     --起始日期
   p_data_dt_str,                     --到期日期
   0.495890,                         --原始期限
   0.328767,                         --剩余期限
   '01',                             --风险分类
   '01',                             --风险暴露状态
   0,                                --逾期天数
   0.000000,                         --专项准备金
   0.000000,                         --一般准备金
   0.000000,                         --特别准备金
   0.000000,                         --已核销金额
   '03',                             --表外暴露来源
   '10',                             --表外业务类型
   '1002',                           --权重法表外业务类型细分
   null,                             --是否可随时无条件撤销
   null,                             --信用转换系数级别
   null,                             --高级法信用转换系数
   '01',                             --债权级别
   '0',                              --是否为债券
   '02',                             --债券发行目的
   '0',                              --是否非自用不动产
   null,                             --抵债资产期限类型
   '0',                              --是否依赖于银行未来盈利
   '0106',                           --内部评级
   null,                             --违约概率
   null,                             --违约损失率级别
   null,                             --高级法违约损失率
   null,                             --高级法有效期限
   null,                             --高级法违约风险暴露
   '0',                             --违约标识
   null,                             --已违约暴露预期损失比率
   null,                             --已违约暴露违约损失率
   null,                             --股权暴露标识
   null,                             --股权投资对象类型
   null,                             --股权投资形成原因
   null,                             --专业贷款标识
   null,                             --专业贷款类型
   null,                             --项目融资阶段
   null,                             --监管评级
   null,                             --银监会认定评级是否更为审慎
   null,                             --是否波动性较大
   null,                             --是否清算过程中风险暴露
   null,                             --是否货款对付模式
   null,                             --延迟交易天数
   null,                             --有价证券标识
   null,                             --证券发行人ID
   null,                             --评级期限类型
   null,                             --证券发行等级
   null,                             --证券剩余期限
   null,                             --证券重估频率
   null,                             --是否中央交易对手相关交易
   null,                             --中央交易对手ID
   null,                             --是否合格中央交易对手
   null,                             --银行角色
   null,                             --清算方式
   null,                             --是否银行提交资产
   null,                             --符合条件情况
   '0',                              --证券融资交易标识
   null,                             --净额结算主协议标识
   null,                             --净额结算主协议ID
   null,                             --证券融资交易类型
   null,                             --证券所有权是否转移
   '0',                              --场外衍生工具标识
   null,                             --有效净额结算协议标识
   null,                             --有效净额结算协议ID
   null,                             --场外衍生工具类型
   null,                             --保证金风险期间
   null,                             --重置成本
   null,                             --重置成本币种
   null,                             --买方卖方
   null,                             --合格参照资产标识
   null,                             --参照资产发行人是否能履约
   null,                             --信用保护买方是否破产
   null,                             --尚未支付费用
   null,                             --零售暴露标识
   null,                             --零售债权类型
   null,                             --住房抵押贷款类型
   null,                             --风险暴露个数
   null,                             --贷款价值比
   null,                             --账龄
   null,                             --新增违约债项标识
   null,                             --PD分池模型ID
   null,                             --LGD分池模型ID
   null,                             --CCF分池模型ID
   null,                             --所属PD池ID
   '0',                              --所属LGD池ID
   null,                             --所属CCF池ID
   null,                             --资产证券化基础资产标识
   '',                   --证券化资产池ID
   null,                             --分组编号
   null,                             --违约时点
   null,                             --资产证券化比重
   null                              --借款人个数
   FROM RWA.RWA_WS_ZHS_BL T1
   WHERE T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD') ;        

    COMMIT;
    
    
---插入无条件随时撤销的贷款承诺
INSERT INTO RWA_DEV.RWA_DZ_EXPOSURE(
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
   TO_DATE(p_data_dt_str,'YYYYMMDD'),  --数据日期
    p_data_dt_str,                    --数据流水号
   'DKCN'||T1.SUPPSERIALNO,            --风险暴露ID
    T1.SUPPSERIALNO,                         --债项ID
   'BL',                             --源系统ID
   'DKCN'||T1.SUPPSERIALNO,             --合同ID
   'XN-YBGS',                       --参与主体ID
   '9998',                       --源机构ID
   '重庆银行股份有限公司',              --源机构名称
   '1',                        --所属机构排序号
   '9998',                       --所属机构ID
   '重庆银行股份有限公司',              --所属机构名称
   '9998',                       --账务机构ID
   '重庆银行股份有限公司',                  --账务机构名称
   'J',                          --所属行业代码
   '金融业',                 --所属行业名称
   '0501',                           --业务条线
   '215',                            --资产大类
   '21501',                          --资产小类
   '102040',                             --业务品种代码
   '贷款承诺',                             --业务品种名称
   '01',                             --信用风险数据类型
   '01',                             --折扣系数对应资产类别
   '07',                             --权重法业务类型
   '0106',                           --权重法暴露大类
   '010601',                         --权重法暴露小类
   '0203',                           --内评法暴露大类
   '020301',                         --内评法暴露小类
   '02',                             --暴露所属标识
   '01',                             --账户类别
   '03',                             --监管交易类型
   '0',                              --回购交易标识
   1,                                --重估频率
   'CNY',                             --币种
   T1.YE,                       --正常本金余额
   0.000000,                         --逾期余额
   0.000000,                         --非应计余额
   T1.YE,                       --表内余额
   0.000000,                         --正常利息
   0.000000,                         --表内欠息
   0.000000,                         --表外欠息
   0.000000,                         --应收费用
   T1.YE,                          --资产余额
   '',                              --科目一
   null,                             --科目二
   null,                             --科目三
  p_data_dt_str,                             --起始日期
   p_data_dt_str,                             --到期日期
   0.495890,                         --原始期限
   0.328767,                         --剩余期限
   '01',                             --风险分类
   '01',                             --风险暴露状态
   0,                                --逾期天数
   0.000000,                         --专项准备金
   0.000000,                         --一般准备金
   0.000000,                         --特别准备金
   0.000000,                         --已核销金额
   '03',                             --表外暴露来源
   '02',                             --表外业务类型
   '0201',                           --权重法表外业务类型细分
   '1',                             --是否可随时无条件撤销
   null,                             --信用转换系数级别
   null,                             --高级法信用转换系数
   '01',                             --债权级别
   '0',                              --是否为债券
   '02',                             --债券发行目的
   '0',                              --是否非自用不动产
   null,                             --抵债资产期限类型
   '0',                              --是否依赖于银行未来盈利
   '0106',                           --内部评级
   null,                             --违约概率
   null,                             --违约损失率级别
   null,                             --高级法违约损失率
   null,                             --高级法有效期限
   null,                             --高级法违约风险暴露
   '0',                             --违约标识
   null,                             --已违约暴露预期损失比率
   null,                             --已违约暴露违约损失率
   null,                             --股权暴露标识
   null,                             --股权投资对象类型
   null,                             --股权投资形成原因
   null,                             --专业贷款标识
   null,                             --专业贷款类型
   null,                             --项目融资阶段
   null,                             --监管评级
   null,                             --银监会认定评级是否更为审慎
   null,                             --是否波动性较大
   null,                             --是否清算过程中风险暴露
   null,                             --是否货款对付模式
   null,                             --延迟交易天数
   null,                             --有价证券标识
   null,                             --证券发行人ID
   null,                             --评级期限类型
   null,                             --证券发行等级
   null,                             --证券剩余期限
   null,                             --证券重估频率
   null,                             --是否中央交易对手相关交易
   null,                             --中央交易对手ID
   null,                             --是否合格中央交易对手
   null,                             --银行角色
   null,                             --清算方式
   null,                             --是否银行提交资产
   null,                             --符合条件情况
   '0',                              --证券融资交易标识
   null,                             --净额结算主协议标识
   null,                             --净额结算主协议ID
   null,                             --证券融资交易类型
   null,                             --证券所有权是否转移
   '0',                              --场外衍生工具标识
   null,                             --有效净额结算协议标识
   null,                             --有效净额结算协议ID
   null,                             --场外衍生工具类型
   null,                             --保证金风险期间
   null,                             --重置成本
   null,                             --重置成本币种
   null,                             --买方卖方
   null,                             --合格参照资产标识
   null,                             --参照资产发行人是否能履约
   null,                             --信用保护买方是否破产
   null,                             --尚未支付费用
   null,                             --零售暴露标识
   null,                             --零售债权类型
   null,                             --住房抵押贷款类型
   null,                             --风险暴露个数
   null,                             --贷款价值比
   null,                             --账龄
   null,                             --新增违约债项标识
   null,                             --PD分池模型ID
   null,                             --LGD分池模型ID
   null,                             --CCF分池模型ID
   null,                             --所属PD池ID
   '0',                              --所属LGD池ID
   null,                             --所属CCF池ID
   null,                             --资产证券化基础资产标识
   '',                         --证券化资产池ID
   null,                             --分组编号
   null,                             --违约时点
   null,                             --资产证券化比重
   null                              --借款人个数
   FROM RWA.RWA_WS_DKCN_BL T1
   WHERE T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD') ;        

    COMMIT;
    
    /*插入抵债和表外追索权合同*/
       INSERT INTO RWA_DEV.RWA_DZ_CONTRACT(
               DATADATE                              --数据日期
              ,DATANO                                --数据流水号
              ,CONTRACTID                            --合同ID
              ,SCONTRACTID                           --源合同ID
              ,SSYSID                                --源系统ID
              ,CLIENTID                              --参与主体ID
              ,SORGID                                --源机构ID
              ,SORGNAME                              --源机构名称
              ,ORGSORTNO                             --所属机构排序号
              ,ORGID                                 --所属机构ID
              ,ORGNAME                               --所属机构名称
              ,INDUSTRYID                            --所属行业代码
              ,INDUSTRYNAME                          --所属行业名称
              ,BUSINESSLINE                          --条线
              ,ASSETTYPE                             --资产大类
              ,ASSETSUBTYPE                          --资产小类
              ,BUSINESSTYPEID                        --业务品种代码
              ,BUSINESSTYPENAME                      --业务品种名称
              ,CREDITRISKDATATYPE                    --信用风险数据类型
              ,STARTDATE                             --起始日期
              ,DUEDATE                               --到期日期
              ,ORIGINALMATURITY                      --原始期限
              ,RESIDUALM                             --剩余期限
              ,SETTLEMENTCURRENCY                    --结算币种
              ,CONTRACTAMOUNT                        --合同总金额
              ,NOTEXTRACTPART                        --合同未提取部分
              ,UNCONDCANCELFLAG                      --是否可随时无条件撤销
              ,ABSUAFLAG                             --资产证券化基础资产标识
              ,ABSPOOLID                             --证券化资产池ID
              ,GROUPID                               --分组编号
              ,GUARANTEETYPE                         --主要担保方式
              ,ABSPROPORTION                         --资产证券化比重
    )
     SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                     AS DATADATE            --数据日期
                ,T1.DATANO                                                         AS DATANO              --数据流水号
                ,T1.CONTRACTID                                         AS CONTRACTID          --合同ID
                ,T1.CONTRACTID                                                    AS SCONTRACTID         --源合同ID
                ,T1.SSYSID                                                             AS SSYSID              --源系统ID
                ,T1.CLIENTID                                            AS CLIENTID            --参与主体ID
                ,T1.SORGID                                                       AS SORGID              --源机构ID
                ,T1.SORGNAME                                              AS SORGNAME            --源机构名称
                ,T1.ORGSORTNO                                                         AS ORGSORTNO           --所属机构排序号
                ,T1.ORGID                                                        AS ORGID               --所属机构ID
                ,T1.ORGNAME                                             AS ORGNAME             --所属机构名称
                ,T1.INDUSTRYID                                                          AS INDUSTRYID          --所属行业代码
                ,T1.INDUSTRYNAME                                                         AS INDUSTRYNAME        --所属行业名称
                ,T1.BUSINESSLINE                                                               AS BUSINESSLINE        --条线
                ,T1.ASSETTYPE                                                             AS ASSETTYPE           --资产大类
                ,T1.ASSETSUBTYPE                                                         AS ASSETSUBTYPE        --资产小类
                ,T1.BUSINESSTYPEID                                                              AS BUSINESSTYPEID      --业务品种代码
                ,T1.BUSINESSTYPENAME                                                              AS BUSINESSTYPENAME    --业务品种名称
                ,'01'                                                              AS CREDITRISKDATATYPE  --信用风险数据类型
                ,T1.STARTDATE                                                    AS STARTDATE           --起始日期
                ,T1.DUEDATE                                                         AS DUEDATE             --到期日期
                ,T1.ORIGINALMATURITY                                                              AS OriginalMaturity    --原始期限
                ,T1.RESIDUALM                                                              AS ResidualM           --剩余期限
                ,'CNY'                                                             AS SETTLEMENTCURRENCY  --结算币种
                ,T1.NORMALPRINCIPAL                                                     AS CONTRACTAMOUNT      --合同总金额
                ,0                                                                 AS NOTEXTRACTPART      --合同未提取部分
                ,T1.UNCONDCANCELFLAG                                                              AS UNCONDCANCELFLAG    --是否可随时无条件撤销    0:否，1：是
                ,'0'                                                               AS ABSUAFLAG           --资产证券化基础资产标识
                ,NULL                                                              AS ABSPOOLID           --证券化资产池ID
                ,''                                                                AS GROUPID             --分组编号
                ,''                                                                AS GUARANTEETYPE       --主要担保方式
                ,NULL                                                              AS ABSPROPORTION       --资产证券化比重
    FROM        RWA_DZ_EXPOSURE T1
    WHERE       T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT; 
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_DZ_EXPOSURE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_DZ_EXPOSURE;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '抵债资产补录暴露表(RWA_DEV.RWA_DZ_EXPOSURE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_DZ_EXPOSURE;
/

