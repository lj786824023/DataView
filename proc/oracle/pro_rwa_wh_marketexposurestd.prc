CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WH_MARKETEXPOSURESTD(
                                                   p_data_dt_str    IN    VARCHAR2,        --数据日期 yyyyMMdd
                                                   p_po_rtncode    OUT    VARCHAR2,        --返回编号 1 成功,0 失败
                                                   p_po_rtnmsg     OUT    VARCHAR2         --返回描述
                )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_WH_MARKETEXPOSURESTD
    实现功能:国结系统-市场风险-标准法暴露表(从数据源外汇现货头寸表全量导入RWA市场风险国结接口表外汇标准法暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-12
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_WH_FESPOTPOSITION|外汇现货头寸表
    源  表2 :RWA.ORG_INFO|RWA机构表
    源  表3 :RWA_DEV.RWA_WH_FEFORWARDSSWAP|外汇远期掉期表（掉期）
    目标表  :RWA_DEV.RWA_WH_MARKETEXPOSURESTD|国结系统外汇标准法暴露表
    变更记录(修改人|修改时间|修改内容):
    pxl  2019/09/09  调整期逻辑
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WH_MARKETEXPOSURESTD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_WH_MARKETEXPOSURESTD';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-现货头寸资产、负债、损益
    INSERT INTO RWA_DEV.RWA_WH_MARKETEXPOSURESTD(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT                       --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME									 --期权基础工具名称
                ,ORGSORTNO														 --机构排序号

    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str                                AS DATANO                   --数据流水号
                ,T1.POSITIONID                                AS EXPOSUREID               --风险暴露ID
                ,T1.BOOKTYPE                                  AS BOOKTYPE                 --账户类别
                ,T1.POSITIONID                                AS INSTRUMENTSID            --金融工具ID
                ,T1.INSTRUMENTSTYPE                           AS INSTRUMENTSTYPE          --金融工具类型
                ,T1.ACCORGID                                  AS ORGID                    --所属机构ID
                ,T3.ORGNAME                                   AS ORGNAME                  --所属机构名称
                ,'01'                                         AS ORGTYPE                  --所属机构类型                                 默认：境内机构(01)
                ,'03'                                         AS MARKETRISKTYPE           --市场风险类型                                 默认：外汇风险(03)
                ,''                                           AS INTERATERISKTYPE         --利率风险类型                                 默认：空
                ,''                                           AS EQUITYRISKTYPE           --股票风险类型                                 默认：空
                ,CASE WHEN T1.CURRENCY = 'CNY' THEN ''                                                                                                                                             --人民币无需映射
                            WHEN T1.CURRENCY = 'USD' THEN '01'                                                                                                                                         --美元(01)
                            WHEN T1.CURRENCY = 'EUR' THEN '02'                                                                                                                                         --欧元(02)
                            WHEN T1.CURRENCY = 'JPY' THEN '03'                                                                                                                                         --日元(03)
                            WHEN T1.CURRENCY = 'GBP' THEN '04'                                                                                                                                         --英镑(04)
                            WHEN T1.CURRENCY = 'HKD' THEN '05'                                                                                                                                         --港元(05)
                            WHEN T1.CURRENCY = 'CHF' THEN '06'                                                                                                                                         --瑞士法郎(06)
                            WHEN T1.CURRENCY = 'AUD' THEN '07'                                                                                                                                         --澳大利亚元(07)
                            WHEN T1.CURRENCY = 'CAD' THEN '08'                                                                                                                                         --加拿大元(08)
                            WHEN T1.CURRENCY = 'SGD' THEN '09'                                                                                                                                         --新加坡元(09)
                            WHEN T1.CURRENCY NOT IN ('CNY','USD','EUR','JPY','GBP','HKD','CHF','AUD','CAD','SGD')
                                     AND T1.POSITIONTYPE = '01' THEN '10'                                                                                                                              --非以上币种，且头寸属性为多头(01),则映射为其他币种多头(10)
                            WHEN T1.CURRENCY NOT IN ('CNY','USD','EUR','JPY','GBP','HKD','CHF','AUD','CAD','SGD')
                                     AND T1.POSITIONTYPE = '02' THEN '11'                                                                                                                              --非以上币种，且头寸属性为空头(02),则映射为其他币种空头(11)
                            ELSE '12'                                                                                                                                                                                             --黄金(12)
                   END                                        AS EXCHANGERISKTYPE         --外汇风险类型                                 如果币种<>人民币，则需根据币种映射；人民币不需映射
                ,''                                           AS COMMODITYNAME            --商品种类名称                                 默认：空
                ,''                                           AS OPTIONRISKTYPE           --期权风险类型                                 默认：空
                ,''                                           AS ISSUERID                 --发行人ID                                     默认：空
                ,''                                           AS ISSUERNAME               --发行人名称                                   默认：空
                ,''                                           AS ISSUERTYPE               --发行人大类                                   默认：空
                ,''                                           AS ISSUERSUBTYPE            --发行人小类                                   默认：空
                ,''                                           AS ISSUERREGISTSTATE        --发行人注册国家                               默认：空
                ,''                                           AS ISSUERRCERATING          --发行人境外注册地外部评级                     默认：空
                ,''                                           AS SMBFLAG                  --小微企业标识                                 默认：空
                ,''                                           AS UNDERBONDFLAG            --是否承销债券                                 默认：空
                ,''                                           AS PAYMENTDATE              --缴款日                                       默认：空
                ,''                                           AS SECURITIESTYPE           --证券类别                                     默认：空
                ,''                                           AS BONDISSUEINTENT          --债券发行目的                                 默认：空
                ,''                                           AS CLAIMSLEVEL              --债权级别                                     默认：空
                ,''                                           AS REABSFLAG                --再资产证券化标识                             默认：空
                ,''                                           AS ORIGINATORFLAG           --是否发起机构                                 默认：空
                ,''                                           AS SECURITIESERATING        --证券外部评级                                 默认：空
                ,''                                           AS STOCKCODE                --股票/股指代码                                默认：空
                ,''                                           AS STOCKMARKET              --交易市场                                     默认：空
                ,''                                           AS EXCHANGEAREA             --交易地区                                     默认：空
                ,T1.STRUCTURALEXPOFLAG                        AS STRUCTURALEXPOFLAG       --是否结构性敞口
                ,'0'                                          AS OPTIONUNDERLYINGFLAG     --是否期权基础工具                             默认：否(0)
                ,''                                           AS OPTIONUNDERLYINGTYPE     --期权基础工具类型                             默认：空
                ,''                                           AS OPTIONID                 --期权工具ID                                   默认：空
                ,NULL                                         AS VOLATILITY               --波动率                                       默认：空
                ,''                                           AS STARTDATE                --起始日期                                     默认：空
                ,''                                           AS DUEDATE                  --到期日期                                     默认：空
                ,0                                            AS ORIGINALMATURITY         --原始期限                                     默认：空
                ,0                                            AS RESIDUALM                --剩余期限                                     默认：空
                ,''                                           AS NEXTREPRICEDATE          --下次重定价日                                 默认：空
                ,NULL                                         AS NEXTREPRICEM             --下次重定价期限                               默认：空
                ,''                                           AS RATETYPE                 --利率类型                                     默认：空
                ,NULL                                         AS COUPONRATE               --票面利率                                     默认：空
                ,''                                           AS MODIFIEDDURATION         --修正久期                                     默认：空
                ,T1.POSITIONTYPE                              AS POSITIONTYPE             --头寸属性
                ,ABS(T1.POSITION)                             AS POSITION                 --头寸
                ,T1.CURRENCY                                  AS CURRENCY                 --币种
                ,''																		 			  AS OPTIONUNDERLYINGNAME		 --期权基础工具名称
                ,T3.SORTNO														 			  AS ORGSORTNO								 --机构排序号

    FROM				RWA_DEV.RWA_WH_FESPOTPOSITION T1	             		 					--外汇现货头寸信息表
    LEFT	JOIN	RWA.ORG_INFO T3
    ON					T1.ACCORGID = T3.ORGID
	  WHERE				T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T1.CURRENCY <> 'CNY'  --根据校验规则，将人民币排除
    AND					T1.POSITION <> 0
	  ;

    COMMIT;

    --2.2 OPICS衍生品系统-外汇掉期\远期\即期 以及结构性敞口
    INSERT INTO RWA_DEV.RWA_WH_MARKETEXPOSURESTD(
                 DATADATE                              --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT                       --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME									 --期权基础工具名称
                ,ORGSORTNO														 --机构排序号

    )
    SELECT   T1.DATADATE                                 AS DATADATE                 --数据日期
            ,TO_CHAR(T1.DATADATE,'yyyyMMdd')             AS DATANO                   --数据流水号
            ,CASE WHEN T1.FLAG='1' AND T1.CURRENCY='CNY'
                  THEN 'LLYBFX'||T1.TRANID||'B'||T1.CURRENCY
                  WHEN T1.FLAG='2' AND T1.CURRENCY='CNY'
                  THEN 'LLYBFX'||T1.TRANID||'S'||T1.CURRENCY
                  WHEN T1.FLAG='1' AND T1.CURRENCY<>'CNY'
                  THEN 'WHFX'||T1.TRANID||'B'||T1.CURRENCY
                  WHEN T1.FLAG='2' AND T1.CURRENCY<>'CNY'
                  THEN 'WHFX'||T1.TRANID||'S'||T1.CURRENCY
                  ELSE '' END                            AS EXPOSUREID               --风险暴露ID
            ,T1.BOOKTYPE                                 AS BOOKTYPE                 --账户类别  默认：交易账户(02)
            ,T1.TRANID                                   AS INSTRUMENTSID            --金融工具ID
            ,T1.INSTRUMENTSTYPE                          AS INSTRUMENTSTYPE          --金融工具类型
            ,T1.TRANORGID                                AS ORGID                    --所属机构ID
            ,T2.ORGNAME	                                 AS ORGNAME                  --所属机构名称
            ,'01'                                        AS ORGTYPE                  --所属机构类型            默认：境内机构(01)
            ,DECODE(T1.CURRENCY,'CNY','01','03')         AS MARKETRISKTYPE           --市场风险类型            CNY 为 01 利率风险 其他为 03 外汇风险
            ,DECODE(T1.CURRENCY,'CNY','02','')           AS INTERATERISKTYPE         --利率风险类型            CNY 为 02 利率 其他为 空
            ,''                                          AS EQUITYRISKTYPE           --股票风险类型            默认：空
            ,CASE WHEN ACCSUBJECTS like '143101%'
                  THEN '12'--黄金
                  WHEN CURRENCY='CNY'
                  THEN ''
                  WHEN CURRENCY='USD'
                  THEN '01'--美元
                  WHEN CURRENCY='EUR'
                  THEN '02'--欧元
                  WHEN CURRENCY='JPY'
                  THEN '03'--日元
                  WHEN CURRENCY='GBP'
                  THEN '04'--英镑
                  WHEN CURRENCY='HKD'
                  THEN '05'--港元
                  WHEN CURRENCY='CHF'
                  THEN '06'--瑞士法郎
                  WHEN CURRENCY='AUD'
                  THEN '07'--澳大利亚元
                  WHEN CURRENCY='CAD'
                  THEN '08'--加拿大元
                  WHEN CURRENCY='SGD'
                  THEN '09'--新加坡元
                  WHEN FLAG='1'
                  THEN '10'--其他币种多头
                  --其他币种空头
                  ELSE '11' END                          AS EXCHANGERISKTYPE         --外汇风险类型
            ,''                                          AS COMMODITYNAME            --商品种类名称            默认：空
            ,''                                          AS OPTIONRISKTYPE           --期权风险类型            默认：空
            ,''                                          AS ISSUERID                 --发行人ID                默认：空
            ,''                                          AS ISSUERNAME               --发行人名称              默认：空
            ,''                                          AS ISSUERTYPE               --发行人大类              默认：空
            ,''                                          AS ISSUERSUBTYPE            --发行人小类              默认：空
            ,''                                          AS ISSUERREGISTSTATE        --发行人注册国家          默认：空
            ,''                                          AS ISSUERRCERATING          --发行人境外注册地外部评级默认：空
            ,''                                          AS SMBFLAG                  --小微企业标识            默认：空
            ,'0'                                         AS UNDERBONDFLAG            --是否承销债券            默认：否(0)
            ,''                                          AS PAYMENTDATE              --缴款日                  默认：空
            ,''                                          AS SECURITIESTYPE           --证券类别                默认：空
            ,''                                          AS BONDISSUEINTENT          --债券发行目的            默认：空
            ,''                                          AS CLAIMSLEVEL              --债权级别                默认：空
            ,''                                          AS REABSFLAG                --再资产证券化标识        默认：空
            ,''                                          AS ORIGINATORFLAG           --是否发起机构            默认：空
            ,''                                          AS SECURITIESERATING        --证券外部评级            默认：空
            ,''                                          AS STOCKCODE                --股票/股指代码           默认：空
            ,''                                          AS STOCKMARKET              --交易市场                默认：空
            ,''                                          AS EXCHANGEAREA             --交易地区                默认：空
            ,T1.STRUCTURALEXPOFLAG                       AS STRUCTURALEXPOFLAG       --是否结构性敞口
            ,'0'                                         AS OPTIONUNDERLYINGFLAG     --是否期权基础工具        默认：否(0)
            ,''                                          AS OPTIONUNDERLYINGTYPE     --期权基础工具类型        默认：空
            ,''                                          AS OPTIONID                 --期权工具ID              默认：空
            ,''                                          AS VOLATILITY               --波动率                  默认：空
            ,T1.STARTDATE                                AS STARTDATE                --起始日期
            ,T1.DUEDATE                                  AS DUEDATE                  --到期日期
            ,T1.ORIGINALMATURITY                         AS ORIGINALMATURITY         --原始期限
            ,T1.RESIDUALM                                AS RESIDUALM                --剩余期限
            ,''                                          AS NEXTREPRICEDATE          --下次重定价日            默认：空
            ,''                                          AS NEXTREPRICEM             --下次重定价期限          默认：空
            ,DECODE(T1.CURRENCY,'CNY','01','')           AS RATETYPE                 --利率类型                CNY 为 01 固定利率 其他为 空
            ,''                                          AS COUPONRATE               --票面利率                默认：空
            ,''                                          AS MODIFIEDDURATION         --修正久期                默认：空
            ,DECODE(T1.FLAG,'1','01','02')               AS POSITIONTYPE             --头寸属性                默认：买入 为 多头 01 卖出 为 空头 02
            ,T1.POSITION                                 AS POSITION                 --头寸
            ,T1.CURRENCY                                 AS CURRENCY                 --币种
            ,''																		 			 AS OPTIONUNDERLYINGNAME		 --期权基础工具名称
            ,T2.SORTNO														 		 	 AS ORGSORTNO								 --机构排序号
    FROM  (
            --买入交易
            SELECT  '1'                      AS FLAG                  --买入卖出标志：1 买入 2 卖出
                   ,DATADATE                 AS DATADATE              --数据日期
                   ,TRANID                   AS TRANID                --交易ID
                   ,BUYCURRENCY              AS CURRENCY              --币种
                   ,BOOKTYPE                 AS BOOKTYPE              --账户类别
                   ,INSTRUMENTSTYPE          AS INSTRUMENTSTYPE       --金融工具类型
                   ,TRANORGID                AS TRANORGID             --交易机构ID
                   ,ACCSUBJECTS              AS ACCSUBJECTS           --会计科目
                   ,STRUCTURALEXPOFLAG       AS STRUCTURALEXPOFLAG    --是否结构性敞口
                   ,STARTDATE                AS STARTDATE             --起始日期
                   ,DUEDATE                  AS DUEDATE               --到期日期
                   ,ORIGINALMATURITY         AS ORIGINALMATURITY      --原始期限
                   ,RESIDUALM                AS RESIDUALM             --剩余期限
                   ,ABS(BUYAMOUNT)           AS POSITION              --买入金额
            FROM RWA_DEV.RWA_WH_FEFORWARDSSWAP
           WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
           	 AND BUYAMOUNT > 0
            UNION ALL
            --卖出交易
            SELECT  '2'                      AS FLAG                  --买入卖出标志：1 买入 2 卖出
                   ,DATADATE                 AS DATADATE              --数据日期
                   ,TRANID                   AS TRANID                --交易ID
                   ,SELLCURRENCY             AS CURRENCY              --币种
                   ,BOOKTYPE                 AS BOOKTYPE              --账户类别
                   ,INSTRUMENTSTYPE          AS INSTRUMENTSTYPE       --金融工具类型
                   ,TRANORGID                AS TRANORGID             --交易机构ID
                   ,ACCSUBJECTS              AS ACCSUBJECTS           --会计科目
                   ,STRUCTURALEXPOFLAG       AS STRUCTURALEXPOFLAG    --是否结构性敞口
                   ,STARTDATE                AS STARTDATE             --起始日期
                   ,DUEDATE                  AS DUEDATE               --到期日期
                   ,ORIGINALMATURITY         AS ORIGINALMATURITY      --原始期限
                   ,RESIDUALM                AS RESIDUALM             --剩余期限
                   ,ABS(SELLAMOUNT)          AS POSITION              --卖出金额
            FROM RWA_DEV.RWA_WH_FEFORWARDSSWAP
           WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
           	 AND SELLAMOUNT > 0
           ) T1
    LEFT JOIN RWA.ORG_INFO T2
    ON T1.TRANORGID = T2.ORGID
	;
    COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_WH_MARKETEXPOSURESTD',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_WH_MARKETEXPOSURESTD;
    --Dbms_output.Put_line('RWA_DEV.RWA_WH_MARKETEXPOSURESTD表当前插入的国结系统-外汇(市场风险)-标准法暴露记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
      p_po_rtnmsg  := '成功' || '-' || v_count;
        --定义异常
        EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := '市场风险标准法暴露信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WH_MARKETEXPOSURESTD;
/

