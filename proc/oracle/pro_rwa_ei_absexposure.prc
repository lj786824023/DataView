CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_ABSEXPOSURE(
                                                p_data_dt_str IN VARCHAR2,    --数据日期
                                                p_po_rtncode OUT VARCHAR2,    --返回编号 1 成功,0 失败
                                                p_po_rtnmsg OUT  VARCHAR2    --返回描述
)
  /*
    存储过程名称:PRO_RWA_EI_ABSEXPOSURE
    实现功能:风险暴露汇总表,插入风险暴露信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-11
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_ABS_INVEST_ABSEXPOSURE|资产证券化-投资机构风险暴露表
    源  表2 :RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE|资产证券化-发行机构风险暴露表
    目标表  :RWA_DEV.RWA_EI_ABSEXPOSURE|风险暴露汇总表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_ABSEXPOSURE';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;
  --聚利产品余额
  v_ye varchar2(20);

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ABSEXPOSURE DROP PARTITION ABSEXPOSURE' || p_data_dt_str;

    COMMIT;


/*处理补录聚力产品的余额 */

delete from rwa.rwa_ws_abs_bl
t1 where t1.datadate=TO_DATE(p_data_dt_str, 'YYYYMMDD')
and t1.suppserialno IN (select  t2.suppserialno
    from rwa.rwa_ws_abs7_bl t2
   where t2.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'));
   
   commit;

insert into rwa.rwa_ws_abs_bl
  select *
    from rwa.rwa_ws_abs7_bl t
   where t.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

commit;

select count(*)
  into v_count
  from rwa.rwa_ws_abs_bl
 where dklx = '个人贷款'
   and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
IF V_COUNT = 1 THEN
  select ye / 2
    into v_ye
    from rwa.RWA_WS_ABS_BL
   where dklx = '个人贷款'
     and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
      ELSE v_ye:=0;
END IF;

/*update rwa.RWA_WS_ABS_BL b
   set b.ye =
       (b.ye + v_ye)
 where b.dklx = '个人住房按揭贷款'
   and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

update rwa.RWA_WS_ABS_BL b
   set b.ye =
       (b.ye + v_ye)
 where b.dklx = '公司贷款'
   and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

update rwa.RWA_WS_ABS_BL b
   set b.ye = '0'
 where b.dklx = '个人贷款'
   and DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

commit;*/

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总资产证券化暴露信息表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ABSEXPOSURE ADD PARTITION ABSEXPOSURE' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入资产证券化-投资机构的风险暴露信息*/
    INSERT INTO RWA_DEV.RWA_EI_ABSEXPOSURE(
               DataDate                                 --数据日期
              ,DataNo                                   --数据流水号
              ,ABSExposureID                            --资产证券化风险暴露ID
              ,ABSPoolID                                --证券化资产池ID
              ,ABSOriginatorID                          --证券化发起人ID
              ,OrgID                                    --所属机构ID
              ,OrgName                                  --所属机构名称
              ,Businessline                             --条线
              ,AssetType                                --资产大类
              ,AssetSubType                             --资产小类
              ,ExpoCategoryIRB                          --内评法暴露类别
              ,ExpoBelong                               --暴露所属标识
              ,BookType                                 --账户类别
              ,AssetTypeOfHaircuts                      --折扣系数对应资产类别
              ,ABSRole                                  --资产证券化角色
              ,ProvideCRMFSPEFlag                       --是否提供信用风险缓释给特别目的机构
              ,ProvideCSRERFlag                         --是否提供信用支持并反映到外部评级
              ,ReABSFlag                                --再资产证券化标识
              ,RetailFlag                               --零售标识
              ,QualFaciFlag                             --合格便利标识
              ,UncondCancelFlag                         --是否可随时无条件撤销
              ,TrenchSN                                 --分档顺序号
              ,TrenchName                               --档次名称
              ,TopTrenchFlag                            --是否最高档次
              ,PreferedTrenchFlag                       --是否最优先档次
              ,ReguTranType                             --监管交易类型
              ,RevaFrequency                            --重估频率
              ,MitiProvMERating                         --缓释提供者缓释时外部评级
              ,MitiProvCERating                         --缓释提供者当前外部评级
              ,RatingDurationType                       --评级期限类型
              ,ERatingResult                            --外部评级结果
              ,InferRatingResult                        --推测评级结果
              ,IssueDate                                --发行日期
              ,DueDate                                  --到期日期
              ,OriginalMaturity                         --原始期限
              ,ResidualM                                --剩余期限
              ,AssetBalance                             --资产余额
              ,Currency                                 --币种
              ,Provisions                               --减值准备
              ,L                                        --档次信用增级水平
              ,T                                        --档次厚度
              ,EarlyAmortType                           --提前摊还类型
              ,RetailCommitType                         --零售承诺类型
              ,Investor                                 --投资者权益
              ,AverIGOnThreeMths                        --三个月平均超额利差
              ,IntGapStopPoint                          --超额利差锁定点
              ,R                                        --三个月平均超额利差/锁定点
              ,OFFABSBUSINESSTYPE                       --表外资产证券化类型
              ,ORGSORTNO                                --所属机构排序号
              ,ISSUSERASSETPROP                         --发行机构基础资产占比
)
     SELECT
                 DataDate                                                AS DataDate             --数据日期
                ,DataNo                                                  AS DataNo               --数据流水号
                ,ABSExposureID                                           AS ABSExposureID        --资产证券化风险暴露ID
                ,ABSPoolID                                               AS ABSPoolID            --证券化资产池ID
                ,ABSOriginatorID                                         AS ABSOriginatorID      --证券化发起人ID
                ,OrgID                                                   AS OrgID                --所属机构ID
                ,OrgName                                                 AS OrgName              --所属机构名称
                ,Businessline                                            AS Businessline         --条线
                ,AssetType                                               AS AssetType            --资产大类
                ,AssetSubType                                            AS AssetSubType         --资产小类
                ,ExpoCategoryIRB                                         AS ExpoCategoryIRB      --内评法暴露类别
                ,ExpoBelong                                              AS ExpoBelong           --暴露所属标识
                ,BookType                                                AS BookType             --账户类别                           ?
                ,AssetTypeOfHaircuts                                     AS AssetTypeOfHaircuts  --折扣系数对应资产类别
                ,ABSRole                                                 AS ABSRole              --资产证券化角色
                ,ProvideCRMFSPEFlag                                      AS ProvideCRMFSPEFlag   --是否提供信用风险缓释给特别目的机构
                ,ProvideCSRERFlag                                        AS ProvideCSRERFlag     --是否提供信用支持并反映到外部评级
                ,ReABSFlag                                               AS ReABSFlag            --再资产证券化标识
                ,RetailFlag                                              AS RetailFlag           --零售标识
                ,QualFaciFlag                                            AS QualFaciFlag         --合格便利标识
                ,UncondCancelFlag                                        AS UncondCancelFlag     --是否可随时无条件撤销
                ,TrenchSN                                                AS TrenchSN             --分档顺序号
                ,TrenchName                                              AS TrenchName           --档次名称
                ,TopTrenchFlag                                           AS TopTrenchFlag        --是否最高档次
                ,PreferedTrenchFlag                                      AS PreferedTrenchFlag   --是否最优先档次
                ,ReguTranType                                            AS ReguTranType         --监管交易类型
                ,RevaFrequency                                           AS RevaFrequency        --重估频率
                ,MitiProvMERating                                        AS MitiProvMERating     --缓释提供者缓释时外部评级
                ,MitiProvCERating                                        AS MitiProvCERating     --缓释提供者当前外部评级
                ,RatingDurationType                                      AS RatingDurationType   --评级期限类型
                ,ERatingResult                                           AS ERatingResult        --外部评级结果
                ,InferRatingResult                                       AS InferRatingResult    --推测评级结果
                ,TO_CHAR(TO_DATE(IssueDate,'YYYY-MM-DD'),'YYYY-MM-DD')   AS IssueDate            --发行日期
                ,TO_CHAR(TO_DATE(DueDate,'YYYY-MM-DD'),'YYYY-MM-DD')     AS DueDate              --到期日期
                ,OriginalMaturity                                        AS OriginalMaturity     --原始期限
                ,ResidualM                                               AS ResidualM            --剩余期限
                ,AssetBalance                                            AS AssetBalance         --资产余额
                ,Currency                                                AS Currency             --币种
                ,Provisions                                              AS Provisions           --减值准备
                ,L                                                       AS L                    --档次信用增级水平
                ,T                                                       AS T                    --档次厚度
                ,EarlyAmortType                                          AS EarlyAmortType       --提前摊还类型
                ,RetailCommitType                                        AS RetailCommitType     --零售承诺类型
                ,Investor                                                AS Investor             --投资者权益
                ,AverIGOnThreeMths                                       AS AverIGOnThreeMths    --三个月平均超额利差
                ,IntGapStopPoint                                         AS IntGapStopPoint      --超额利差锁定点
                ,R                                                       AS R                    --三个月平均超额利差/锁定点
                ,OFFABSBUSINESSTYPE                                      AS OFFABSBUSINESSTYPE   --表外资产证券化类型
                ,ORGSORTNO                                               AS ORGSORTNO            --所属机构排序号
                ,null                                                    AS ISSUSERASSETPROP     --发行机构基础资产占比
    FROM 				RWA_DEV.RWA_ABS_INVEST_ABSEXPOSURE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入资产证券化-发行机构的风险暴露信息*/
    INSERT INTO RWA_DEV.RWA_EI_ABSEXPOSURE(
               DataDate                                 --数据日期
              ,DataNo                                   --数据流水号
              ,ABSExposureID                            --资产证券化风险暴露ID
              ,ABSPoolID                                --证券化资产池ID
              ,ABSOriginatorID                          --证券化发起人ID
              ,OrgID                                    --所属机构ID
              ,OrgName                                  --所属机构名称
              ,Businessline                             --条线
              ,AssetType                                --资产大类
              ,AssetSubType                             --资产小类
              ,ExpoCategoryIRB                          --内评法暴露类别
              ,ExpoBelong                               --暴露所属标识
              ,BookType                                 --账户类别
              ,AssetTypeOfHaircuts                      --折扣系数对应资产类别
              ,ABSRole                                  --资产证券化角色
              ,ProvideCRMFSPEFlag                       --是否提供信用风险缓释给特别目的机构
              ,ProvideCSRERFlag                         --是否提供信用支持并反映到外部评级
              ,ReABSFlag                                --再资产证券化标识
              ,RetailFlag                               --零售标识
              ,QualFaciFlag                             --合格便利标识
              ,UncondCancelFlag                         --是否可随时无条件撤销
              ,TrenchSN                                 --分档顺序号
              ,TrenchName                               --档次名称
              ,TopTrenchFlag                            --是否最高档次
              ,PreferedTrenchFlag                       --是否最优先档次
              ,ReguTranType                             --监管交易类型
              ,RevaFrequency                            --重估频率
              ,MitiProvMERating                         --缓释提供者缓释时外部评级
              ,MitiProvCERating                         --缓释提供者当前外部评级
              ,RatingDurationType                       --评级期限类型
              ,ERatingResult                            --外部评级结果
              ,InferRatingResult                        --推测评级结果
              ,IssueDate                                --发行日期
              ,DueDate                                  --到期日期
              ,OriginalMaturity                         --原始期限
              ,ResidualM                                --剩余期限
              ,AssetBalance                             --资产余额
              ,Currency                                 --币种
              ,Provisions                               --减值准备
              ,L                                        --档次信用增级水平
              ,T                                        --档次厚度
              ,EarlyAmortType                           --提前摊还类型
              ,RetailCommitType                         --零售承诺类型
              ,Investor                                 --投资者权益
              ,AverIGOnThreeMths                        --三个月平均超额利差
              ,IntGapStopPoint                          --超额利差锁定点
              ,R                                        --三个月平均超额利差/锁定点
              ,OFFABSBUSINESSTYPE                       --表外资产证券化类型
              ,ORGSORTNO                                --所属机构排序号
              ,ISSUSERASSETPROP                         --发行机构基础资产占比
)
     SELECT
                 DataDate                                                AS DataDate             --数据日期
                ,DataNo                                                  AS DataNo               --数据流水号
                ,ABSExposureID                                           AS ABSExposureID        --资产证券化风险暴露ID
                ,ABSPoolID                                               AS ABSPoolID            --证券化资产池ID
                ,ABSOriginatorID                                         AS ABSOriginatorID      --证券化发起人ID
                ,OrgID                                                   AS OrgID                --所属机构ID
                ,OrgName                                                 AS OrgName              --所属机构名称
                ,Businessline                                            AS Businessline         --条线
                ,AssetType                                               AS AssetType            --资产大类
                ,AssetSubType                                            AS AssetSubType         --资产小类
                ,ExpoCategoryIRB                                         AS ExpoCategoryIRB      --内评法暴露类别
                ,ExpoBelong                                              AS ExpoBelong           --暴露所属标识
                ,BookType                                                AS BookType             --账户类别                           ?
                ,AssetTypeOfHaircuts                                     AS AssetTypeOfHaircuts  --折扣系数对应资产类别
                ,ABSRole                                                 AS ABSRole              --资产证券化角色
                ,ProvideCRMFSPEFlag                                      AS ProvideCRMFSPEFlag   --是否提供信用风险缓释给特别目的机构
                ,ProvideCSRERFlag                                        AS ProvideCSRERFlag     --是否提供信用支持并反映到外部评级
                ,ReABSFlag                                               AS ReABSFlag            --再资产证券化标识
                ,RetailFlag                                              AS RetailFlag           --零售标识
                ,QualFaciFlag                                            AS QualFaciFlag         --合格便利标识
                ,UncondCancelFlag                                        AS UncondCancelFlag     --是否可随时无条件撤销
                ,TrenchSN                                                AS TrenchSN             --分档顺序号
                ,TrenchName                                              AS TrenchName           --档次名称
                ,TopTrenchFlag                                           AS TopTrenchFlag        --是否最高档次
                ,PreferedTrenchFlag                                      AS PreferedTrenchFlag   --是否最优先档次
                ,ReguTranType                                            AS ReguTranType         --监管交易类型
                ,RevaFrequency                                           AS RevaFrequency        --重估频率
                ,MitiProvMERating                                        AS MitiProvMERating     --缓释提供者缓释时外部评级
                ,MitiProvCERating                                        AS MitiProvCERating     --缓释提供者当前外部评级
                ,RatingDurationType                                      AS RatingDurationType   --评级期限类型
                ,ERatingResult                                           AS ERatingResult        --外部评级结果
                ,InferRatingResult                                       AS InferRatingResult    --推测评级结果
                ,TO_CHAR(TO_DATE(IssueDate,'YYYY-MM-DD'),'YYYY-MM-DD')   AS IssueDate            --发行日期
                ,TO_CHAR(TO_DATE(DueDate,'YYYY-MM-DD'),'YYYY-MM-DD')     AS DueDate              --到期日期
                ,OriginalMaturity                                        AS OriginalMaturity     --原始期限
                ,ResidualM                                               AS ResidualM            --剩余期限
                ,AssetBalance                                            AS AssetBalance         --资产余额
                ,Currency                                                AS Currency             --币种
                ,Provisions                                              AS Provisions           --减值准备
                ,L                                                       AS L                    --档次信用增级水平
                ,T                                                       AS T                    --档次厚度
                ,EarlyAmortType                                          AS EarlyAmortType       --提前摊还类型
                ,RetailCommitType                                        AS RetailCommitType     --零售承诺类型
                ,Investor                                                AS Investor             --投资者权益
                ,AverIGOnThreeMths                                       AS AverIGOnThreeMths    --三个月平均超额利差
                ,IntGapStopPoint                                         AS IntGapStopPoint      --超额利差锁定点
                ,R                                                       AS R                    --三个月平均超额利差/锁定点
                ,OFFABSBUSINESSTYPE                                      AS OFFABSBUSINESSTYPE   --表外资产证券化类型
                ,ORGSORTNO                                               AS ORGSORTNO            --所属机构排序号
                ,ISSUSERASSETPROP                                        AS ISSUSERASSETPROP     --发行机构基础资产占比
    FROM 				RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;



--聚利产品补录

INSERT INTO 
RWA_EI_ABSEXPOSURE -- 资产证券化风险暴露表
(DATADATE         --数据日期
,DATANO         --数据流水号
,ABSEXPOSUREID         --资产证券化风险暴露ID
,ABSPOOLID         --证券化资产池ID
,ABSORIGINATORID         --证券化发起人ID
,ORGSORTNO         --机构流水号
,ORGID         --所属机构ID
,ORGNAME         --所属机构名称
,BUSINESSLINE         --条线
,ASSETTYPE         --资产大类
,ASSETSUBTYPE         --资产小类
,EXPOCATEGORYIRB         --内评法暴露类别
,EXPOBELONG         --暴露所属标识
,BOOKTYPE         --账户类别
,ASSETTYPEOFHAIRCUTS         --折扣系数对应资产类别
,ABSROLE         --资产证券化角色
,PROVIDECRMFSPEFLAG         --是否提供信用风险缓释给特别目的机构
,PROVIDECSRERFLAG         --是否提供信用支持并反映到外部评级
,REABSFLAG         --再资产证券化标识
,RETAILFLAG         --零售标识
,QUALFACIFLAG         --合格便利标识
,UNCONDCANCELFLAG         --是否可随时无条件撤销
,TRENCHSN         --分档顺序号
,TRENCHNAME         --档次名称
,TOPTRENCHFLAG         --是否最高档次
,PREFEREDTRENCHFLAG         --是否最优先档次
,REGUTRANTYPE         --监管交易类型
,REVAFREQUENCY         --重估频率
,MITIPROVMERATING         --缓释提供者缓释时外部评级
,MITIPROVCERATING         --缓释提供者当前外部评级
,RATINGDURATIONTYPE         --评级期限类型
,ERATINGRESULT         --外部评级结果
,INFERRATINGRESULT         --推测评级结果
,ISSUEDATE         --发行日期
,DUEDATE         --到期日期
,ORIGINALMATURITY         --原始期限
,RESIDUALM         --剩余期限
,ASSETBALANCE         --资产余额
,CURRENCY         --币种
,PROVISIONS         --减值准备
,L         --档次信用增级水平
,T         --档次厚度
,EARLYAMORTTYPE         --提前摊还类型
,RETAILCOMMITTYPE         --零售承诺类型
,INVESTOR         --投资者权益
,AVERIGONTHREEMTHS         --三个月平均超额利差
,INTGAPSTOPPOINT         --超额利差锁定点
,R         --三个月平均超额利差/锁定点
,OFFABSBUSINESSTYPE         --表外资产证券化业务类型
,ISSUSERASSETPROP         --发行机构基础资产占比
)
SELECT 
  TO_DATE(p_data_dt_str,'YYYYMMDD')   AS DATADATE  -- 数据日期
 ,p_data_dt_str                  AS DATANO    ---数据流水号
 ,CASE WHEN T1.DKLX='个人住房按揭贷款' THEN 'B201712285095'
 WHEN  T1.DKLX='个人贷款' THEN 'B201803296435A'
 WHEN  T1.DKLX='公司贷款' THEN 'B201803296435B'
 END, --资产证券化风险暴露ID
 CASE WHEN T1.DKLX='个人住房按揭贷款' THEN 'B201712285095'
 WHEN  T1.DKLX='个人贷款' THEN 'B201803296435A'
 WHEN  T1.DKLX='公司贷款' THEN 'B201803296435B'
 END, --证券化资产池ID
 '重庆银行股份有限公司' --证券化发起人ID
 ,'1'         --机构流水号
,'9998'         --所属机构ID
,'重庆银行股份有限公司'         --所属机构名称
,'0501'         --条线
,'310'         --资产大类
,'31001'         --资产小类
,'020601'         --内评法暴露类别
,'01'         --暴露所属标识
,'01'         --账户类别
,'01'         --折扣系数对应资产类别
,''         --资产证券化角色
,''         --是否提供信用风险缓释给特别目的机构
,''         --是否提供信用支持并反映到外部评级
,'0'         --再资产证券化标识
,'0'         --零售标识
,''         --合格便利标识
,''         --是否可随时无条件撤销
,''         --分档顺序号
,''         --档次名称
,''         --是否最高档次
,''         --是否最优先档次
,'02'         --监管交易类型
,'01'         --重估频率
,''         --缓释提供者缓释时外部评级
,''         --缓释提供者当前外部评级
,''         --评级期限类型
,CASE WHEN T1.DKLX='个人住房按揭贷款' THEN '0106' --50%
 WHEN  T1.DKLX='个人贷款' THEN '0109' --100%
 WHEN  T1.DKLX='公司贷款' THEN '0109' --100%
 END --资产证券化风险暴露ID         --外部评级结果
,''         --推测评级结果
,CASE WHEN T1.ZCZQHMC='聚利6号' THEN '2018-03-29'
 ELSE '2017-12-28' END     --发行日期
,CASE WHEN T1.ZCZQHMC='聚利6号' THEN '2027-09-28'
 ELSE '2021-03-29' END          --到期日期
,CASE WHEN T1.ZCZQHMC='聚利6号' THEN '9.756164384'
 ELSE '3.002739726' END         --原始期限
,CASE WHEN T1.ZCZQHMC='聚利6号' THEN  ( CASE WHEN (TO_DATE(20270928,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                    THEN 0
                    ELSE (TO_DATE(20270928,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
              END )
 ELSE ( CASE WHEN (TO_DATE(20210329,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                    THEN 0
                    ELSE (TO_DATE(20210329,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
              END ) END   --剩余期限
, 

 CASE WHEN T1.DKLX='个人住房按揭贷款' THEN T1.YE+v_ye
 WHEN  T1.DKLX='个人贷款' THEN 0
 WHEN  T1.DKLX='公司贷款' THEN T1.YE+v_ye
 END
 --资产余额
,'CNY'         --币种
,'0'         --减值准备
,''         --档次信用增级水平
,''         --档次厚度
,''         --提前摊还类型
,''         --零售承诺类型
,''         --投资者权益
,''         --三个月平均超额利差
,''         --超额利差锁定点
,''         --三个月平均超额利差/锁定点
,''         --表外资产证券化业务类型
,''         --发行机构基础资产占比


FROM RWA.RWA_WS_ABS_BL T1
WHERE T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD');
COMMIT;

/*减值*/

UPDATE RWA_EI_ABSEXPOSURE T
   SET T.PROVISIONS =
       (SELECT I9.FINAL_ECL
          FROM SYS_IFRS9_RESULT I9
         WHERE I9.CONTRACT_REFERENCE = T.ABSEXPOSUREID
           AND I9.DATANO = p_data_dt_str)
 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
   and exists (SELECT 1
          FROM SYS_IFRS9_RESULT I9
         WHERE I9.CONTRACT_REFERENCE = T.ABSEXPOSUREID
           AND I9.DATANO = p_data_dt_str);
COMMIT;

    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ABSEXPOSURE',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ABSEXPOSURE',partname => 'ABSEXPOSURE'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_ABSEXPOSURE WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_EI_ABSEXPOSURE表当前插入的数据记录为:' || v_count1 || '条');

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '资产证券化的风险暴露表(RWA_DEV.PRO_RWA_EI_ABSEXPOSURE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_ABSEXPOSURE;
/

