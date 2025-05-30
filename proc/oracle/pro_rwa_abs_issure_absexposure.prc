CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_ABSEXPOSURE(p_data_dt_str  IN  VARCHAR2, --数据日期
                                                           p_po_rtncode   OUT VARCHAR2, --返回编号
                                                           p_po_rtnmsg    OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ABS_ISSURE_ABSEXPOSURE
    实现功能:将相关信息全量导入RWA接口表资产证券化发行机构风险暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-06-23
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA.RWA_WS_ABS_ISSUE_EXPOSURE|资产证券化-发行机构-风险暴露补录表
    源  表2 :RWA.RWA_WS_ABS_ISSUE_POOL|资产证券化-发行机构-合约与池补录表
    源  表3 :RWA.ORG_INFO|机构表
    源  表4 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表1 :RWA_ABS_ISSURE_ABSEXPOSURE|发行机构-资产证券化风险暴露表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_ABSEXPOSURE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE';


    --DBMS_OUTPUT.PUT_LINE('开始：导入【资产证券化发行机构风险暴露表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE(
		             DataDate            --数据日期
		            ,DataNo              --数据流水号
		            ,ABSExposureID       --资产证券化风险暴露ID
		            ,ABSPoolID           --证券化资产池ID
		            ,ABSOriginatorID     --证券化发起人ID
		            ,OrgSortNo           --所属机构排序号
		            ,OrgID               --所属机构ID
		            ,OrgName             --所属机构名称
		            ,BusinessLine        --业务条线
		            ,AssetType           --资产大类
		            ,AssetSubType        --资产小类
		            ,ExpoCategoryIRB     --内评法暴露类别
		            ,ExpoBelong          --暴露所属标识
		            ,BookType            --账户类别
		            ,AssetTypeOfHaircuts --折扣系数对应资产类别
		            ,ReABSFlag           --再资产证券化标识
		            ,RetailFlag          --零售标识
		            ,ReguTranType        --监管交易类型
		            ,RevaFrequency       --重估频率
		            ,OffABSBusinessType  --表外资产证券化业务类型
		            ,ABSRole             --资产证券化角色
		            ,ProvideCSRERFlag    --是否提供信用支持并反映到外部评级
		            ,ProvideCRMFSPEFlag  --是否提供信用风险缓释给特别目的机构
		            ,MitiProvMERating    --缓释提供者缓释时外部评级
		            ,MitiProvCERating    --缓释提供者当前外部评级
		            ,QualFaciFlag        --合格便利标识
		            ,UncondCancelFlag    --是否可随时无条件撤销
		            ,TrenchSN            --分档顺序号
		            ,TrenchName          --档次名称
		            ,TopTrenchFlag       --是否最高档次
		            ,PreferedTrenchFlag  --是否最优先档次
		            ,RatingDurationType  --评级期限类型
		            ,ERatingResult       --外部评级结果
		            ,InferRatingResult   --推测评级结果
		            ,IssueDate           --发行日期
		            ,DueDate             --到期日期
		            ,OriginalMaturity    --原始期限
		            ,ResidualM           --剩余期限
		            ,AssetBalance        --资产余额
		            ,Currency            --币种
		            ,Provisions          --减值准备
		            ,L                   --档次信用增级水平
		            ,T                   --档次厚度
		            ,EarlyAmortType      --提前摊还类型
		            ,RetailCommitType    --零售承诺类型
		            ,AverIGOnThreeMths   --三个月平均超额利差
		            ,IntGapStopPoint     --超额利差锁定点
		            ,R                   --三个月平均超额利差/锁定点
		            ,ISSUSERASSETPROP    --发行机构基础资产占比
		            ,INVESTOR            --投资者权益
    )
    SELECT
				         RWAIE.DATADATE                                                 AS DATADATE            --数据日期_数据日期
				        ,TO_CHAR(RWAIE.DATADATE,'yyyyMMdd')                             AS DATANO              --数据流水号_数据日期
				        ,'ABS'||RWAIE.SUPPSERIALNO                                      AS ABSEXPOSUREID       --资产证券化风险暴露代号_流水号('BL_ABS'||流水号)
				        ,RWAIE.ZCCBH                                                    AS ABSPOOLID           --证券化资产池代号_资产池代号
				        ,RWAIP.ZQHFQRZZJGDM                                             AS ABSORIGINATORID     --证券化发起人代号_证券化发起人组织机构代码
				        ,OI.SORTNO                                                      AS ORGSORTNO           --所属机构排序号
				        ,RWAIE.YWSSJG                                                   AS ORGID               --所属机构_机构代号
				        ,OI.ORGNAME                                                     AS ORGNAME             --所属机构名称
				        ,RWAIE.TX                                                       AS BUSINESSLINE        --条线_条线
				        ,'310'                                                          AS ASSETTYPE           --资产大类_(默认为资产证券化)
				        ,'31001'                                                        AS ASSETSUBTYPE        --资产小类_(默认为资产证券化)
				        ,CASE WHEN RWAIE.YHJS IN ('01','02') THEN '020601'
				              WHEN RWAIE.YHJS='03' THEN '020602'
				              WHEN RWAIE.YHJS='04' THEN '020603'
				              WHEN RWAIE.YHJS='05' THEN '020604'
				              ELSE '020605'
				         END						                                                AS EXPOCATEGORYIRB     --风险暴露类别_银行角色("码值：ExpoCategoryIRB")
				        ,CASE WHEN RWAIE.YHJS IN ('03','04','05')  OR RWAIE.TQTHLXDM IS NOT NULL THEN '02'
				              ELSE '01'
				         END												                                    AS EXPOBELONG          --暴露所属标识          --暴露所属标识_暴露所属标识：  若表外资产证券化类型为01、02、03、04，则暴露标识为02-表外，否则为01-表内
				        ,CASE WHEN FBI.ASSET_CLASS = '10' THEN '02'
				        			ELSE '01'
				         END												                                    AS BOOKTYPE            --账户类别_账户类别
				        ,'21907'                                                        AS ASSETTYPEOFHAIRCUTS --折扣系数对应资产类型_(默认为其他 资产类型 AssetCategory 中的’21907 其他)
				        ,RWAIE.ZZCZQHBZ                                                 AS REABSFLAG           --再资产证券化标识
				        ,CASE WHEN RWAIP.JCZCYWLX IN ('02','03','04')	THEN '1'																 --02 住房抵押贷款,03 汽车抵押贷款,04 信用卡
				              ELSE '0'
				         END											                                      AS RETAILFLAG          --零售标识
				        ,'02'                                                           AS REGUTRANTYPE        --监管交易类型_(默认为02-其他资本市场交易 码值：ReguTranType)
				        ,1                                                              AS REVAFREQUENCY       --重估频率_(默认为1天)
				        ,CASE WHEN RWAIE.TQTHLXDM IS NOT NULL THEN '03'
				        			WHEN RWAIE.YHJS = '03' THEN '04'
				              WHEN RWAIE.YHJS = '04' THEN '01'
				              WHEN RWAIE.YHJS = '05' THEN '02'
				              ELSE ''
				         END						                                                AS OFFABSBUSINESSTYPE  --表外资产证券化类型
				        ,RWAIE.YHJS                                                     AS ABSROLE             --资产证券化角色_银行角色
				        ,RWAIE.SFTGXYZCBFYDWBPJ                                         AS PROVIDECSRERFLAG    --是否提供信用支持并反映到外部评级_是否提供信用支持并反映到外部评级
				        ,RWAIE.XYFXHSSFTGGTBMDDJG                                       AS PROVIDECRMFSPEFLAG  --是否提供信用风险缓释给特别目的机构_信用风险缓释是否提供给特别目的机构
				        ,CASE WHEN RWAIE.FQSHSTGZWBPJ IS NULL THEN '0124'
				         ELSE RWA_DEV.GETSTANDARDRATING1(RWAIE.FQSHSTGZWBPJ)
				         END                                                            AS MITIPROVMERATING    --提供缓释时外部评级_发起时缓释提供者外部评级
				        ,CASE WHEN RWAIE.DQHSTGZWBPJ IS NULL THEN '0124'
				         ELSE RWA_DEV.GETSTANDARDRATING1(RWAIE.DQHSTGZWBPJ)
				         END                                                            AS MITIPROVCERATING    --当前外部评级_当前缓释提供者外部评级
				        ,CASE WHEN RWAIE.HGLDXBLBZ='1' OR RWAIE.HGXJTZBLBZ='1' THEN '1'
				              ELSE '0'
				         END                                              							AS QUALFACIFLAG        --合格便利标识_"合格流动性便利标识合格现金透支便利标识"("若合格流动性便利标识或合格现金透支便利标识中任意一个为'1-是'，则置为是；否则，置为否。 1 是 0 否")
				        ,RWAIE.XJTZBLSFKSSWTJCX                                         AS UNCONDCANCELFLAG    --是否可随时无条件撤销_现金透支便利是否可随时无条件撤销
				        ,RWAIE.FDSXH                                                    AS TRENCHSN            --分档顺序号_分档顺序号
				        ,RWAIE.DCMC                                                     AS TRENCHNAME          --档次名称_档次名称
				        ,RWAIE.SFZYXDC                                                  AS TOPTRENCHFLAG       --是否最高档次_是否最优先档次
				        ,RWAIE.SFZYXDC                                                  AS PREFEREDTRENCHFLAG  --是否最优先档次_是否最优先档次
				        ,CASE WHEN RWAIE.ZQWBPJDJ IS NULL THEN '01'
				         ELSE NVL(RWAIE.ZQWBPJQX,SUBSTR(RWA_DEV.GETSTANDARDRATING1(RWAIE.ZQWBPJDJ),1,2))
				         END                                                            AS RATINGDURATIONTYPE  --评级期限类型_外部评级结果
				        ,CASE WHEN RWAIE.ZQWBPJDJ IS NULL THEN '0124'
				         ELSE RWA_DEV.GETSTANDARDRATING1(RWAIE.ZQWBPJDJ)
				         END                                                            AS ERATINGRESULT       --外部评级结果_外部评级结果
				        ,'0124'                                                         AS INFERRATINGRESULT   --推测评级结果_(默认为未评级 0124 未评级)
				        ,TO_CHAR(TO_DATE(RWAIE.FXR,'YYYY-MM-DD'),'YYYYMMDD')            AS ISSUEDATE           --发行日期_发行日
				        ,TO_CHAR(TO_DATE(RWAIE.DQR,'YYYY-MM-DD'),'YYYYMMDD')            AS DUEDATE             --到期日期_到期日
				        ,CASE WHEN (TO_DATE(RWAIE.DQR,'YYYY-MM-DD')-TO_DATE(RWAIE.FXR,'YYYY-MM-DD')) / 365 < 0
				              THEN 0
				              ELSE (TO_DATE(RWAIE.DQR,'YYYY-MM-DD')-TO_DATE(RWAIE.FXR,'YYYY-MM-DD')) / 365
				        END                                                             AS ORIGINALMATURITY    --原始期限_"到期日发行日"("到期日-发行日以年为单位")
				        ,CASE WHEN (TO_DATE(RWAIE.DQR,'YYYY-MM-DD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
				              THEN 0
				              ELSE (TO_DATE(RWAIE.DQR,'YYYY-MM-DD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
				        END                                                             AS RESIDUALM           --剩余期限_"到期日发行日"("到期日-当前日以年为单位")
				        ,ROUND(TO_NUMBER(REPLACE(NVL(RWAIE.ZCYE,'0'),',','')),6)
				                                                                        AS ASSETBALANCE        --资产余额_资产余额
				        ,NVL(RWAIE.BZ,'CNY')                                            AS CURRENCY            --币种_币种
				        ,ROUND(TO_NUMBER(REPLACE(NVL(RWAIE.JZZB,'0'),',','')),6)
				                                                                        AS PROVISIONS          --减值准备_减值准备
				        ,0                                                              AS L                   --档次信用增级水平(默认 0)
				        ,0                                                              AS T                   --档次厚度(默认 0)
				        ,RWAIE.TQTHLXDM                                                 AS EARLYAMORTTYPE      --提前摊还类型(默认 空)
				        ,RWAIE.LSCNLXDM                                                 AS RETAILCOMMITTYPE    --零售承诺类型(默认 空)
				        ,NVL(TO_NUMBER(REPLACE(RWAIE.SGYPJCELC,',','')),0)              AS AVERIGONTHREEMTHS   --三个月平均超额利差(默认 0)
				        ,NVL(TO_NUMBER(REPLACE(RWAIE.CELCSDD,',','')),0)	              AS INTGAPSTOPPOINT     --超额利差锁定点(默认 0)
				        ,CASE WHEN NVL(TO_NUMBER(REPLACE(RWAIE.CELCSDD,',','')),0) <> 0 THEN NVL(TO_NUMBER(REPLACE(RWAIE.SGYPJCELC,',','')),0) / NVL(TO_NUMBER(REPLACE(RWAIE.CELCSDD,',','')),0)
				         ELSE 0
				         END                                                            AS R                   --R_(默认 0)
				        ,RWAIE.FXBLZB                                                   AS ISSUSERASSETPROP    --发行机构基础资产占比
				        ,NULL                                                           AS INVESTOR            --投资者权益(默认 0)

    FROM 				RWA.RWA_WS_ABS_ISSUE_EXPOSURE RWAIE
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
    ON          RWAIE.SUPPORGID = RWD.ORGID
    AND         RWD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         RWD.SUPPTMPLID = 'M-0131'
    AND         RWD.SUBMITFLAG = '1'
    INNER JOIN  (SELECT  T1.ZCCBH
                         ,T1.ZQHFQRZZJGDM
                         ,T1.JCZCYWLX
                  FROM RWA.RWA_WS_ABS_ISSUE_POOL T1
            INNER JOIN RWA.RWA_WP_DATASUPPLEMENT RWD
                    ON T1.SUPPORGID = RWD.ORGID
                   AND RWD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                   AND RWD.SUPPTMPLID = 'M-0132'
                   AND RWD.SUBMITFLAG = '1'
                 WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                ) RWAIP                       												--资产证券化-发行机构-合约与池补录表
    ON          RWAIP.ZCCBH = RWAIE.ZCCBH
    LEFT JOIN   RWA.ORG_INFO OI                                       --机构表
    ON          RWAIE.YWSSJG = OI.ORGID
    LEFT JOIN   RWA_DEV.FNS_BND_INFO_B FBI                            --债券信息表
    ON          FBI.BOND_ID = RWAIE.ZQNM
    AND         FBI.DATANO = p_data_dt_str
    WHERE 			RWAIE.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND   			RWAIE.YHJS <> '02'																		--除了 02 投资机构  其它全放到发行机构暴露中
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_ABSEXPOSURE',cascade => true);
    --DBMS_OUTPUT.PUT_LINE('结束：导入【资产证券化发行机构风险暴露表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_ABS_ISSURE_ABSEXPOSURE-资产证券化发行机构风险暴露表，中插入数量为：' || v_count1 || '条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;

    commit;
    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '导入【资产证券化发行机构风险暴露表】('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_ABS_ISSURE_ABSEXPOSURE;
/

