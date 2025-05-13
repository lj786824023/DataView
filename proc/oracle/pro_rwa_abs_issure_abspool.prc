CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_ABSPOOL(p_data_dt_str  IN  VARCHAR2, --数据日期
                                                       p_po_rtncode   OUT VARCHAR2, --返回编号
                                                       p_po_rtnmsg    OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ABS_ISSURE_ABSPOOL
    实现功能:将相关信息全量导入RWA接口表发行机构-资产证券化合约与池信息表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-06-23
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA.RWA_WS_ABS_ISSUE_POOL|资产证券化-发行机构-合约与池补录表
    源  表2 :RWA.ORG_INFO|机构表
    源  表3 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表1 :RWA_DEV.RWA_ABS_ISSURE_ABSPOOL|发行机构-资产证券化合约与池信息表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_ABSPOOL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_ABSPOOL';

    --DBMS_OUTPUT.PUT_LINE('开始：导入【发行机构-资产证券化合约与池信息表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_ABSPOOL(
         DataDate           	--数据日期
        ,DataNo             	--数据流水号
        ,ABSPoolID          	--证券化资产池ID
        ,ABSOriginatorID    	--证券化发起人ID
        ,OrgSortNo          	--所属机构排序号
        ,OrgID              	--所属机构ID
        ,OrgName            	--所属机构名称
        ,BusinessLine       	--业务条线
        ,AssetType          	--资产大类
        ,AssetSubType       	--资产小类
        ,ABSName            	--资产证券化名称
        ,ABSType            	--资产证券化类型
        ,UnderAssetType     	--基础资产类型
        ,ReABSFlag          	--再资产证券化标识
        ,OriginatorFlag     	--是否发起机构
        ,IRBBankFlag        	--发起机构是否内评法银行
        ,SatisfyManageFlag   	--是否符合管理条件
        ,ComplianceABSFlag   	--是否合规资产证券化
        ,ProvideISFlag       	--是否提供隐性支持
        ,SaleGains           	--销售利得
        ,PropUnderAssetIRB   	--基础资产采用内评法计算比重
        ,SimplAlgoFlag       	--是否采用简化方法
        ,LargestExpoPP       	--最大风险暴露的资产组合份额
        ,N                    --有效数量
    )
    WITH TEMP_TABLE AS (
          SELECT  		ZCCBH                                               AS ZCCBH          --资产池代号
		                 ,SUM(CASE WHEN RWAIE.YHJS='01' THEN 1 ELSE 0 END)    AS ORIGINATORFLAG --是否发起机构
		                 ,MAX(RWAIE.TX)                                       AS BUSINESSLINE   --条线
		                 ,MAX(RWAIE.ZZCZQHBZ)                                 AS REABSFLAG      --再资产证券化标识
          FROM 				RWA.RWA_WS_ABS_ISSUE_EXPOSURE RWAIE
          INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
          ON          RWAIE.SUPPORGID=RWD.ORGID
          AND         RWD.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
          AND         RWD.SUPPTMPLID='M-0131'
          AND         RWD.SUBMITFLAG='1'
          WHERE				RWAIE.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
          GROUP BY    RWAIE.ZCCBH
    )
    SELECT
				         RWAIP.DATADATE                           AS DATADATE          --数据日期_数据日期
				        ,TO_CHAR(RWAIP.DATADATE,'YYYYMMDD')       AS DATANO            --数据流水号_数据日期
				        ,RWAIP.ZCCBH                              AS ABSPOOLID         --证券化资产池代号_资产池代号
				        ,RWAIP.ZQHFQRZZJGDM                       AS ABSORIGINATORID   --证券化发起人代号_证券发起人组织机构代码
				        ,OI.SORTNO                                AS ORGSORTNO         --所属机构排序号
				        ,RWAIP.YWSSJG                             AS ORGID             --所属机构代号_机构代号
				        ,OI.ORGNAME                               AS ORGNAME           --机构名称
				        ,NVL(TT.BUSINESSLINE,'0401')              AS BUSINESSLINE      --条线
				        ,'310'                                    AS ASSETTYPE         --资产大类
				        ,'31001'                                  AS ASSETSUBTYPE      --资产小类
				        ,RWAIP.ZCZQHMC                            AS ABSNAME           --资产证券化名称_资产证券化名称
				        ,RWAIP.ZCZQHLX                            AS ABSTYPE           --资产证券化类型_补录数据表中默认：ABSType 01 传统型
				        ,RWAIP.JCZCYWLX                           AS UNDERASSETTYPE    --基础资产类型_基础资产业务类型
				        ,NVL(TT.REABSFLAG,'0')                    AS REABSFLAG         --再资产证券化标识
				        ,NVL(CASE WHEN TT.ORIGINATORFLAG > 0
				                  THEN '1' ELSE '0' END,'0')      AS ORIGINATORFLAG    --是否发起机构(如果暴露表中有一个为发起机构则为 是，否则为 否)
				        ,'1'                                      AS IRBBANKFLAG       --发起机构是否内评法银行
				        ,RWAIP.SFFHGLTJ                           AS SATISFYMANAGEFLAG --是否符合管理条件_是否符合管理条件
				        ,RWAIP.SFHGZCZQH                          AS COMPLIANCEABSFLAG --是否合规证券化_是否合规资产证券化
				        ,RWAIP.SFTGYXZC                           AS PROVIDEISFLAG     --是否提供隐性支持_是否提供隐形支持
				        ,CASE WHEN TT.ORIGINATORFLAG > 0 THEN NVL(RWAIP.XSLD,0)
				              ELSE RWAIP.XSLD
				         END										                  AS SALEGAINS         --销售利得_销售利得
				        ,NULL                                     AS PROPUNDERASSETIRB --基础资产采用内评法计算比重_默认为空
				        ,'0'                                      AS SIMPLALGOFLAG     --是否采用简化方法_（默认 是 1 是 0 否）
				        ,0.03                                     AS LARGESTEXPOPP     --最大风险暴露的资产组合份额_默认0.03
				        ,NULL																			AS N								 --有效数量

   	FROM 				RWA.RWA_WS_ABS_ISSUE_POOL RWAIP
   	INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
   	ON          RWAIP.SUPPORGID=RWD.ORGID
   	AND         RWD.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
   	AND         RWD.SUPPTMPLID='M-0132'
   	AND         RWD.SUBMITFLAG='1'
   	INNER JOIN  TEMP_TABLE TT
   	ON          TT.ZCCBH = RWAIP.ZCCBH
   	LEFT JOIN   RWA.ORG_INFO OI
   	ON          RWAIP.YWSSJG = OI.ORGID
   	WHERE 			RWAIP.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
   	;

   	COMMIT;

   	dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_ABSPOOL',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('结束：导入【发行机构-资产证券化合约与池信息表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_ABSPOOL;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_ABS_ISSURE_ABSPOOL-发行机构-资产证券化合约与池信息表，中插入数量为：' || v_count1 || '条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '导入【发行机构-资产证券化合约与池信息表】('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_ABS_ISSURE_ABSPOOL;
/

