CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_ABSPOOL(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_ABSPOOL
    实现功能:合约与池信息汇总表,插入合约与池信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-11
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_DEV.RWA_ABS_INVEST_ABSPOOL|资产证券化-投资机构合约与池信息表
    源  表2	:RWA_DEV.RWA_ABS_ISSURE_ABSPOOL|资产证券化-发行机构合约与池信息表
    目标表  :RWA_DEV.RWA_EI_ABSPOOL|合约与池信息汇总表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
  	*/
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_ABSPOOL';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ABSPOOL DROP PARTITION ABSPOOL' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总合约与池信息表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_ABSPOOL ADD PARTITION ABSPOOL' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入资产证券化-投资机构的合约与池信息*/
    INSERT INTO RWA_DEV.RWA_EI_ABSPOOL(
               DataDate         	                    --数据日期
              ,DataNo                                 --数据流水号
              ,ABSPoolID                              --证券化资产池ID
              ,ABSOriginatorID                        --证券化发起人ID
              ,OrgID                                  --所属机构ID
              ,ABSName                                --资产证券化名称
              ,IRBBankFlag                            --发起机构是否内评法行
              ,ABSType                                --资产证券化类型
              ,UnderAssetType                         --基础资产类型
              ,OriginatorFlag   	                    --是否发起机构
              ,SatisfyManageFlag	                    --是否符合管理条件
              ,ComplianceABSFlag                      --是否合规资产证券化
              ,ProvideISFlag                          --是否提供隐性支持
              ,SaleGains                              --销售利得
              ,PropUnderAssetIRB                      --基础资产采用内评法计算比重
              ,SimplAlgoFlag                          --是否采用简化方法
              ,LargestExpoPP                          --最大风险暴露的资产组合份额
              ,ORGSORTNO                              --所属机构排序号
              ,ORGNAME                                --机构名称
              ,BUSINESSLINE                           --条线
              ,ASSETSUBTYPE                           --资产小类
              ,ASSETTYPE                              --资产大类
              ,REABSFLAG                              --再资产证券化标识
)
     SELECT
                 DataDate                                             AS DataDate           --数据日期
                ,DataNo                                               AS DataNo             --数据流水号
                ,ABSPoolID                                            AS ABSPoolID          --证券化资产池ID
                ,ABSOriginatorID                                      AS ABSOriginatorID    --证券化发起人ID
                ,OrgID                                                AS OrgID              --所属机构ID
                ,ABSName                                              AS ABSName            --资产证券化名称
                ,IRBBankFlag                                          AS IRBBankFlag        --发起机构是否内评法行
                ,ABSType                                              AS ABSType            --资产证券化类型
                ,UnderAssetType                                       AS UnderAssetType     --基础资产类型
                ,OriginatorFlag                                       AS OriginatorFlag     --是否发起机构
                ,SatisfyManageFlag                                    AS SatisfyManageFlag  --是否符合管理条件
                ,ComplianceABSFlag                                    AS ComplianceABSFlag  --是否合规资产证券化
                ,ProvideISFlag                                        AS ProvideISFlag      --是否提供隐性支持
                ,SaleGains                                            AS SaleGains          --销售利得
                ,PropUnderAssetIRB                                    AS PropUnderAssetIRB  --基础资产采用内评法计算比重
                ,SimplAlgoFlag                                        AS SimplAlgoFlag      --是否采用简化方法
                ,LargestExpoPP                                        AS LargestExpoPP      --最大风险暴露的资产组合份额
                ,ORGSORTNO                                            AS ORGSORTNO          --所属机构排序号
                ,ORGNAME                                              AS ORGNAME            --机构名称
                ,BUSINESSLINE                                         AS BUSINESSLINE       --条线
                ,ASSETSUBTYPE                                         AS ASSETSUBTYPE       --资产小类
                ,ASSETTYPE                                            AS ASSETTYPE          --资产大类
                ,REABSFLAG                                            AS REABSFLAG          --再资产证券化标识
    FROM 				RWA_DEV.RWA_ABS_INVEST_ABSPOOL
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入资产证券化-发行机构的合约与池信息*/
    INSERT INTO RWA_DEV.RWA_EI_ABSPOOL(
               DataDate         	                    --数据日期
              ,DataNo                                 --数据流水号
              ,ABSPoolID                              --证券化资产池ID
              ,ABSOriginatorID                        --证券化发起人ID
              ,OrgID                                  --所属机构ID
              ,ABSName                                --资产证券化名称
              ,IRBBankFlag                            --发起机构是否内评法行
              ,ABSType                                --资产证券化类型
              ,UnderAssetType                         --基础资产类型
              ,OriginatorFlag   	                    --是否发起机构
              ,SatisfyManageFlag	                    --是否符合管理条件
              ,ComplianceABSFlag                      --是否合规资产证券化
              ,ProvideISFlag                          --是否提供隐性支持
              ,SaleGains                              --销售利得
              ,PropUnderAssetIRB                      --基础资产采用内评法计算比重
              ,SimplAlgoFlag                          --是否采用简化方法
              ,LargestExpoPP                          --最大风险暴露的资产组合份额
              ,ORGSORTNO                              --所属机构排序号
              ,ORGNAME                                --机构名称
              ,BUSINESSLINE                           --条线
              ,ASSETSUBTYPE                           --资产小类
              ,ASSETTYPE                              --资产大类
              ,REABSFLAG                              --再资产证券化标识
)
     SELECT
                 DataDate                                             AS DataDate           --数据日期
                ,DataNo                                               AS DataNo             --数据流水号
                ,ABSPoolID                                            AS ABSPoolID          --证券化资产池ID
                ,ABSOriginatorID                                      AS ABSOriginatorID    --证券化发起人ID
                ,OrgID                                                AS OrgID              --所属机构ID
                ,ABSName                                              AS ABSName            --资产证券化名称
                ,IRBBankFlag                                          AS IRBBankFlag        --发起机构是否内评法行
                ,ABSType                                              AS ABSType            --资产证券化类型
                ,UnderAssetType                                       AS UnderAssetType     --基础资产类型
                ,OriginatorFlag                                       AS OriginatorFlag     --是否发起机构
                ,SatisfyManageFlag                                    AS SatisfyManageFlag  --是否符合管理条件
                ,ComplianceABSFlag                                    AS ComplianceABSFlag  --是否合规资产证券化
                ,ProvideISFlag                                        AS ProvideISFlag      --是否提供隐性支持
                ,SaleGains                                            AS SaleGains          --销售利得
                ,PropUnderAssetIRB                                    AS PropUnderAssetIRB  --基础资产采用内评法计算比重
                ,SimplAlgoFlag                                        AS SimplAlgoFlag      --是否采用简化方法
                ,LargestExpoPP                                        AS LargestExpoPP      --最大风险暴露的资产组合份额
                ,ORGSORTNO                                            AS ORGSORTNO          --所属机构排序号
                ,ORGNAME                                              AS ORGNAME            --机构名称
                ,BUSINESSLINE                                         AS BUSINESSLINE       --条线
                ,ASSETSUBTYPE                                         AS ASSETSUBTYPE       --资产小类
                ,ASSETTYPE                                            AS ASSETTYPE          --资产大类
                ,REABSFLAG                                            AS REABSFLAG          --再资产证券化标识
    FROM 				RWA_DEV.RWA_ABS_ISSURE_ABSPOOL
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


INSERT INTO
 RWA_EI_ABSPOOL
(
DATADATE         --数据日期
,DATANO         --数据流水号
,ABSPOOLID         --证券化资产池ID
,ABSORIGINATORID         --证券化发起人ID
,ORGSORTNO         --机构流水号
,ORGID         --所属机构ID
,ORGNAME         --机构名称
,BUSINESSLINE         --条线
,ASSETTYPE         --资产大类
,ASSETSUBTYPE         --资产小类
,ABSNAME         --资产证券化名称
,IRBBANKFLAG         --发起机构是否内评法行
,ABSTYPE         --资产证券化类型
,UNDERASSETTYPE         --基础资产类型
,ORIGINATORFLAG         --是否发起机构
,SATISFYMANAGEFLAG         --是否符合管理条件
,COMPLIANCEABSFLAG         --是否合规资产证券化
,PROVIDEISFLAG         --是否提供隐性支持
,SALEGAINS         --销售利得
,PROPUNDERASSETIRB         --基础资产采用内评法计算比重
,SIMPLALGOFLAG         --是否采用简化方法
,LARGESTEXPOPP         --最大风险暴露的资产组合份额
,REABSFLAG         --再资产证券化标识
,N         --风险暴露有效数量
)
SELECT
DATADATE         --数据日期
,DATANO         --数据流水号
,ABSPOOLID         --证券化资产池ID
,ABSORIGINATORID         --证券化发起人ID
,ORGSORTNO         --机构流水号
,ORGID         --所属机构ID
,ORGNAME         --所属机构名称
,BUSINESSLINE         --条线
,ASSETTYPE         --资产大类
,ASSETSUBTYPE         --资产小类
,''         --资产证券化名称
,''         --发起机构是否内评法行
,'01'         --资产证券化类型
,'01'         --基础资产类型
,''         --是否发起机构
,''         --是否符合管理条件
,''         --是否合规资产证券化
,''         --是否提供隐性支持
,''         --销售利得
,''         --基础资产采用内评法计算比重
,''         --是否采用简化方法
,''         --最大风险暴露的资产组合份额
,''         --再资产证券化标识
,''       --风险暴露有效数量
FROM RWA_EI_ABSEXPOSURE T WHERE T.DATADATE= TO_DATE(p_data_dt_str, 'YYYYMMDD');
COMMIT;
/*by chengang*/


    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ABSPOOL',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_ABSPOOL',partname => 'ABSPOOL'||p_data_dt_str,granularity => 'PARTITION',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_ABSPOOL WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_ABSPOOL表当前插入的数据记录为:' || v_count1 || '条');

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '资产证券化的合约与池信息表(RWA_DEV.PRO_RWA_EI_ABSPOOL)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_ABSPOOL;
/

