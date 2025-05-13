CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_CONTRACT(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_PJ_CONTRACT
    实现功能:核心系统-票据贴现-合同信息(从数据源核心系统将合同相关信息全量导入RWA票据贴现接口表合同信息表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_PJ_EXPOSURE|贴现卡片帐

    目标表  :RWA_DEV.RWA_PJ_CONTRACT|核心系统票据贴现类合同信息表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_CONTRACT';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_PJ_CONTRACT';

    --1.将满足条件的数据从源表插入到目标表中 贴现、转贴现、内部转贴现
    INSERT INTO RWA_DEV.RWA_PJ_CONTRACT(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                         	 --合同ID
                ,SCONTRACTID                        	 --源合同ID
                ,SSYSID                             	 --源系统ID
                ,CLIENTID                           	 --参与主体ID
                ,SORGID                             	 --源机构ID
                ,SORGNAME                           	 --源机构名称
                ,ORGSORTNO                             --所属机构排序号
                ,ORGID                              	 --所属机构ID
                ,ORGNAME                            	 --所属机构名称
                ,INDUSTRYID                         	 --所属行业代码
                ,INDUSTRYNAME                       	 --所属行业名称
                ,BUSINESSLINE                       	 --条线
                ,ASSETTYPE                          	 --资产大类
                ,ASSETSUBTYPE                       	 --资产小类
                ,BUSINESSTYPEID               				 --业务品种代码
                ,BUSINESSTYPENAME                   	 --业务品种名称
                ,CREDITRISKDATATYPE                 	 --信用风险数据类型
                ,STARTDATE                          	 --起始日期
                ,DUEDATE                            	 --到期日期
                ,ORIGINALMATURITY                   	 --原始期限
                ,RESIDUALM                          	 --剩余期限
                ,SETTLEMENTCURRENCY                 	 --结算币种
                ,CONTRACTAMOUNT                     	 --合同总金额
                ,NOTEXTRACTPART                     	 --合同未提取部分
                ,UNCONDCANCELFLAG                   	 --是否可随时无条件撤销
                ,ABSUAFLAG                          	 --资产证券化基础资产标识
                ,ABSPOOLID                          	 --资产证券化池ID
                ,GROUPID                            	 --分组编号
                ,GUARANTEETYPE												 --主要担保方式
                ,ABSPROPORTION                      	 --资产证券化比重

    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                             --数据日期
                ,p_data_dt_str                               AS DataNo                               --数据流水号
                ,T1.CONTRACTID                               AS ContractID                           --合同ID
                ,T1.CONTRACTID                               AS SContractID                          --源合同ID
                ,T1.SSysID                                   AS SSysID                               --源系统ID
                ,T1.CLIENTID                                 AS ClientID                             --参与主体ID
                ,T1.SORGID                                   AS SOrgID                               --源机构ID
                ,T1.SORGNAME                                 AS SOrgName                             --源机构名称
                ,T1.ORGSORTNO                                AS OrgSortNo                            --所属机构排序号
                ,T1.ORGID                                    AS OrgID                                --所属机构ID
                ,T1.ORGNAME                                  AS OrgName                              --所属机构名称
                ,T1.INDUSTRYID                               AS IndustryID                           --所属行业代码
                ,T1.INDUSTRYNAME                             AS IndustryName                         --所属行业名称
                ,T1.BusinessLine                             AS BusinessLine                         --业务条线                     默认 同业(04)
                ,T1.ASSETTYPE                                AS AssetType                            --资产大类
                ,T1.ASSETSUBTYPE                             AS AssetSubType                         --资产小类
                ,T1.BUSINESSTYPEID                           AS BusinessTypeID                       --业务品种代码
                ,T1.BUSINESSTYPENAME                         AS BusinessTypeName                     --业务品种名称
                ,T1.CREDITRISKDATATYPE                       AS CreditRiskDataType                   --信用风险数据类型
                ,T1.STARTDATE                                AS StartDate                            --起始日期
                ,T1.DUEDATE                                  AS DueDate                              --到期日期
                ,T1.ORIGINALMATURITY                         AS OriginalMaturity                     --原始期限
                ,T1.RESIDUALM                                AS ResidualM                            --剩余期限
                ,T1.CURRENCY                                 AS SettlementCurrency                   --结算币种
                ,T1.NORMALPRINCIPAL                          AS ContractAmount                       --合同总金额
                ,0                                           AS NotExtractPart                       --合同未提取部分                默认 0
                ,'0'                                         AS UncondCancelFlag                     --是否可随时无条件撤销         默认 否(0)
                ,'0'                                         AS ABSUAFlag                            --资产证券化基础资产标识        默认 否(0)
                ,''                                          AS ABSPoolID                            --证券化资产池ID               默认 空
                ,''                                          AS GroupID                              --分组编号                     默认 空
                ,''                                          AS GUARANTEETYPE                        --主要担保方式
                ,NULL                                        AS ABSPROPORTION                        --资产证券化比重

    FROM        RWA_DEV.RWA_PJ_EXPOSURE T1
    WHERE T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_PJ_CONTRACT',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_PJ_CONTRACT;

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '合同信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_PJ_CONTRACT;
/

