CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_CONTRACT(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:PRO_RWA_SMZ_CONTRACT
    实现功能:私募债-合同
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-08
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_SMZ_EXPOSURE|私募债暴露表
    目标表  :RWA_SMZ_CONTRACT|私募债合同表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_CONTRACT';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_SMZ_CONTRACT';

    --2.将满足条件的数据插入到目标表中
    INSERT INTO RWA_SMZ_CONTRACT(
                DataDate                               --数据日期
                ,DataNo                                --数据流水号
                ,ContractID                      	 		 --合同ID
                ,SContractID                     	 		 --源合同ID
                ,SSysID                          	 		 --源系统ID
                ,ClientID                        	 		 --参与主体ID
                ,SOrgID                          	 		 --源机构ID
                ,SOrgName                        	 		 --源机构名称
                ,OrgID                           	 		 --所属机构ID
                ,OrgName                         	 		 --所属机构名称
                ,IndustryID                      	 		 --所属行业代码
                ,IndustryName                    	 		 --所属行业名称
                ,BusinessLine                    	 		 --条线
                ,AssetType                       	 		 --资产大类
                ,AssetSubType                   	 		 --资产小类
                ,BusinessTypeID            				 		 --业务品种代码
                ,BusinessTypeName                	 		 --业务品种名称
                ,CreditRiskDataType              	 		 --信用风险数据类型
                ,StartDate                       	 		 --起始日期
                ,DueDate                         	 		 --到期日期
                ,OriginalMaturity                	 		 --原始期限
                ,ResidualM                       	 		 --剩余期限
                ,SettlementCurrency              	 		 --结算币种
                ,ContractAmount                    		 --合同总金额
                ,NotExtractPart                  	 		 --合同未提取部分
                ,UncondCancelFlag                	 		 --是否可随时无条件撤销
                ,ABSUAFlag                       	 		 --资产证券化基础资产标识
                ,ABSPoolID                       	 		 --证券化资产池ID
                ,ABSProportion                   	 		 --资产证券化比重
                ,GroupID                         	 		 --分组编号
                ,GUARANTEETYPE												 --主要担保方式
                ,ORGSORTNO                             --所属机构排序号
    )WITH TEMP_WS_PRIVATE_BOND AS (
                  SELECT ZYDBFSDM,ZQID
                    FROM RWA.RWA_WS_PRIVATE_BOND WHERE ROWID IN(
                    SELECT MAX(BOND.ROWID)
                      FROM RWA.RWA_WS_PRIVATE_BOND BOND
                INNER JOIN RWA.RWA_WP_DATASUPPLEMENT T5
    										ON BOND.SUPPORGID=T5.ORGID
    									 AND T5.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    									 AND T5.SUPPTMPLID='M-0110'
    									 AND T5.SUBMITFLAG='1'
                     WHERE BOND.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
                  GROUP BY BOND.ZQID
                    )
              )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --数据日期
                ,p_data_dt_str													     AS DataNo                   --数据流水号
                ,T1.ContractID                               AS ContractID            	 --合同ID
                ,T1.ContractID                        	 		 AS SContractID        	 	 	 --源合同ID
                ,'SMZ'                                       AS SSysID                	 --源系统ID
                ,T1.ClientID                       			     AS ClientID              	 --参与主体ID
                ,T1.OrgID                          			     AS SOrgID                	 --源机构ID
                ,T1.OrgName                              	   AS SOrgName            	 	 --源机构名称
                ,T1.OrgID                               	   AS OrgID                 	 --所属机构ID
                ,T1.OrgName                                  AS OrgName               	 --所属机构名称
                ,T1.IndustryID                               AS IndustryID            	 --所属行业代码
                ,T1.IndustryName                             AS IndustryName          	 --所属行业名称
                ,''                              		         AS BusinessLine          	 --条线
                ,''                                          AS AssetType                --资产大类
                ,''                                          AS AssetSubType             --资产小类
                ,''                                          AS BusinessTypeID           --业务品种代码
                ,''                                          AS BusinessTypeName         --业务品种名称
                ,'06'                                        AS CreditRiskDataType       --信用风险数据类型
                ,T1.StartDate                                AS StartDate                --起始日期
                ,T1.DueDate                                  AS DueDate                  --到期日期
                ,T1.OriginalMaturity                         AS OriginalMaturity         --原始期限
                ,T1.ResidualM                                AS ResidualM                --剩余期限
                ,T1.CURRENCY                                 AS SettlementCurrency       --结算币种
                ,''                                          AS ContractAmount           --合同总金额
                ,''                                          AS NotExtractPart           --合同未提取部分
                ,'0'                                         AS UncondCancelFlag         --是否可随时无条件撤销
                ,'0'                                         AS ABSUAFlag                --资产证券化基础资产标识
                ,''                                          AS ABSPoolID                --证券化资产池ID
                ,0                                           AS ABSProportion            --资产证券化比重
                ,''                                          AS GroupID                  --分组编号
                ,T2.ZYDBFSDM                                 AS GUARANTEETYPE            --主要担保方式
                ,T1.ORGSORTNO                                AS ORGSORTNO                --所属机构排序号

    FROM        RWA_DEV.RWA_SMZ_EXPOSURE T1
    LEFT JOIN   TEMP_WS_PRIVATE_BOND T2
    ON          T1.CONTRACTID=T2.ZQID
    WHERE       T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_CONTRACT;
    --Dbms_output.Put_line('RWA_SMZ_CONTRACT表当前插入的信用卡系统数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
          --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '合同('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_SMZ_CONTRACT;
/

