CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZQ_TRADBONDPOSITION(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZQ_TRADBONDPOSITION
    实现功能:财务系统-债券-市场风险-交易债券头寸(从数据源财务系统将业务相关信息全量导入RWA市场风险债券接口表交易债券头寸表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-12
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    源  表2 :RWA_DEV.FNS_BND_BOOK_B|财务系统账面活动表
    源  表3 :RWA_DEV.FNS_BND_TRANSACTION_B|交易活动表
    源  表4 :RWA.RWA_WS_BONDTRADE|债券投资补录信息表
    源  表5 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表  :RWA_DEV.RWA_ZQ_TRADBONDPOSITION|财务系统债券交易债券头寸表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZQ_TRADBONDPOSITION';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZQ_TRADBONDPOSITION';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-债券
    INSERT INTO RWA_DEV.RWA_ZQ_TRADBONDPOSITION(
                DATADATE                               --数据日期
                ,POSITIONID                          	 --头寸ID
                ,BONDID                              	 --债券ID
                ,TRANORGID                           	 --交易机构ID
                ,ACCORGID                            	 --账务机构ID
                ,INSTRUMENTSTYPE                     	 --金融工具类型
                ,ACCSUBJECTS                         	 --会计科目
                ,DENOMINATION                        	 --面额
                ,MARKETVALUE                         	 --市值
                ,DISCOUNTPREMIUM                     	 --折溢价
                ,FAIRVALUECHANGE                     	 --公允价值变动
                ,BOOKBALANCE                         	 --账面余额
                ,CURRENCY                            	 --币种

    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT,
												       PAR_VALUE
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               PAR_VALUE,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,T1.BOND_ID														     	 AS POSITIONID               --头寸ID
                ,T1.BOND_ID	                    				 		 AS BONDID                   --债券ID
                ,T1.DEPARTMENT  	                      		 AS TRANORGID                --交易机构ID
                ,T1.DEPARTMENT	                             AS ACCORGID                 --账务机构ID
                ,'0101'			                           			 AS INSTRUMENTSTYPE          --金融工具类型					 默认：债券(0101)
                ,CASE WHEN T1.BOND_TYPE2 IN ('30', '50') THEN '11012001'			 					 --交易性其他投资本金
                			ELSE '11010101'																					 					 --交易性投资本金
                 END		 						                         AS ACCSUBJECTS              --会计科目    					 根据原系统的资产分类对照会计科目表确认
                ,T3.PAR_VALUE			 						               AS DENOMINATION             --面额
                ,NVL(T3.INITIAL_COST,0) + NVL(T3.MKT_VALUE_CHANGE,0)
                		                            						 AS MARKETVALUE              --市值
                ,NULL		                              			 AS DISCOUNTPREMIUM          --折溢价      					 默认：空
                ,NVL(T3.MKT_VALUE_CHANGE,0)                  AS FAIRVALUECHANGE          --公允价值变动
                ,NVL(T3.INITIAL_COST,0) + NVL(T3.INT_ADJUST,0) + NVL(T3.MKT_VALUE_CHANGE,0) + NVL(T3.ACCOUNTABLE_INT,0)
                 																						 AS BOOKBALANCE              --账面余额
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS CURRENCY                 --币种

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T3
		ON					T1.BOND_ID = T3.BOND_ID
		WHERE 			T1.ASSET_CLASS = '10'																			--仅交易性账户进入市场风险
		AND					T1.DATANO = p_data_dt_str
		AND 				T1.BOND_CODE IS NOT NULL																	--排除无效的债券数据
	  ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZQ_TRADBONDPOSITION',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZQ_TRADBONDPOSITION;
    --Dbms_output.Put_line('RWA_DEV.RWA_ZQ_TRADBONDPOSITION表当前插入的财务系统-债券(市场风险)-交易债券头寸记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '交易债券头寸('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZQ_TRADBONDPOSITION;
/

