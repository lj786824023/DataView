CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_TRADBONDPOSITION(
														p_data_dt_str IN  VARCHAR2, --数据日期
                            p_po_rtncode  OUT VARCHAR2, --返回编号
                            p_po_rtnmsg   OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_TRADBONDPOSITION
    实现功能:理财系统-债券理财投资-市场风险-交易债券头寸(从数据源理财系统将业务相关信息全量导入RWA市场风险理财接口表交易债券头寸表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-04-14
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.ZGS_INVESTASSETDETAI|资产详情表
    源  表2 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
    源  表3 :RWA_DEV.ZGS_ATBOND|债券信息表
    目标表1 :RWA_DEV.RWA_LC_TRADBONDPOSITION|交易债券头寸表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_TRADBONDPOSITION';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --DELETE FROM RWA_LC_TRADBONDPOSITION WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_TRADBONDPOSITION';


    --DBMS_OUTPUT.PUT_LINE('开始：导入【交易债券头寸表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    INSERT INTO RWA_DEV.RWA_LC_TRADBONDPOSITION(
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
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT  T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
        			 ,T3.FLD_FINANC_CODE					AS FLD_FINANC_CODE
        			 ,T3.FLD_ASSET_SHARES					AS FLD_ASSET_SHARES
        			 ,T3.FLD_CURRENCY							AS FLD_CURRENCY
        			 ,T3.FLD_MARKET_AMOUNT				AS FLD_MARKET_AMOUNT
        			 ,T3.FLD_MTM_AMOUNT						AS FLD_MTM_AMOUNT
        			 ,T4.FLD_TRANSWAY							AS FLD_TRANSWAY
        			 ,T4.FLD_SELLOBJ							AS FLD_SELLOBJ
        			 ,T4.FLD_INCOME_TYPE					AS FLD_INCOME_TYPE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           AND T4.FLD_INCOME_TYPE <> '3'																		--3：排除非保本类型
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '2'																			-- 2：债券，24：资产管理计划
           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
           AND T3.C_ACC_TYPE = 'D'																					--D：交易类，该部分数据作为市场风险
           AND T3.FLD_DATE = p_data_dt_str																	--有效的理财产品其估值日期每日更新
           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_BALANCE AS (
    							SELECT T1.C_CODE					AS FLD_FINANC_CODE
    										,CASE WHEN T2.C_VOUCHER_NO = '160226000000101' THEN 'C109311602051SZ' 			--记账凭证为160226000000101的记录，其资产代码没有，应该是C109311602051SZ，补上
    										 ELSE T1.C_ASSET_CODE
    										 END								AS FLD_ASSET_CODE
    										,T2.C_SUBJECT_CODE	AS C_SUBJECT_CODE
    										,SUM(DECODE(T2.C_CD_FLAG,'C',-T2.F_AMT,T2.F_AMT)) AS BALANCE
    								FROM RWA_DEV.ZGS_ATAGENTVOUCHER T1																		--代客会计凭证表
    					INNER JOIN RWA_DEV.ZGS_ATAGENTVOUCHER_DT T2																	--代客凭证分录表
    									ON T1.C_VOUCHER_NO = T2.C_VOUCHER_NO
    								 AND T2.C_SUBJECT_CODE IN ('12220101',
													                     '12220102',
													                     '12220103',
													                     '12220201',
													                     '12220202',
													                     '12220203')
    								 AND T2.DATANO = p_data_dt_str
    							 WHERE T1.C_ACCT_STATUS <> '3'
    								 AND T1.D_ACCT_DATE <= p_data_dt_str
    								 AND T1.DATANO = p_data_dt_str
    						GROUP BY T1.C_CODE,CASE WHEN T2.C_VOUCHER_NO = '160226000000101' THEN 'C109311602051SZ' ELSE T1.C_ASSET_CODE END,T2.C_SUBJECT_CODE
    )
    SELECT
        				TO_DATE(p_data_dt_str,'yyyyMMdd')      				AS DATADATE        					--RWA系统赋值
        				,T1.FLD_FINANC_CODE || T1.FLD_ASSET_CODE			AS POSITIONID      					--产品代码+标的代码
        				,T2.C_BOND_CODE																AS BONDID          					--债券内码
        				,'9998'                          					AS TRANORGID       					--默认 总行资产管理部(01160000)
        				,'9998'                          					AS ACCORGID        					--默认 总行资产管理部(01160000)
        				,'0101'                              					AS INSTRUMENTSTYPE 					--默认：债券 InstrumentsType 金融工具类型: 0101  债券
        				,/*CASE WHEN T1.FLD_INCOME_TYPE = '1' AND T1.FLD_SELLOBJ = '0' THEN '12220101'
        							WHEN T1.FLD_INCOME_TYPE = '1' AND T1.FLD_SELLOBJ = '5' THEN '12220103'
        							WHEN T1.FLD_INCOME_TYPE = '1' AND T1.FLD_SELLOBJ = '7' THEN '12220102'
        							WHEN T1.FLD_INCOME_TYPE = '2' AND T1.FLD_SELLOBJ = '0' THEN '12220201'
        							WHEN T1.FLD_INCOME_TYPE = '2' AND T1.FLD_SELLOBJ = '5' THEN '12220203'
        							WHEN T1.FLD_INCOME_TYPE = '2' AND T1.FLD_SELLOBJ = '7' THEN '12220202'
        							WHEN T1.FLD_INCOME_TYPE = '3' THEN '13212003'
        							ELSE ''
        				END*/
        				 T3.C_SUBJECT_CODE                   					AS ACCSUBJECTS     					--同信用风险科目映射逻辑一样 逻辑是什么？
        				,T1.FLD_ASSET_SHARES                 					AS DENOMINATION    					--份额
        				,T1.FLD_MTM_AMOUNT                   					AS MARKETVALUE     					--资产mtm值(债券按行情市价计算)
        				,''                                  					AS DISCOUNTPREMIUM 					--默认 空
        				,''                                  					AS FAIRVALUECHANGE 					--默认 空
        				,T1.FLD_MARKET_AMOUNT                					AS BOOKBALANCE     					--资产价值
        				,T1.FLD_CURRENCY                     					AS CURRENCY        					--币种

    FROM				TEMP_INVESTASSETDETAIL T1																					--交易明细表的最新记录
    INNER JOIN	RWA_DEV.ZGS_ATBOND T2
    ON					T2.C_BOND_CODE = T1.FLD_ASSET_CODE
    AND					T2.DATANO = p_data_dt_str
    INNER JOIN	TEMP_BALANCE T3																										--余额汇总表
    ON					T1.FLD_FINANC_CODE = T3.FLD_FINANC_CODE
    AND					T1.FLD_ASSET_CODE = T3.FLD_ASSET_CODE
    ;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_TRADBONDPOSITION',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('结束：导入【交易债券头寸表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_TRADBONDPOSITION;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_TRADBONDPOSITION表当前插入的理财系统-债券理财投资(市场风险)-交易债券头寸记录为: ' || v_count || ' 条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;


    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '交易债券头寸('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_LC_TRADBONDPOSITION;
/

