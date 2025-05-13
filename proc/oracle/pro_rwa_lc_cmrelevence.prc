CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_CMRELEVENCE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_CMRELEVENCE
    实现功能:理财系统-资管计划理财投资-合同缓释物关联(从数据源理财系统将资管计划理财投资相关信息全量导入RWA理财投资接口表合同缓释物关联表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.ZGS_INVESTASSETDETAIL|资产详情表
    源  表2 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
    源  表3 :RWA_DEV.ZGS_ATINTRUST_PLAN|资产管理计划表
    --源	 表4 :RWA.RWA_WS_FCII_PLAN|资管计划理财投资补录表 弃用
    --源  表5 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表 弃用
    目标表  :RWA_DEV.RWA_LC_CMRELEVENCE|核心系统票据贴现类合同缓释物关联表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_CMRELEVENCE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_CMRELEVENCE';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1更新补录表的缓释物ID
    UPDATE RWA.RWA_WS_FCII_PLAN SET MITIGATIONID = p_data_dt_str || 'LC' || lpad(rownum, 10, '0') WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;
    
    /*
    --2.2 理财系统-资管计划投资
    INSERT INTO RWA_DEV.RWA_LC_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT T3.FLD_FINANC_CODE					AS FLD_FINANC_CODE
        			,T3.FLD_ASSET_CODE					AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           --AND T4.FLD_INCOME_TYPE <> '3'			--3：排除非保本类型 --20190625 --该条件过滤导致查询结果为0
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '24'				-- 2：债券，24：资产管理计划
           AND T3.FLD_ASSET_STATUS = '1' 			--1：状态正常
           AND T3.FLD_ASSET_FLAG = '1'   			--1：理财产品
           AND T3.FLD_DATE  = p_data_dt_str		--有效的理财产品其估值日期每日更新 
           AND T3.DATANO = p_data_dt_str
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.FLD_FINANC_CODE || T1.FLD_ASSET_CODE		 AS CONTRACTID               --合同ID
                ,T2.C_PRD_CODE		     											 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,CASE WHEN T2.C_GUARANTEE_FOURTH IN ('004001004001','004001005001','004001006001','004001006002')     					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
                			  OR T2.C_GUARANTEE_FIRST = '010' THEN '02'												 --保证
                ELSE '03'																																 --抵质押品
                END	                                         AS MITIGCATEGORY            --缓释物类型
                ,''																			     AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				TEMP_INVESTASSETDETAIL T1																					--交易明细表的最新记录
    INNER JOIN	RWA_DEV.ZGS_ATINTRUST_PLAN T2																			--资管计划表
    ON					T1.FLD_ASSET_CODE = T2.C_PRD_CODE																	--信托编号唯一，故以此字段关联
    AND					nvl(T2.C_GUARANTEE_FIRST,1) <> '005'		                          --排除信用(005) 
    AND					T2.DATANO = p_data_dt_str
		;

    COMMIT;
    */

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_CMRELEVENCE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_CMRELEVENCE表当前插入的理财系统-资管计划投资数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '合同缓释物关联('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LC_CMRELEVENCE;
/

