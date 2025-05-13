CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_TRADBONDPOSITION(
														p_data_dt_str IN  VARCHAR2, --数据日期
                            p_po_rtncode  OUT VARCHAR2, --返回编号
                            p_po_rtnmsg   OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_TRADBONDPOSITION
    实现功能:汇总交易债券头寸表，插入所有交易债券头寸信息
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-07
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_LC_TRADBONDPOSITION|理财债券交易债券头寸表
    源  表2 :RWA_DEV.RWA_ZQ_TRADBONDPOSITION|债券交易债券头寸表
    源  表3 :RWA_DEV.RWA_ZJ_TRADBONDPOSITION|债券交易债券头寸表 -资金系统   
    
    目标表1 :RWA_DEV.RWA_EI_TRADBONDPOSITION|交易债券头寸汇总表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    pxl 2019/05/08  增加资金系统债券头寸信息   
    pxl  2019/09/05  移除 理财、财务系统相关债券 只保留资金系统中的 11010101 以公允价值计量且其变动计入当期损益的金融资产 债券
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_TRADBONDPOSITION';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_TRADBONDPOSITION DROP PARTITION TRADBONDPOSITION' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总债券信息表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_TRADBONDPOSITION ADD PARTITION TRADBONDPOSITION' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;


    --DBMS_OUTPUT.PUT_LINE('开始：导入【交易债券头寸表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*插入理财债券的交易债券头寸信息
    INSERT INTO RWA_DEV.RWA_EI_TRADBONDPOSITION(
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
                ,SYSID                                 --系统来源数据测试用
    )
    SELECT
        				DATADATE      				                        AS DATADATE
        				,POSITIONID			                              AS POSITIONID
        				,BONDID																        AS BONDID
        				,TRANORGID                          					AS TRANORGID
        				,ACCORGID                          					  AS ACCORGID
        				,INSTRUMENTSTYPE                              AS INSTRUMENTSTYPE
        				,ACCSUBJECTS                                  AS ACCSUBJECTS
        				,DENOMINATION                 					      AS DENOMINATION
        				,MARKETVALUE                   					      AS MARKETVALUE
        				,DISCOUNTPREMIUM                              AS DISCOUNTPREMIUM
        				,FAIRVALUECHANGE                              AS FAIRVALUECHANGE
        				,BOOKBALANCE                					        AS BOOKBALANCE
        				,CURRENCY                    					        AS CURRENCY
                ,'LC'

    FROM				RWA_DEV.RWA_LC_TRADBONDPOSITION
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

		COMMIT;

    */

		/*插入债券的交易债券头寸信息
    INSERT INTO RWA_DEV.RWA_EI_TRADBONDPOSITION(
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
                ,SYSID                                 --系统来源数据测试用
    )
    SELECT
        				DATADATE      				                        AS DATADATE
        				,POSITIONID			                              AS POSITIONID
        				,BONDID																        AS BONDID
        				,TRANORGID                          					AS TRANORGID
        				,ACCORGID                          					  AS ACCORGID
        				,INSTRUMENTSTYPE                              AS INSTRUMENTSTYPE
        				,ACCSUBJECTS                                  AS ACCSUBJECTS
        				,DENOMINATION                 					      AS DENOMINATION
        				,MARKETVALUE                   					      AS MARKETVALUE
        				,DISCOUNTPREMIUM                              AS DISCOUNTPREMIUM
        				,FAIRVALUECHANGE                              AS FAIRVALUECHANGE
        				,BOOKBALANCE                					        AS BOOKBALANCE
        				,CURRENCY                    					        AS CURRENCY
                ,'FS'                                 --系统来源数据测试用

    FROM				RWA_DEV.RWA_ZQ_TRADBONDPOSITION
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

		COMMIT;
    
    */
    
    
    /*插入债券的交易债券头寸信息*/
    INSERT INTO RWA_DEV.RWA_EI_TRADBONDPOSITION(
                DATADATE                               --数据日期
                ,POSITIONID                            --头寸ID
                ,BONDID                                --债券ID
                ,TRANORGID                             --交易机构ID
                ,ACCORGID                              --账务机构ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ACCSUBJECTS                           --会计科目
                ,DENOMINATION                          --面额
                ,MARKETVALUE                           --市值
                ,DISCOUNTPREMIUM                       --折溢价
                ,FAIRVALUECHANGE                       --公允价值变动
                ,BOOKBALANCE                           --账面余额
                ,CURRENCY                              --币种
                ,SYSID                                 --系统来源数据测试用
    )
    SELECT
                DATADATE                                      AS DATADATE
                ,POSITIONID                                   AS POSITIONID
                ,BONDID                                       AS BONDID
                ,TRANORGID                                    AS TRANORGID
                ,ACCORGID                                     AS ACCORGID
                ,INSTRUMENTSTYPE                              AS INSTRUMENTSTYPE
                ,ACCSUBJECTS                                  AS ACCSUBJECTS
                ,DENOMINATION                                 AS DENOMINATION
                ,MARKETVALUE                                  AS MARKETVALUE
                ,DISCOUNTPREMIUM                              AS DISCOUNTPREMIUM
                ,FAIRVALUECHANGE                              AS FAIRVALUECHANGE
                ,BOOKBALANCE                                  AS BOOKBALANCE
                ,CURRENCY                                     AS CURRENCY
                ,'ZJ'

    FROM        RWA_DEV.RWA_ZJ_TRADBONDPOSITION   --债券交易债券头寸表 -资金系统
    WHERE       DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;
    
    

		--整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_TRADBONDPOSITION',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_TRADBONDPOSITION',partname => 'TRADBONDPOSITION'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    --DBMS_OUTPUT.PUT_LINE('结束：导入【交易债券头寸表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_TRADBONDPOSITION WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_TRADBONDPOSITION表当前汇总交易债券头寸记录为: ' || v_count || ' 条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;


    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '交易债券头寸汇总('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_TRADBONDPOSITION;
/

