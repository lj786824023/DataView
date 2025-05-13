CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_FEFORWARDSSWAP(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_FEFORWARDSSWAP
    实现功能:汇总外汇远期掉期
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-08-02
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_WH_FEFORWARDSSWAP|国结外汇远期掉期表（掉期）
    目标表  :RWA_DEV.RWA_EI_FEFORWARDSSWAP|外汇远期掉期表（掉期）
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_FEFORWARDSSWAP';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_FEFORWARDSSWAP DROP PARTITION FEFORWARDSSWAP' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
      	IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
        	p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '外汇远期掉期表（掉期）表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         	RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_FEFORWARDSSWAP ADD PARTITION FEFORWARDSSWAP' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 国结系统-外汇远期掉期表（掉期）表
    INSERT INTO RWA_DEV.RWA_EI_FEFORWARDSSWAP(
                DATADATE                               --数据日期
                ,TRANID                                --交易ID
                ,TRANORGID                             --交易机构ID
                ,ACCORGID                              --账务机构ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ACCSUBJECTS                           --会计科目
                ,BOOKTYPE                              --账户类别
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,BUYCURRENCY                           --买入币种
                ,BUYAMOUNT                             --买入金额
                ,SELLCURRENCY      										 --卖出币种
                ,SELLAMOUNT                            --卖出金额
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,BUYZERORATE                           --买入币种零息利率
                ,BUYDISCOUNTRATE                       --买入折现因子
                ,SELLZERORATE                          --卖出币种零息利率
                ,SELLDISCOUNTRATE                      --卖出折现因子

    )
    SELECT
                DATADATE            													AS DATADATE                 --数据日期
                ,TRANID            											     	AS TRANID                   --交易ID
                ,TRANORGID                       				 		 	AS TRANORGID                --交易机构ID
                ,ACCORGID                                 		AS ACCORGID                 --账务机构ID
                ,INSTRUMENTSTYPE                              AS INSTRUMENTSTYPE          --金融工具类型
                ,ACCSUBJECTS       													 	AS ACCSUBJECTS              --会计科目
                ,BOOKTYPE          													 	AS BOOKTYPE                 --账户类别
                ,STRUCTURALEXPOFLAG													 	AS STRUCTURALEXPOFLAG       --是否结构性敞口
                ,BUYCURRENCY       													 	AS BUYCURRENCY              --买入币种
                ,BUYAMOUNT         													 	AS BUYAMOUNT                --买入金额
                ,SELLCURRENCY                                 AS SELLCURRENCY      			 	--卖出币种
                ,SELLAMOUNT                                   AS SELLAMOUNT               --卖出金额
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                					                                    AS STARTDATE                --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                				                                      AS DUEDATE                  --到期日期
                ,ORIGINALMATURITY                             AS ORIGINALMATURITY         --原始期限
                ,RESIDUALM                                    AS RESIDUALM                --剩余期限
                ,BUYZERORATE                                  AS BUYZERORATE              --买入币种零息利率
                ,BUYDISCOUNTRATE                              AS BUYDISCOUNTRATE          --买入折现因子
                ,SELLZERORATE                                 AS SELLZERORATE             --卖出币种零息利率
                ,SELLDISCOUNTRATE                             AS SELLDISCOUNTRATE         --卖出折现因子

    FROM				RWA_DEV.RWA_WH_FEFORWARDSSWAP
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;

    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FEFORWARDSSWAP',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FEFORWARDSSWAP',partname => 'FEFORWARDSSWAP'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_FEFORWARDSSWAP WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_FEFORWARDSSWAP表当前插入的国结系统-外汇交易(市场风险)-外汇远期掉期记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '外汇远期掉期('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_FEFORWARDSSWAP;
/

