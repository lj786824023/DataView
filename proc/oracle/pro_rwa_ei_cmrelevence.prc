CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CMRELEVENCE(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--数据日期
       											P_PO_RTNCODE	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														P_PO_RTNMSG		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_CMRELEVENCE
    实现功能:汇总合同缓释物表,插入所有合同缓释物表信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-06-01
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_DEV.RWA_XD_CMRELEVENCE|信贷合同缓释物关联表
    源  表2	:RWA_DEV.RWA_PJ_CMRELEVENCE|票据合同缓释物关联表
    源  表3	:RWA_DEV.RWA_LC_CMRELEVENCE|理财合同缓释物关联表
    源  表4	:RWA_DEV.RWA_TZ_CMRELEVENCE|投资合同缓释物关联表
    源  表5	:RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE|资产证券化合同缓释物关联表
    目标表	:RWA_DEV.RWA_EI_CMRELEVENCE|合同缓释物关联信息汇总表
    辅助表	:无
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CMRELEVENCE';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;
    --定义临时表名
  v_tabname VARCHAR2(200);
  --定义创建语句
  v_create VARCHAR2(1000);

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

   BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CMRELEVENCE DROP PARTITION CMRELEVENCE' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总合同缓释物信息表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CMRELEVENCE ADD PARTITION CMRELEVENCE' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入信贷的合同缓释物关联信息*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )
    SELECT
         				DATADATE       										          AS	datadate       									--数据日期
         				,DATANO         							              AS	datano              						--数据流水号
         				,CONTRACTID     								            AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,MITIGATIONID        												AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--缓释物类型
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,GROUPID        													  AS	groupid             						--分组编号
    FROM  			RWA_DEV.RWA_XD_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入票据的合同缓释物关联信息*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )
    SELECT
         				DATADATE       										          AS	datadate       									--数据日期
         				,DATANO         							              AS	datano              						--数据流水号
         				,CONTRACTID     								            AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,MITIGATIONID        												AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--缓释物类型
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,GROUPID        													  AS	groupid             						--分组编号
    FROM  			RWA_DEV.RWA_PJ_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入理财的合同缓释物关联信息*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )
    SELECT
         				DATADATE       										          AS	datadate       									--数据日期
         				,DATANO         							              AS	datano              						--数据流水号
         				,CONTRACTID     								            AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,MITIGATIONID        												AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--缓释物类型
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,GROUPID        													  AS	groupid             						--分组编号
    FROM  			RWA_DEV.RWA_LC_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入投资的合同缓释物关联信息*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )
    SELECT
         				DATADATE       										          AS	datadate       									--数据日期
         				,DATANO         							              AS	datano              						--数据流水号
         				,CONTRACTID     								            AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,MITIGATIONID        												AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--缓释物类型
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,GROUPID        													  AS	groupid             						--分组编号
    FROM  			RWA_DEV.RWA_TZ_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入资产证券化的合同缓释物关联信息*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )
    SELECT
         				DATADATE       										          AS	datadate       									--数据日期
         				,DATANO         							              AS	datano              						--数据流水号
         				,CONTRACTID     								            AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,MITIGATIONID        												AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--缓释物类型
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,GROUPID        													  AS	groupid             						--分组编号
    FROM  			RWA_DEV.RWA_ABS_ISSURE_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

     /*插入回购的合同缓释物关联信息*/
    INSERT INTO RWA_DEV.RWA_EI_CMRELEVENCE(
         				DATADATE       									  --数据日期
         				,DATANO                           --数据流水号
         				,CONTRACTID                       --合同代号
         				,MITIGATIONID                     --缓释物代号
         				,MITIGCATEGORY                    --缓释物类型
         				,SGUARCONTRACTID                  --源担保合同代号
         				,GROUPID                          --分组编号
    )
    SELECT
         				DATADATE       										          AS	datadate       									--数据日期
         				,DATANO         							              AS	datano              						--数据流水号
         				,CONTRACTID     								            AS	contractid          						--合同代号  (关联主合同，判断主合同是否有效)
         				,MITIGATIONID        												AS 	mitigationid        						--缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
         				,MITIGCATEGORY  							              AS	mitigcategory       						--缓释物类型
         				,SGUARCONTRACTID									          AS	sguarcontractid     						--源担保合同代号(担保编号)
         				,GROUPID        													  AS	groupid             						--分组编号
    FROM  			RWA_DEV.RWA_HG_CMRELEVENCE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CMRELEVENCE',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CMRELEVENCE',partname => 'CMRELEVENCE'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count FROM RWA_EI_CMRELEVENCE WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_EI_CMRELEVENCE表当前插入的数据记录为:' || v_count1 || '条');

		--Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		P_PO_RTNCODE := '1';
	  P_PO_RTNMSG  := '成功'||'-'||v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 P_PO_RTNCODE := sqlcode;
   			 P_PO_RTNMSG  := '汇总合同与缓释物关联(pro_rwa_ei_cmrelevence)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CMRELEVENCE;
/

