CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_BONDINFO(
														p_data_dt_str  IN  VARCHAR2, --数据日期
                            p_po_rtncode   OUT VARCHAR2, --返回编号
                            p_po_rtnmsg    OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_BONDINFO
    实现功能:汇总债券信息表，插入所有债券信息
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-07
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_LC_BONDINFO|理财债券的债券信息表
    源  表2 :RWA_DEV.RWA_ZQ_BONDINFO|债券的债券信息表
    源  表3 :RWA_DEV.RWA_ZJ_BONDINFO|债券信息表-资金系统
    源  表4 :RWA_DEV.RWA_EI_CLIENT|客户汇总表
    目标表1 :RWA_DEV.RWA_EI_BONDINFO|汇总债券信息表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    pxl  2019/05/08  新增资金系统债券信息数据
    pxl  2019/09/05  移除 理财、财务系统相关债券 只保留资金系统中的 11010101 以公允价值计量且其变动计入当期损益的金融资产 债券
   */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_BONDINFO';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_BONDINFO DROP PARTITION BONDINFO' || p_data_dt_str;

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
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_BONDINFO ADD PARTITION BONDINFO' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;


    --DBMS_OUTPUT.PUT_LINE('开始：导入【债券信息表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*插入理财债券的债券信息
    INSERT INTO RWA_DEV.RWA_EI_BONDINFO(
        				DATADATE                               --数据日期
                ,BONDID                                --债券ID
                ,BONDNAME                              --债券名称
                ,BONDTYPE                              --债券类型
                ,ERATING                               --外部评级
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERSMBFLAG                         --发行人小微企业标识
                ,BONDISSUEINTENT                       --债券发行目的
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,RATETYPE                              --利率类型
                ,EXECUTIONRATE                         --执行利率
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,MODIFIEDDURATION                      --修正久期
                ,DENOMINATION                          --面额
                ,CURRENCY                              --币种
    )
    SELECT
        				DATADATE                                     AS DATADATE                 --数据日期
                ,BONDID												     	         AS BONDID                   --债券ID
                ,BONDNAME                  				 		       AS BONDNAME                 --债券名称
                ,BONDTYPE                          		       AS BONDTYPE                 --债券类型
                ,ERATING                                     AS ERATING                  --外部评级          					 补录，通过期限、机构、等级转换为标普
                ,ISSUERID                           			   AS ISSUERID                 --发行人ID          					 补录
                ,ISSUERNAME					                         AS ISSUERNAME               --发行人名称        					 补录
                ,ISSUERTYPE			 											       AS ISSUERTYPE               --发行人大类        					 规则映射
                ,ISSUERSUBTYPE							                 AS ISSUERSUBTYPE            --发行人小类        					  规则映射
                ,ISSUERREGISTSTATE							             AS ISSUERREGISTSTATE        --发行人注册国家    					 	默认：中国
                ,ISSUERSMBFLAG								               AS ISSUERSMBFLAG            --发行人小微企业标识					 	默认：否(0)
                ,BONDISSUEINTENT											       AS BONDISSUEINTENT          --债券发行目的      					 默认：其他(02)
                ,REABSFLAG						                       AS REABSFLAG                --再资产证券化标识  					 	默认：否(0)
                ,ORIGINATORFLAG															 AS ORIGINATORFLAG   				 --是否发起机构      					 1. 发行人名称＝重庆银行，则为是： 2. 否则为否
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --起始日期          					 补录
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                	                                           AS DUEDATE                  --到期日期
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --原始期限
                ,RESIDUALM                                   AS RESIDUALM                --剩余期限
                ,RATETYPE                                    AS RATETYPE                 --利率类型
                ,EXECUTIONRATE                               AS EXECUTIONRATE            --执行利率
                ,NEXTREPRICEDATE                             AS NEXTREPRICEDATE          --下次重定价日      					1. 若利率类型＝固定，则下次重定价日＝到期日期；2. 否则取系统字段 补录
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --下次重定价期限    					1. 若利率类型＝固定，则默认为：NULL；2. 否则取系统字段
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --修正久期
                ,DENOMINATION                                AS DENOMINATION             --面额
                ,CURRENCY 	                                 AS CURRENCY                 --币种

   	FROM				RWA_DEV.RWA_LC_BONDINFO
   	WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
 		;

 		COMMIT;
*/

 		/*插入债券的债券信息
    INSERT INTO RWA_DEV.RWA_EI_BONDINFO(
        				DATADATE                               --数据日期
                ,BONDID                                --债券ID
                ,BONDNAME                              --债券名称
                ,BONDTYPE                              --债券类型
                ,ERATING                               --外部评级
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERSMBFLAG                         --发行人小微企业标识
                ,BONDISSUEINTENT                       --债券发行目的
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,RATETYPE                              --利率类型
                ,EXECUTIONRATE                         --执行利率
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,MODIFIEDDURATION                      --修正久期
                ,DENOMINATION                          --面额
                ,CURRENCY                              --币种
    )
    SELECT
        				DATADATE                                     AS DATADATE                 --数据日期
                ,BONDID												     	         AS BONDID                   --债券ID
                ,BONDNAME                  				 		       AS BONDNAME                 --债券名称
                ,BONDTYPE                          		       AS BONDTYPE                 --债券类型
                ,ERATING                                     AS ERATING                  --外部评级          					 补录，通过期限、机构、等级转换为标普
                ,ISSUERID                           			   AS ISSUERID                 --发行人ID          					 补录
                ,ISSUERNAME					                         AS ISSUERNAME               --发行人名称        					 补录
                ,ISSUERTYPE			 											       AS ISSUERTYPE               --发行人大类        					 规则映射
                ,ISSUERSUBTYPE							                 AS ISSUERSUBTYPE            --发行人小类        					  规则映射
                ,ISSUERREGISTSTATE							             AS ISSUERREGISTSTATE        --发行人注册国家    					 	默认：中国
                ,ISSUERSMBFLAG								               AS ISSUERSMBFLAG            --发行人小微企业标识					 	默认：否(0)
                ,BONDISSUEINTENT											       AS BONDISSUEINTENT          --债券发行目的      					 默认：其他(02)
                ,REABSFLAG						                       AS REABSFLAG                --再资产证券化标识  					 	默认：否(0)
                ,ORIGINATORFLAG															 AS ORIGINATORFLAG   				 --是否发起机构      					 1. 发行人名称＝重庆银行，则为是： 2. 否则为否
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --起始日期          					 补录
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                	                                           AS DUEDATE                  --到期日期
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --原始期限
                ,RESIDUALM                                   AS RESIDUALM                --剩余期限
                ,RATETYPE                                    AS RATETYPE                 --利率类型
                ,EXECUTIONRATE                               AS EXECUTIONRATE            --执行利率
                ,NEXTREPRICEDATE                             AS NEXTREPRICEDATE          --下次重定价日      					1. 若利率类型＝固定，则下次重定价日＝到期日期；2. 否则取系统字段 补录
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --下次重定价期限    					1. 若利率类型＝固定，则默认为：NULL；2. 否则取系统字段
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --修正久期
                ,DENOMINATION                                AS DENOMINATION             --面额
                ,CURRENCY 	                                 AS CURRENCY                 --币种

   	FROM				RWA_DEV.RWA_ZQ_BONDINFO
   	WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
 		;

 		COMMIT;
    
    */
    
        
    /*插入资金系统债券信息*/
    INSERT INTO RWA_DEV.RWA_EI_BONDINFO(
                DATADATE                               --数据日期
                ,BONDID                                --债券ID
                ,BONDNAME                              --债券名称
                ,BONDTYPE                              --债券类型
                ,ERATING                               --外部评级
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERSMBFLAG                         --发行人小微企业标识
                ,BONDISSUEINTENT                       --债券发行目的
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,RATETYPE                              --利率类型
                ,EXECUTIONRATE                         --执行利率
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,MODIFIEDDURATION                      --修正久期
                ,DENOMINATION                          --面额
                ,CURRENCY                              --币种
    )
    SELECT
                DATADATE                                     AS DATADATE                 --数据日期
                ,BONDID                                      AS BONDID                   --债券ID
                ,BONDNAME                                    AS BONDNAME                 --债券名称
                ,BONDTYPE                                    AS BONDTYPE                 --债券类型
                ,ERATING                                     AS ERATING                  --外部评级                    补录，通过期限、机构、等级转换为标普
                ,ISSUERID                                    AS ISSUERID                 --发行人ID                     补录
                ,ISSUERNAME                                  AS ISSUERNAME               --发行人名称                   补录
                ,ISSUERTYPE                                  AS ISSUERTYPE               --发行人大类                   规则映射
                ,ISSUERSUBTYPE                               AS ISSUERSUBTYPE            --发行人小类                    规则映射
                ,ISSUERREGISTSTATE                           AS ISSUERREGISTSTATE        --发行人注册国家                默认：中国
                ,ISSUERSMBFLAG                               AS ISSUERSMBFLAG            --发行人小微企业标识            默认：否(0)
                ,BONDISSUEINTENT                             AS BONDISSUEINTENT          --债券发行目的                默认：其他(02)
                ,REABSFLAG                                   AS REABSFLAG                --再资产证券化标识             默认：否(0)
                ,ORIGINATORFLAG                              AS ORIGINATORFLAG           --是否发起机构                1. 发行人名称＝重庆银行，则为是： 2. 否则为否
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS STARTDATE                --起始日期                    补录
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                             AS DUEDATE                  --到期日期
                ,ORIGINALMATURITY                            AS ORIGINALMATURITY         --原始期限
                ,RESIDUALM                                   AS RESIDUALM                --剩余期限
                ,RATETYPE                                    AS RATETYPE                 --利率类型
                ,EXECUTIONRATE                               AS EXECUTIONRATE            --执行利率
                ,NEXTREPRICEDATE                             AS NEXTREPRICEDATE          --下次重定价日               1. 若利率类型＝固定，则下次重定价日＝到期日期；2. 否则取系统字段 补录
                ,NEXTREPRICEM                                AS NEXTREPRICEM             --下次重定价期限              1. 若利率类型＝固定，则默认为：NULL；2. 否则取系统字段
                ,MODIFIEDDURATION                            AS MODIFIEDDURATION         --修正久期
                ,DENOMINATION                                AS DENOMINATION             --面额
                ,CURRENCY                                    AS CURRENCY                 --币种

    FROM        RWA_DEV.RWA_ZJ_BONDINFO --债券信息-资金系统
    WHERE       DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;

    COMMIT;

 		----------------------------------------------更新债券信息汇总表发行人大小类---------------------------------------------------------
    UPDATE RWA_DEV.RWA_EI_BONDINFO T1
      SET T1.ISSUERTYPE = (
                           SELECT T2.CLIENTTYPE
                           FROM RWA_DEV.RWA_EI_CLIENT T2
                           WHERE T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                           AND T1.ISSUERID=T2.CLIENTID
                          )
          ,T1.ISSUERSUBTYPE = (
                               SELECT T2.CLIENTSUBTYPE
                               FROM RWA_DEV.RWA_EI_CLIENT T2
                               WHERE T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                               AND T1.ISSUERID=T2.CLIENTID
                              )
    WHERE   T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND   T1.ISSUERTYPE    IS NULL
      AND   T1.ISSUERSUBTYPE IS NULL
    ;
    COMMIT;

    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_BONDINFO',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_BONDINFO',partname => 'BONDINFO'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    --DBMS_OUTPUT.PUT_LINE('结束：导入【债券信息表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_BONDINFO WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_BONDINFO表当前汇总债券信息记录为: ' || v_count || ' 条');



    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '债券信息汇总('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_BONDINFO;
/

