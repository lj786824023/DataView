CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_CMRELEVENCE(
                             P_DATA_DT_STR  IN  VARCHAR2,    --数据日期
                             P_PO_RTNCODE  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                            P_PO_RTNMSG    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:PRO_RWA_SMZ_CMRELEVENCE
    实现功能:私募债-合同与缓释物关联,表结构为合同缓释物关联表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-08
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :RWA_WS_PRIVATE_BOND|私募债业务补录模板
    目标表  :RWA_SMZ_CMRELEVENCE|私募债-合同与缓释物关联
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_CMRELEVENCE';
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

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_SMZ_CMRELEVENCE';
    --2 更新补录表的担保ID
    UPDATE RWA.RWA_WS_PRIVATE_BOND T1 SET T1.DBID = p_data_dt_str || 'SMZ' || lpad(rownum, 10, '0')
       WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'yyyymmdd') and T1.DBLX IS NOT NULL
    ;
    COMMIT;
    /*插入有效借据下合同对应的抵质押品数据*/
    INSERT INTO RWA_DEV.RWA_SMZ_CMRELEVENCE(
                  DATADATE                         --数据日期
                 ,DATANO                           --数据流水号
                 ,CONTRACTID                       --合同代号
                 ,MITIGATIONID                     --缓释物代号
                 ,MITIGCATEGORY                    --缓释物类型
                 ,SGUARCONTRACTID                  --源担保合同代号
                 ,GROUPID                          --分组编号
    )
    SELECT
                 TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                              AS  datadate                         --数据日期
                 ,P_DATA_DT_STR                                                AS  datano                          --数据流水号
                 ,T1.ZQID                                                      AS  contractid                      --合同代号  (关联主合同，判断主合同是否有效)
                 ,T1.DBID                                                      AS   mitigationid                    --缓释物代号(保证数据的缓释合同和抵质押的缓释合同分开取)
                 ,CASE WHEN T1.DBLX IN ('030010','030020','030030','020080','020090') THEN '02'                      --保证
                      ELSE '03'                                                                                              --抵质押品
                END                                                            AS  mitigcategory                   --缓释物类型
                 ,''                                                            AS  sguarcontractid                 --源担保合同代号(担保编号)
                 ,''                                                            AS  groupid                         --分组编号
    FROM    RWA.RWA_WS_PRIVATE_BOND T1
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2                    --数据补录表
    ON          T1.SUPPORGID=T2.ORGID
    AND         T2.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID='M-0110'
    AND         T2.SUBMITFLAG='1'
    WHERE   T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND     T1.DBLX IS NOT NULL
    ;
    COMMIT;


    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_SMZ_CMRELEVENCE表当前插入的数据记录为:' || v_count1 || '条');

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          P_PO_RTNCODE := sqlcode;
          P_PO_RTNMSG  := '私募债-合同与缓释物关联(PRO_RWA_SMZ_CMRELEVENCE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_SMZ_CMRELEVENCE;
/

