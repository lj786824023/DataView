CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZX_WSIB(
                            p_data_dt_str IN  VARCHAR2,   --数据日期 yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --返回编号 1 成功,0 失败
                            p_po_rtnmsg   OUT VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZX_WSIB
    实现功能:核心系统-直销银行垫款-补录铺底(从数据源核心系统将业务相关信息全量导入RWA直销银行垫款补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-06
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.CBS_IAC|通用分户账
    源  表2 :RWA.RWA_WS_DSBANK_ADV|直销银行垫款补录表
    源  表3 :RWA.RWA_WP_SUPPTASKORG|补录任务机构分发配置表
    源  表4 :RWA.RWA_WP_SUPPTASK|补录任务发布表
    目标表  :RWA.RWA_WSIB_DSBANK_ADV|直销银行垫款铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZX_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空直销银行垫款铺底表
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_DSBANK_ADV';
    DELETE FROM RWA.RWA_WSIB_DSBANK_ADV WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    INSERT INTO RWA.RWA_WSIB_DSBANK_ADV(
                DATADATE                               --数据日期
                ,ORGID                                 --机构ID
                ,IACAC_NO                              --账号
                ,IACCURBAL                             --余额
                ,IACCRTDAT                             --起始日
                ,IACDLTDAT                             --到日期
                ,BELONGORGCODE                         --业务所属机构
                ,CLIENTNAME                            --交易对手名称
                ,ORGANIZATIONCODE                      --交易对手组织机构代码
                ,COUNTRYCODE                           --交易对手注册国家代码
                ,INDUSTRYID                            --交易对手所属行业代码
                ,CLIENTCATEGORY                        --交易对手客户类型
    )
    WITH TMP_SUPPORG AS (
                SELECT T1.ORGID AS ORGID
                       ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
                  FROM RWA.RWA_WP_SUPPTASKORG T1
            INNER JOIN RWA.RWA_WP_SUPPTASK T2
                    ON T1.SUPPTASKID = T2.SUPPTASKID
                   AND T2.ENABLEFLAG = '01'
             LEFT JOIN RWA.ORG_INFO T3
                    ON T1.ORGID = T3.ORGID
             LEFT JOIN RWA.ORG_INFO T4
                    ON T3.BELONGORGID = T4.ORGID
                 WHERE T1.SUPPTMPLID = 'M-0190'
              ORDER BY T3.SORTNO
    )
    SELECT
                DATADATE                               --数据日期
                ,ORGID                                 --机构ID
                ,IACAC_NO                              --账号
                ,IACCURBAL                             --余额
                ,IACCRTDAT                             --起始日
                ,IACDLTDAT                             --到日期
                ,BELONGORGCODE                         --业务所属机构
                ,CLIENTNAME                            --交易对手名称
                ,ORGANIZATIONCODE                      --交易对手组织机构代码
                ,COUNTRYCODE                           --交易对手注册国家代码
                ,INDUSTRYID                            --交易对手所属行业代码
                ,CLIENTCATEGORY                        --交易对手客户类型
    FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T4.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                                                             AS ORGID                    --机构ID                     按照补录任务分配情况，默认为总行个人银行部(01280000)
                ,RANK() OVER(PARTITION BY T1.IACAC_NO ORDER BY LENGTH(NVL(T4.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                                                             AS RECORDNO                 --数据序号
                ,T1.IACAC_NO                                 AS IACAC_NO                 --账号
                ,T1.IACCURBAL                                AS IACCURBAL                --余额
                ,NVL(T2.IACCRTDAT,TO_CHAR(TO_DATE(T1.IACCRTDAT,'YYYYMMDD'),'YYYY-MM-DD'))
                                                             AS IACCRTDAT                --起始日
                ,NVL(T2.IACDLTDAT,TO_CHAR(TO_DATE(T1.IACDLTDAT,'YYYYMMDD'),'YYYY-MM-DD'))
                                                             AS IACDLTDAT                --到日期
                ,T1.IACGACBR                                 AS BELONGORGCODE            --业务所属机构
                ,T2.CLIENTNAME                               AS CLIENTNAME               --交易对手名称
                ,T2.ORGANIZATIONCODE                         AS ORGANIZATIONCODE         --交易对手组织机构代码
                ,NVL(T2.COUNTRYCODE,'CHN')                   AS COUNTRYCODE              --交易对手注册国家代码         默认CHN-中国
                ,NVL(T2.INDUSTRYID,'J66')                    AS INDUSTRYID               --交易对手所属行业代码         默认J66-货币金融服务
                ,NVL(T2.CLIENTCATEGORY,'0202')               AS CLIENTCATEGORY           --客户类型                     默认0202-中国商业银行

    FROM        RWA_DEV.CBS_IAC T1
    LEFT JOIN   (SELECT IACAC_NO
                       ,CLIENTNAME
                       ,ORGANIZATIONCODE
                       ,COUNTRYCODE
                       ,INDUSTRYID
                       ,IACCRTDAT
                       ,IACDLTDAT
                       ,CLIENTCATEGORY
                   FROM RWA.RWA_WS_DSBANK_ADV
                  WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_DSBANK_ADV WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
                ) T2                                   --取最近一期补录数据铺底
    ON          T1.IACAC_NO = T2.IACAC_NO
    LEFT JOIN   RWA.ORG_INFO T3
    ON          T1.IACGACBR = T3.ORGID
    LEFT  JOIN  TMP_SUPPORG T4
    ON          T3.SORTNO LIKE T4.SORTNO || '%'
    WHERE       T1.IACITMNO = '13070800'               --直销银行垫款科目号
    AND         T1.IACCURBAL <> 0                      --余额不为0
    AND         T1.DATANO = p_data_dt_str
    )
    WHERE RECORDNO = 1
    ORDER BY    IACAC_NO
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_DSBANK_ADV',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_DSBANK_ADV WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_DSBANK_ADV表当前插入的核心系统-直销银行垫款铺底数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '直销银行垫款补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZX_WSIB;
/

