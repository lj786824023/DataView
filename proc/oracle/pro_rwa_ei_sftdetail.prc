CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_SFTDETAIL(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期 yyyyMMdd
                             p_po_rtncode  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                            p_po_rtnmsg    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_SFTDETAIL
    实现功能:插入所有买断回购信息
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-06-01
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_HG_SFTDETAIL|买断回购表
    目标表  :RWA_DEV.RWA_EI_SFTDETAIL|汇总买断回购表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_SFTDETAIL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_SFTDETAIL DROP PARTITION SFTDETAIL' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总参与主体表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_SFTDETAIL ADD PARTITION SFTDETAIL' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入回购类的买断回购信息信息*/
    INSERT INTO RWA_DEV.RWA_EI_SFTDETAIL(
                DATADATE                               --数据日期
                ,DATANO                                  --数据流水号
                ,SFTDETAILID                             --证券融资交易明细ID
                ,MASTERNETAGREEID                        --净额结算主协议ID
                ,EXPOSUREID                              --风险暴露ID
                ,SECUID                                  --证券ID
                ,SSYSID                                  --源系统ID
                ,BOOKTYPE                                --账户类别
                ,TRANROLE                                --交易角色
                ,TRADINGASSETTYPE                        --交易资产类型
                ,CLAIMSLEVEL                             --债权级别
                ,QUALFLAGSTD                             --权重法合格标识
                ,QUALFLAGFIRB                            --内评初级法合格标识
                ,COLLATERALSDVSSTD                      --权重法抵质押品细分
                ,STARTDATE                               --起始日期
                ,DUEDATE                                 --到期日期
                ,ORIGINALMATURITY                        --原始期限
                ,RESIDUALM                               --剩余期限
                ,ASSETBALANCE                            --资产余额
                ,ASSETCURRENCY                           --资产币种
                ,APPZEROHAIRCUTSFLAG                     --是否适用零折扣系数
                ,INTEHAIRCUTSFLAG                        --自行估计折扣系数标识
                ,INTERNALHC                             --内部折扣系数
                ,SECUISSUERID                            --证券发行人ID
                ,BONDISSUEINTENT                         --债券发行目的
                ,FCTYPE                                  --金融质押品类型
                ,ABSFLAG                                 --资产证券化标识
                ,RATINGDURATIONTYPE                      --评级期限类型
                ,SECUISSUERATING                         --证券发行等级
                ,SECUREVAFREQUENCY                       --证券重估频率
                ,RCERating														 	 --发行人境外注册地外部评级
    )
    SELECT
                DATADATE                                AS DATADATE                 --数据日期
                ,DATANO                                 AS DATANO                   --数据流水号
                ,SFTDETAILID                             AS SFTDETAILID               --证券融资交易明细ID
                ,MASTERNETAGREEID                       AS MASTERNETAGREEID          --净额结算主协议ID
                ,EXPOSUREID                             AS EXPOSUREID                --风险暴露ID
                ,SECUID                                   AS SECUID                    --证券ID
                ,SSYSID                                  AS SSYSID                    --源系统ID
                ,BOOKTYPE                                 AS BOOKTYPE                   --账户类别
                ,TRANROLE                                 AS TRANROLE                  --交易角色
                ,TRADINGASSETTYPE                        AS TRADINGASSETTYPE          --交易资产类型
                ,CLAIMSLEVEL                            AS CLAIMSLEVEL               --债权级别
                ,QUALFLAGSTD                            AS QUALFLAGSTD               --权重法合格标识
                ,QUALFLAGFIRB                            AS QUALFLAGFIRB              --内评初级法合格标识
                ,COLLATERALSDVSSTD                       AS COLLATERALSDVSSTD         --权重法抵质押品细分
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                        AS STARTDATE                --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                        AS DUEDATE                   --到期日期
                ,ORIGINALMATURITY                       AS ORIGINALMATURITY          --原始期限
                ,RESIDUALM                              AS RESIDUALM                 --剩余期限
                ,ASSETBALANCE                            AS ASSETBALANCE              --资产余额
                ,ASSETCURRENCY                          AS ASSETCURRENCY             --资产币种
                ,APPZEROHAIRCUTSFLAG                     AS APPZEROHAIRCUTSFLAG       --是否适用零折扣系数
                ,INTEHAIRCUTSFLAG                       AS INTEHAIRCUTSFLAG          --自行估计折扣系数标识
                ,INTERNALHC                             AS INTERNALHC                --内部折扣系数
                ,SECUISSUERID                           AS SECUISSUERID             --证券发行人ID
                ,BONDISSUEINTENT                        AS BONDISSUEINTENT           --债券发行目的
                ,FCTYPE                                 AS FCTYPE                    --金融质押品类型
                ,ABSFLAG                                AS ABSFLAG                   --资产证券化标识
                ,RATINGDURATIONTYPE                      AS RATINGDURATIONTYPE        --评级期限类型
                ,SECUISSUERATING                         AS SECUISSUERATING          --证券发行等级
                ,SECUREVAFREQUENCY                      AS SECUREVAFREQUENCY         --证券重估频率
                ,RCERating														 	AS RCERating								 --发行人境外注册地外部评级

    FROM   			RWA_DEV.RWA_HG_SFTDETAIL
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_SFTDETAIL',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_SFTDETAIL',partname => 'SFTDETAIL'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_SFTDETAIL WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_SFTDETAIL表当前插入买断回购记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '买断回购('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_SFTDETAIL;
/

