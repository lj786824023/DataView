CREATE OR REPLACE PROCEDURE RWA_DEV.pro_rwa_cd_collateral_qlifd(
                                    v_data_dt_str	IN 	VARCHAR2,   --数据日期
											              v_po_rtncode	OUT	VARCHAR2,   --返回编号
														        v_po_rtnmsg		OUT	VARCHAR2 		--返回描述)
                                     )
AS
/*
    存储过程名称:pro_rwa_cd_collateral_qlifd
    实现功能: 加工rwa_ei_collateral 抵质押品表 中权重法 QUALFLAGSTD、内评法 QUALFLAGFIRB 下的合格标示
    数据口径:全量
    跑批频率:月末
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2015-07-03
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :合格抵质押品代码表
    目标表  :抵质押品表
    辅助表  :无
    备   注：
    变更记录(修改人|修改时间|修改内容)：
  */
  --定义更新的sql语句
  v_update_sql VARCHAR2(4000);
  --定义匹配条件的记录
  v_count number(18) := 0;
  --定义用于存放判断条件
  --押品小类代码
  GUARANTY_KIND3 VARCHAR2(300);
  --册国家或地区代码
  REGIST_STATE_CODE VARCHAR2(500);
  --外部评级代码
  OUT_JUDEGE_TYPE	VARCHAR2(500);
  --产权性质代码
  OWNER_PROPERTY_TYPE	VARCHAR2(500);
  --发行目的代码
  ISSUE_INTENT_TYPE	VARCHAR2(200);
  --原始期限
  ORIGINALMATURITY VARCHAR2(100);
  --合格标识
  QUALIFIED_FLAG VARCHAR2(10);
BEGIN
  DECLARE
   --同过游标读取需要的判断条件
  CURSOR c_cursor IS
  SELECT
    CASE WHEN GUARANTY_KIND3 IS NOT NULL
         THEN ' AND SOURCECOLSUBTYPE='''||GUARANTY_KIND3||''' '
         ELSE ''
    END AS GUARANTY_KIND3    --押品小类代码
    ,CASE WHEN REGIST_STATE_CODE IS NOT NULL and CUSTOMER_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T1 WHERE T.issuerid = T1.ClientID AND T.DataNO=T1.DataNO AND T1.RegistState='''||REGIST_STATE_CODE||''' AND T1.ClientSubType='''||CUSTOMER_TYPE||''' AND T1.RCERating='''||COUNTRY_LEVEL_NO||''') '
         WHEN REGIST_STATE_CODE IS NULL and CUSTOMER_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T1 WHERE T.issuerid = T1.ClientID AND T.DataNO=T1.DataNO AND T1.ClientSubType='''||CUSTOMER_TYPE||''' AND T1.RCERating='''||COUNTRY_LEVEL_NO||''') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CUSTOMER_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T1 WHERE T.issuerid = T1.ClientID AND T.DataNO=T1.DataNO AND T1.RegistState='''||REGIST_STATE_CODE||''' AND T1.RCERating='''||COUNTRY_LEVEL_NO||''') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CUSTOMER_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T1 WHERE T.issuerid = T1.ClientID AND T.DataNO=T1.DataNO AND T1.RegistState='''||REGIST_STATE_CODE||''' AND T1.ClientSubType='''||CUSTOMER_TYPE||''') '
         WHEN REGIST_STATE_CODE IS NULL and CUSTOMER_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T1 WHERE T.issuerid = T1.ClientID AND T.DataNO=T1.DataNO AND T1.RCERating='''||COUNTRY_LEVEL_NO||''') '
         WHEN REGIST_STATE_CODE IS NULL and CUSTOMER_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T1 WHERE T.issuerid = T1.ClientID AND T.DataNO=T1.DataNO AND T1.ClientSubType='''||CUSTOMER_TYPE||''') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CUSTOMER_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T1 WHERE T.issuerid = T1.ClientID AND T.DataNO=T1.DataNO AND T1.RegistState='''||REGIST_STATE_CODE||''' ) '
         ELSE ''
     END AS REGIST_STATE_CODE    --注册国家地区代码
     ,/*CASE WHEN OUT_JUDEGE_TYPE IS NULL THEN*/ ''
			  	 /* WHEN INSTR(OUT_JUDEGE_TYPE,',')>0 THEN 'AND EXISTS(SELECT 1 FROM RWA_CD_O_LEVEL_TYPE T4 WHERE T.FCIssueRating=T4.SP_LEVEL AND (T4.LEVEL_NO='''||SUBSTR(OUT_JUDEGE_TYPE,0,INSTR(OUT_JUDEGE_TYPE,',')-1)||' AND T4.LEVEL_NO'|| SUBSTR(OUT_JUDEGE_TYPE,INSTR(OUT_JUDEGE_TYPE,',')+1)||''')) '
						ELSE 'AND EXISTS(SELECT 1 FROM RWA_CD_O_LEVEL_TYPE T4 WHERE T.FCIssueRating=T4.SP_LEVEL AND T4.LEVEL_NO='''||OUT_JUDEGE_TYPE||''')'
					   END*/ AS OUT_JUDEGE_TYPE  --外部评级代码
    ,CASE WHEN OWNER_PROPERTY_TYPE IS NOT NULL AND EARTH_PROPERTY_TYPE IS NOT NULL
           THEN 'AND EXISTS(SELECT 1 FROM BSG_GUAR_INFO T5 WHERE T.CollateralID=T5.guaranty_code AND T5.DEF_CHR300_013='''||OWNER_PROPERTY_TYPE||''' AND T5.DEF_CHR300_024='''||EARTH_PROPERTY_TYPE||''') '
           WHEN OWNER_PROPERTY_TYPE IS NOT NULL AND EARTH_PROPERTY_TYPE IS  NULL
           THEN 'AND EXISTS(SELECT 1 FROM BSG_GUAR_INFO T5 WHERE T.CollateralID=T5.guaranty_code AND T5.DEF_CHR300_013='''||OWNER_PROPERTY_TYPE||''') '
           WHEN OWNER_PROPERTY_TYPE IS NULL AND EARTH_PROPERTY_TYPE IS NOT NULL
           THEN 'AND EXISTS(SELECT 1 FROM BSG_GUAR_INFO T5 WHERE T.CollateralID=T5.guaranty_code AND T5.DEF_CHR300_024='''||EARTH_PROPERTY_TYPE||''') '
           ELSE ''
     END AS OWNER_PROPERTY_TYPE    --产权性质代码
    ,CASE WHEN ISSUE_INTENT_TYPE IS NOT NULL
          THEN 'AND EXISTS(SELECT 1 FROM RWA_EI_EXPOSURE T2 WHERE T.issuerid = T2.SecuIssuerID AND T.DataNO=T2.DataNO AND T2.BondIssueIntent='''||ISSUE_INTENT_TYPE||''' ) '
          ELSE ''
     END  AS ISSUE_INTENT_TYPE  --发行目的
    ,CASE WHEN ORIGINALMATURITY IS NOT NULL
         THEN ' AND ORIGINALMATURITY'||ORIGINALMATURITY||''
         ELSE ''
     END AS ORIGINALMATURITY    --原始期限
    ,QUALIFIED_FLAG   --合格标识 码值:QualificationFlag
    FROM RWA_CD_COLLATERAL_QUALIFIED  --抵质押品合格映射表
    ORDER BY GUARANTY_KIND3;
  BEGIN
    --开启游标
    OPEN c_cursor;
    --通过循环来遍历检索游标
    DBMS_OUTPUT.PUT_LINE('>>>>>>Update语句开始执行中>>>>>>>');
    LOOP
      v_count := v_count + 1;
      -- 将游标获取的值赋予定义的匹配条件
      FETCH c_cursor INTO
        GUARANTY_KIND3       --押品三类代码
       ,REGIST_STATE_CODE    --注册国家地区代码
       ,OUT_JUDEGE_TYPE      --内评法.外部评级代码 码值:IRBRatingGroup
       ,OWNER_PROPERTY_TYPE  --产权性质代码
       ,ISSUE_INTENT_TYPE  --权重法.发行目的代码 码值:BondPublishPurpose
       ,ORIGINALMATURITY  --原始期限
       ,QUALIFIED_FLAG  --合格标识 码值:QualificationFlag
        ;
      --档游标检索完成后退出游标
      EXIT WHEN c_cursor%NOTFOUND;
      IF QUALIFIED_FLAG ='01' THEN  --权重法内评法都合格
        v_update_sql:='UPDATE rwa_ei_collateral T SET T.QUALFLAGSTD=''1'', T.QUALFLAGFIRB=''1''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      ELSIF QUALIFIED_FLAG='02' THEN  --权重法不合格、内评法合格
        v_update_sql:='UPDATE rwa_ei_collateral T SET T.QUALFLAGSTD=''0'', T.QUALFLAGFIRB=''1''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      ELSE --QUALIFIED_FLAG ='03' THEN  --权重法内评法都不合格  --04 权重法合格、内评法不合格
        v_update_sql:='UPDATE rwa_ei_collateral T SET T.QUALFLAGSTD=''0'', T.QUALFLAGFIRB=''0''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      END IF;
      --合并sql，拼接where条件
      v_update_sql := v_update_sql
      ||GUARANTY_KIND3
      ||REGIST_STATE_CODE
      ||OUT_JUDEGE_TYPE
      ||OWNER_PROPERTY_TYPE
      ||ISSUE_INTENT_TYPE
      ||ORIGINALMATURITY
      ;
--     DBMS_OUTPUT.PUT_LINE(v_update_sql);
      --执行sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --结束循环
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('遍历的总记录为：'|| v_count);
    DBMS_OUTPUT.PUT_LINE('Update语句已经执行结束！！！');
      --关闭游标
    CLOSE c_cursor;
  END;
----------------------------------------循环结束，不符合条件的都不合格----------------
    UPDATE rwa_ei_collateral SET QUALFLAGSTD=0 WHERE datadate=TO_DATE(v_data_dt_str,'YYYYMMDD') AND QUALFLAGSTD IS NULL;
    UPDATE rwa_ei_collateral SET QUALFLAGFIRB=0 WHERE datadate=TO_DATE(v_data_dt_str,'YYYYMMDD') AND QUALFLAGFIRB IS NULL;
    COMMIT;
    v_po_rtncode := '1';
		v_po_rtnmsg  := '成功';
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
         v_po_rtncode := sqlcode;
         v_po_rtnmsg  := '合格抵质押品映射出错：'|| sqlerrm;
         RETURN;
END pro_rwa_cd_collateral_qlifd;
/

