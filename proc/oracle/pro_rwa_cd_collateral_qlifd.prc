CREATE OR REPLACE PROCEDURE RWA_DEV.pro_rwa_cd_collateral_qlifd(
                                    v_data_dt_str	IN 	VARCHAR2,   --��������
											              v_po_rtncode	OUT	VARCHAR2,   --���ر��
														        v_po_rtnmsg		OUT	VARCHAR2 		--��������)
                                     )
AS
/*
    �洢��������:pro_rwa_cd_collateral_qlifd
    ʵ�ֹ���: �ӹ�rwa_ei_collateral ����ѺƷ�� ��Ȩ�ط� QUALFLAGSTD�������� QUALFLAGFIRB �µĺϸ��ʾ
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2015-07-03
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :�ϸ����ѺƷ�����
    Ŀ���  :����ѺƷ��
    ������  :��
    ��   ע��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
  --������µ�sql���
  v_update_sql VARCHAR2(4000);
  --����ƥ�������ļ�¼
  v_count number(18) := 0;
  --�������ڴ���ж�����
  --ѺƷС�����
  GUARANTY_KIND3 VARCHAR2(300);
  --����һ��������
  REGIST_STATE_CODE VARCHAR2(500);
  --�ⲿ��������
  OUT_JUDEGE_TYPE	VARCHAR2(500);
  --��Ȩ���ʴ���
  OWNER_PROPERTY_TYPE	VARCHAR2(500);
  --����Ŀ�Ĵ���
  ISSUE_INTENT_TYPE	VARCHAR2(200);
  --ԭʼ����
  ORIGINALMATURITY VARCHAR2(100);
  --�ϸ��ʶ
  QUALIFIED_FLAG VARCHAR2(10);
BEGIN
  DECLARE
   --ͬ���α��ȡ��Ҫ���ж�����
  CURSOR c_cursor IS
  SELECT
    CASE WHEN GUARANTY_KIND3 IS NOT NULL
         THEN ' AND SOURCECOLSUBTYPE='''||GUARANTY_KIND3||''' '
         ELSE ''
    END AS GUARANTY_KIND3    --ѺƷС�����
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
     END AS REGIST_STATE_CODE    --ע����ҵ�������
     ,/*CASE WHEN OUT_JUDEGE_TYPE IS NULL THEN*/ ''
			  	 /* WHEN INSTR(OUT_JUDEGE_TYPE,',')>0 THEN 'AND EXISTS(SELECT 1 FROM RWA_CD_O_LEVEL_TYPE T4 WHERE T.FCIssueRating=T4.SP_LEVEL AND (T4.LEVEL_NO='''||SUBSTR(OUT_JUDEGE_TYPE,0,INSTR(OUT_JUDEGE_TYPE,',')-1)||' AND T4.LEVEL_NO'|| SUBSTR(OUT_JUDEGE_TYPE,INSTR(OUT_JUDEGE_TYPE,',')+1)||''')) '
						ELSE 'AND EXISTS(SELECT 1 FROM RWA_CD_O_LEVEL_TYPE T4 WHERE T.FCIssueRating=T4.SP_LEVEL AND T4.LEVEL_NO='''||OUT_JUDEGE_TYPE||''')'
					   END*/ AS OUT_JUDEGE_TYPE  --�ⲿ��������
    ,CASE WHEN OWNER_PROPERTY_TYPE IS NOT NULL AND EARTH_PROPERTY_TYPE IS NOT NULL
           THEN 'AND EXISTS(SELECT 1 FROM BSG_GUAR_INFO T5 WHERE T.CollateralID=T5.guaranty_code AND T5.DEF_CHR300_013='''||OWNER_PROPERTY_TYPE||''' AND T5.DEF_CHR300_024='''||EARTH_PROPERTY_TYPE||''') '
           WHEN OWNER_PROPERTY_TYPE IS NOT NULL AND EARTH_PROPERTY_TYPE IS  NULL
           THEN 'AND EXISTS(SELECT 1 FROM BSG_GUAR_INFO T5 WHERE T.CollateralID=T5.guaranty_code AND T5.DEF_CHR300_013='''||OWNER_PROPERTY_TYPE||''') '
           WHEN OWNER_PROPERTY_TYPE IS NULL AND EARTH_PROPERTY_TYPE IS NOT NULL
           THEN 'AND EXISTS(SELECT 1 FROM BSG_GUAR_INFO T5 WHERE T.CollateralID=T5.guaranty_code AND T5.DEF_CHR300_024='''||EARTH_PROPERTY_TYPE||''') '
           ELSE ''
     END AS OWNER_PROPERTY_TYPE    --��Ȩ���ʴ���
    ,CASE WHEN ISSUE_INTENT_TYPE IS NOT NULL
          THEN 'AND EXISTS(SELECT 1 FROM RWA_EI_EXPOSURE T2 WHERE T.issuerid = T2.SecuIssuerID AND T.DataNO=T2.DataNO AND T2.BondIssueIntent='''||ISSUE_INTENT_TYPE||''' ) '
          ELSE ''
     END  AS ISSUE_INTENT_TYPE  --����Ŀ��
    ,CASE WHEN ORIGINALMATURITY IS NOT NULL
         THEN ' AND ORIGINALMATURITY'||ORIGINALMATURITY||''
         ELSE ''
     END AS ORIGINALMATURITY    --ԭʼ����
    ,QUALIFIED_FLAG   --�ϸ��ʶ ��ֵ:QualificationFlag
    FROM RWA_CD_COLLATERAL_QUALIFIED  --����ѺƷ�ϸ�ӳ���
    ORDER BY GUARANTY_KIND3;
  BEGIN
    --�����α�
    OPEN c_cursor;
    --ͨ��ѭ�������������α�
    DBMS_OUTPUT.PUT_LINE('>>>>>>Update��俪ʼִ����>>>>>>>');
    LOOP
      v_count := v_count + 1;
      -- ���α��ȡ��ֵ���趨���ƥ������
      FETCH c_cursor INTO
        GUARANTY_KIND3       --ѺƷ�������
       ,REGIST_STATE_CODE    --ע����ҵ�������
       ,OUT_JUDEGE_TYPE      --������.�ⲿ�������� ��ֵ:IRBRatingGroup
       ,OWNER_PROPERTY_TYPE  --��Ȩ���ʴ���
       ,ISSUE_INTENT_TYPE  --Ȩ�ط�.����Ŀ�Ĵ��� ��ֵ:BondPublishPurpose
       ,ORIGINALMATURITY  --ԭʼ����
       ,QUALIFIED_FLAG  --�ϸ��ʶ ��ֵ:QualificationFlag
        ;
      --���α������ɺ��˳��α�
      EXIT WHEN c_cursor%NOTFOUND;
      IF QUALIFIED_FLAG ='01' THEN  --Ȩ�ط����������ϸ�
        v_update_sql:='UPDATE rwa_ei_collateral T SET T.QUALFLAGSTD=''1'', T.QUALFLAGFIRB=''1''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      ELSIF QUALIFIED_FLAG='02' THEN  --Ȩ�ط����ϸ��������ϸ�
        v_update_sql:='UPDATE rwa_ei_collateral T SET T.QUALFLAGSTD=''0'', T.QUALFLAGFIRB=''1''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      ELSE --QUALIFIED_FLAG ='03' THEN  --Ȩ�ط������������ϸ�  --04 Ȩ�ط��ϸ����������ϸ�
        v_update_sql:='UPDATE rwa_ei_collateral T SET T.QUALFLAGSTD=''0'', T.QUALFLAGFIRB=''0''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      END IF;
      --�ϲ�sql��ƴ��where����
      v_update_sql := v_update_sql
      ||GUARANTY_KIND3
      ||REGIST_STATE_CODE
      ||OUT_JUDEGE_TYPE
      ||OWNER_PROPERTY_TYPE
      ||ISSUE_INTENT_TYPE
      ||ORIGINALMATURITY
      ;
--     DBMS_OUTPUT.PUT_LINE(v_update_sql);
      --ִ��sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --����ѭ��
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('�������ܼ�¼Ϊ��'|| v_count);
    DBMS_OUTPUT.PUT_LINE('Update����Ѿ�ִ�н���������');
      --�ر��α�
    CLOSE c_cursor;
  END;
----------------------------------------ѭ�������������������Ķ����ϸ�----------------
    UPDATE rwa_ei_collateral SET QUALFLAGSTD=0 WHERE datadate=TO_DATE(v_data_dt_str,'YYYYMMDD') AND QUALFLAGSTD IS NULL;
    UPDATE rwa_ei_collateral SET QUALFLAGFIRB=0 WHERE datadate=TO_DATE(v_data_dt_str,'YYYYMMDD') AND QUALFLAGFIRB IS NULL;
    COMMIT;
    v_po_rtncode := '1';
		v_po_rtnmsg  := '�ɹ�';
		--�����쳣
		EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
         v_po_rtncode := sqlcode;
         v_po_rtnmsg  := '�ϸ����ѺƷӳ�����'|| sqlerrm;
         RETURN;
END pro_rwa_cd_collateral_qlifd;
/

