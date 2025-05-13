CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_STD_EXPOSURE_TYPE(
                                 p_data_dt_str  IN   VARCHAR2,   --��������
                                 p_po_rtncode    OUT  VARCHAR2,   --���ر��
                                 p_po_rtnmsg    OUT  VARCHAR2     --��������
                                 )
AS
/*
    �洢��������:PRO_RWA_CD_SUBJECT_ASSET
    ʵ�ֹ���:���±�¶���еı�¶����ͱ�¶С��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-08-23
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :���÷��ձ�¶��
    Ŀ���  :RWA_CD_STD_EXPOSURE_TYPE
    ������  :��
    ��   ע��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
    --�����쳣����
  v_raise EXCEPTION;
  --������µ�sql���
  v_update_sql VARCHAR2(4000);
  --����ƥ�������ļ�¼
  v_count number(18);
  --�������ڴ���ж�����

  --�ʲ�С�����
  ASSET_SUB_TYPE VARCHAR2(200);
  --��������С��
  CLIENT_SUB_TYPE VARCHAR2(4000);
   --ע��ع�������
  COUNTRY_RATTING VARCHAR2(4000);
  --ծȨ�ȼ�
  CLAIMSLEVEL VARCHAR2(200);
  --ծȯ����Ŀ��
  BONDISSUEINTENT VARCHAR2(200);
  --ԭʼ����
  ORIGINAL_MATURITY VARCHAR2(200);
  --ҵ��Ʒ��
  BUSINESS_TYPE VARCHAR2(200);
  --��ȨͶ���γ�ԭ��
  EQUITYINVESTCAUSE VARCHAR2(200);
  --��Ŀ��
  SUBJECT_NO VARCHAR2(200);
  --�Ƿ�����ò�����
  NSUREALPROPERTYFLAG VARCHAR2(200);
  --��¶����
  EXPOCLASSSTD VARCHAR2(100);
  --��¶С��
  EXPOSUBCLASSSTD VARCHAR2(100);


BEGIN

     --ִ�и��±�¶��С��֮ǰ�������б�¶��С���ÿ�
     /*��Ҫ�ų������̫���ˣ��ɴ��ֱ�Ӳ��ÿգ���Ϊÿ��ִ�б�¶���࣬����Ҫȫ������ִ��ETL
     UPDATE RWA_DEV.RWA_EI_EXPOSURE
		    SET EXPOCLASSSTD = NULL, EXPOSUBCLASSSTD = NULL
		  WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		    AND CASE
		          WHEN SSYSID NOT IN ('XYK', 'ABS', 'TZ', 'XD', 'TY','GQ','JDYS') THEN
		           '1'
		          WHEN SSYSID = 'TZ' AND CLIENTID NOT IN ('MZZXZ', 'XN-YBGS') THEN
		           '1'
		          WHEN SSYSID IN ('XD', 'TY') AND CLIENTID NOT IN ('XN-GEKH', 'XN-YBGS') THEN
		           '1'
		          WHEN EXPOSUREID LIKE 'YPWF%' OR EXPOSUREID LIKE 'MDSP%' OR EXPOSUREID LIKE 'MDYP%' THEN
		           '0'
		          ELSE
		           '0'
		        END = '1'
     ;
     COMMIT;
     */

  DECLARE
   --ͬ���α��ȡ��Ҫ���ж�����
  CURSOR c_cursor IS
    SELECT
    CASE WHEN ASSET_SUB_TYPE IS NOT NULL
         THEN ' AND AssetSubType= '''||ASSET_SUB_TYPE||''' '
         ELSE ''
    END  AS ASSET_SUB_TYPE    --�ʲ�С��
    ,CASE WHEN CLAIMSLEVEL IS NOT NULL
         THEN ' AND ClaimsLevel= '''||CLAIMSLEVEL||''' '
         ELSE ''
    END  AS CLAIMSLEVEL    --ծȨ�ȼ�
    ,CASE WHEN BONDISSUEINTENT IS NOT NULL
         THEN ' AND BONDISSUEINTENT= '''||BONDISSUEINTENT||''' '
         ELSE ''
    END  AS BONDISSUEINTENT    --ծȯ����Ŀ��
    ,CASE WHEN ORIGINAL_MATURITY='01'
         THEN ' AND OriginalMaturity<=0.25 '
         WHEN ORIGINAL_MATURITY='02'
         THEN ' AND OriginalMaturity>0.25 '
         WHEN ORIGINAL_MATURITY='05'
         THEN ' AND OriginalMaturity<=2 '
         WHEN ORIGINAL_MATURITY='06'
         THEN ' AND OriginalMaturity>2 '
         ELSE ''
    END  AS ORIGINAL_MATURITY    --ԭʼ����
    ,CASE WHEN BUSINESS_TYPE IS NOT NULL
         THEN ' AND BusinessTypeID='''||BUSINESS_TYPE||''' '
         ELSE ''
    END  AS BUSINESS_TYPE    --ҵ��Ʒ��
    ,CASE WHEN EQUITYINVESTCAUSE IS NOT NULL
         THEN ' AND EQUITYINVESTCAUSE= '''||EQUITYINVESTCAUSE||''' '
         ELSE ''
    END  AS EQUITYINVESTCAUSE    --��ȨͶ���γ�ԭ��
    ,CASE WHEN SUBJECT_NO IS NOT NULL
         THEN ' AND AccSubject1 LIKE '''||SUBJECT_NO||'%'' '
         ELSE ''
    END  AS SUBJECT_NO    --��Ŀ��
    ,CASE WHEN NSUREALPROPERTYFLAG IS NOT NULL
         THEN ' AND NSUREALPROPERTYFLAG= '''||NSUREALPROPERTYFLAG||''' '
         ELSE ''
    END  AS NSUREALPROPERTYFLAG    --�Ƿ�����ò�����
    ,CASE WHEN CLIENT_SUB_TYPE IS NOT NULL AND SSMBFlag IS NOT NULL AND ORGANIZATIONCODE IS NOT NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType LIKE '''||CLIENT_SUB_TYPE||'%'' AND T1.SSMBFLAGSTD='''||SSMBFlag||''' AND REPLACE(T1.ORGANIZATIONCODE,''-'','''')='''||ORGANIZATIONCODE||''') '
          WHEN CLIENT_SUB_TYPE IS NOT NULL AND SSMBFlag IS NOT NULL AND ORGANIZATIONCODE IS  NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType LIKE '''||CLIENT_SUB_TYPE||'%'' AND T1.SSMBFLAGSTD='''||SSMBFlag||''') '
          WHEN CLIENT_SUB_TYPE IS NOT NULL AND SSMBFlag IS  NULL AND ORGANIZATIONCODE IS NOT  NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType LIKE '''||CLIENT_SUB_TYPE||'%'' AND REPLACE(T1.ORGANIZATIONCODE,''-'','''')='''||ORGANIZATIONCODE||''') '
          WHEN CLIENT_SUB_TYPE IS  NULL AND SSMBFlag IS  NULL AND ORGANIZATIONCODE IS NOT  NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND REPLACE(T1.ORGANIZATIONCODE,''-'','''')='''||ORGANIZATIONCODE||''') '
          WHEN CLIENT_SUB_TYPE IS NOT NULL AND SSMBFlag IS  NULL AND ORGANIZATIONCODE IS  NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType LIKE '''||CLIENT_SUB_TYPE||'%'')'
          ELSE ''
    END  AS CLIENT_SUB_TYPE    --��������С��,��׼С΢��ҵ��ʶ
    ,CASE WHEN COUNTRY_RATTING='01'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating<=''0104'') '  -- AA-������
          WHEN COUNTRY_RATTING='02'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0104'' AND T1.RCERating<=''0107'') ' --AA-�����£�A-��������
          WHEN COUNTRY_RATTING='03'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0107'' AND T1.RCERating<=''0110'') ' --A-�����£�BBB-��������
          WHEN COUNTRY_RATTING='04'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0110'' AND T1.RCERating<=''0116'') ' --BBB-�����£�B-��������
          WHEN COUNTRY_RATTING='09'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0107'' AND T1.RCERating<=''0116'') ' --A-�����£�B-��������
          WHEN COUNTRY_RATTING='05'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0116'' AND T1.RCERating<''0124'') ' --B-������
          WHEN COUNTRY_RATTING='06'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating=''0124'') ' --δ����
          ELSE ''
     END AS COUNTRY_RATTING
    ,EXPOCLASSSTD     --��¶�������
    ,EXPOSUBCLASSSTD  --��¶С�����
    FROM RWA_CD_STD_EXPOSURE_TYPE
          --  WHERE SERIALNO<>'20160819000000000203'

        ORDER BY SORTNO
    ;

  BEGIN
    --�����α�
    OPEN c_cursor;
    --ͨ��ѭ�������������α�
    LOOP
      --���α��ȡ��ֵ���趨���ƥ������
      FETCH c_cursor INTO
        ASSET_SUB_TYPE
       ,CLIENT_SUB_TYPE
       ,COUNTRY_RATTING
       ,CLAIMSLEVEL
       ,BONDISSUEINTENT
       ,ORIGINAL_MATURITY
       ,BUSINESS_TYPE
       ,EQUITYINVESTCAUSE
       ,SUBJECT_NO
       ,NSUREALPROPERTYFLAG
       ,EXPOCLASSSTD
       ,EXPOSUBCLASSSTD
       ;
      --���α������ɺ��˳��α�
      EXIT WHEN c_cursor%NOTFOUND;
      v_update_sql:='UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.EXPOCLASSSTD='''||EXPOCLASSSTD||''', T.EXPOSUBCLASSSTD='''||EXPOSUBCLASSSTD||''' WHERE T.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') 
                AND T.EXPOCLASSSTD IS NULL AND T.EXPOSUBCLASSSTD IS NULL ';

      --�ϲ�sql��ƴ��where����
      v_update_sql := v_update_sql
      ||ASSET_SUB_TYPE
      ||CLIENT_SUB_TYPE
      ||COUNTRY_RATTING
      ||CLAIMSLEVEL
      ||BONDISSUEINTENT
      ||ORIGINAL_MATURITY
      ||BUSINESS_TYPE
      ||EQUITYINVESTCAUSE
      ||SUBJECT_NO
      ||NSUREALPROPERTYFLAG
      ;
      --ִ��sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --����ѭ��
    END LOOP;
      --�ر��α�
    CLOSE c_cursor;
  END;
    --ͨ����¶С�����Ȩ�ط�ҵ������
    UPDATE RWA_DEV.RWA_EI_EXPOSURE T1
        SET T1.BUSINESSTYPESTD=(SELECT T2.BUSINESSSTDTYPE
                             FROM RWA_DEV.RWA_CD_STD_BUSINESS_TYPE T2
                             WHERE T1.EXPOSUBCLASSSTD=T2.EXPOSUBCLASSSTD
                             )
    WHERE T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    
     -----------------���³��⾻����㹤�߱�¶���   BY WZB 20191130---
    MERGE INTO RWA_EI_OTCNETTING A
USING (SELECT T.VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID,
              MAX(T.EXPOCLASSSTD) AS EXPOCLASSSTD ,
              MAX(T.EXPOSUBCLASSSTD) AS EXPOSUBCLASSSTD,
              MAX(T.EXPOCLASSIRB) AS EXPOCLASSIRB,
              MAX(T.EXPOSUBCLASSIRB) AS EXPOSUBCLASSIRB
         FROM rwa_ei_exposure T
        WHERE SSYSID = 'YSP'
          AND DATANO = p_data_dt_str
        GROUP BY VALIDNETAGREEMENTID) B
on (A.VALIDNETAGREEMENTID = B.VALIDNETAGREEMENTID AND A.DATANO = p_data_dt_str)
 WHEN MATCHED 
   THEN UPDATE SET A.EXPOCLASSSTD=B.EXPOCLASSSTD,
                   A.EXPOSUBCLASSSTD=B.EXPOSUBCLASSSTD,
                   A.EXPOCLASSIRB=B.EXPOCLASSIRB,
                   A.EXPOSUBCLASSIRB=B.EXPOSUBCLASSIRB;
     COMMIT;


    
    
    --ͳ��û�и��³ɹ��ı�¶��С������
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATANO=p_data_dt_str AND ExpoSubClassSTD IS  NULL;
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '����Ȩ�ط���¶��С��(PRO_RWA_CD_STD_EXPOSURE_TYPE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_STD_EXPOSURE_TYPE;
/

