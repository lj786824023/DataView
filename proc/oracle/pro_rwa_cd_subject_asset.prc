CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_SUBJECT_ASSET(
                                 p_data_dt_str  IN   VARCHAR2,   --��������
                                 p_po_rtncode    OUT  VARCHAR2,   --���ر��
                                 p_po_rtnmsg    OUT  VARCHAR2     --��������
                                 )
AS
/*
    �洢��������:PRO_RWA_CD_SUBJECT_ASSET
    ʵ�ֹ���:���±�¶��ͺ�ͬ���е��ʲ���С��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-08-18
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :���÷��ձ�¶��
    Դ  ��2  :���÷��պ�ͬ��
    Դ  ��3  :���������
    Ŀ���  :RWA_CD_SUBJECT_ASSET
    ������  :��
    ��   ע��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
    --�����쳣����
  v_raise EXCEPTION;
  --������µ�sql���
  v_update_sql VARCHAR2(2000);
  --����ƥ�������ļ�¼
  v_count number(18);
  --�������ڴ���ж�����

  --��Ŀ��
  SUBJECT_NO VARCHAR2(100);
  --��������С��
  CLIENT_SUB_TYPE VARCHAR2(300);
   --ԭʼ����
  ORIGINAL_MATURITY VARCHAR2(300);
  --ҵ��Ʒ�ִ���
  BUSINESS_TYPE VARCHAR2(300);
  --�ʲ��������
  ASSET_TYPE VARCHAR2(50);
  --�ʲ�С�����
  ASSET_SUB_TYPE VARCHAR2(50);

BEGIN

     --ִ�и��±�¶����  �ʲ���С��֮ǰ���������ʲ���С���ÿ�
     UPDATE RWA_DEV.RWA_EI_EXPOSURE SET AssetType=NULL,AssetSubType=NULL
     WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
     AND SSYSID NOT IN ('XYK','ABS','DZ')
     ;
     COMMIT;
  DECLARE
      --ͬ���α��ȡ��Ҫ���ж�����
      CURSOR c_cursor IS
        SELECT
        CASE WHEN SUBJECT_NO IS NOT NULL
             THEN ' AND AccSubject1 LIKE '''||SUBJECT_NO||'%'' '
             ELSE ''
        END  AS SUBJECT_NO    --��Ŀ��
        ,CASE WHEN CLIENT_SUB_TYPE IS NOT NULL
              THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType='''||CLIENT_SUB_TYPE||''') '
               ELSE ''
        END  AS CLIENT_SUB_TYPE    --��������С��
        ,CASE WHEN ORIGINAL_MATURITY ='03'
              THEN ' AND OriginalMaturity<=1 '
              WHEN ORIGINAL_MATURITY ='04'
              THEN ' AND OriginalMaturity>1 '
              ELSE ''
         END AS ORIGINAL_MATURITY     --ԭʼ����
        ,CASE WHEN BUSINESS_TYPE IS NOT NULL
             THEN ' AND BusinessTypeID='''||BUSINESS_TYPE||''' '
             ELSE ''
        END AS BUSINESS_TYPE     --ԭʼ����
        ,ASSET_TYPE   --��¶�������
        ,ASSET_SUB_TYPE  --��¶С�����
        FROM RWA_CD_SUBJECT_ASSET
        ORDER BY SUBJECT_NO,CLIENT_SUB_TYPE,BUSINESS_TYPE ASC
        ;

  BEGIN
    
    --�����α�
    OPEN c_cursor;
    --ͨ��ѭ�������������α�
    DBMS_OUTPUT.PUT_LINE('>>>>>>Update��俪ʼִ����>>>>>>>');
    LOOP
      --v_count := v_count + 1;
      --���α��ȡ��ֵ���趨���ƥ������
      FETCH c_cursor INTO
        SUBJECT_NO
       ,CLIENT_SUB_TYPE
       ,ORIGINAL_MATURITY
       ,BUSINESS_TYPE
       ,ASSET_TYPE
       ,ASSET_SUB_TYPE
       ;
      --���α������ɺ��˳��α�
      EXIT WHEN c_cursor%NOTFOUND;
      v_update_sql:='UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.AssetType='''||ASSET_TYPE||''', T.AssetSubType='''||ASSET_SUB_TYPE||''' WHERE T.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T.AssetType IS NULL AND T.AssetSubType IS NULL ';

      --�ϲ�sql��ƴ��where����
      v_update_sql := v_update_sql
      ||SUBJECT_NO
      ||CLIENT_SUB_TYPE
      ||ORIGINAL_MATURITY
      ||BUSINESS_TYPE
      ;

      --ִ��sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --����ѭ��
    END LOOP;
      --�ر��α�
    CLOSE c_cursor;
  END;
  
  
  
  
  
    --ִ�и��º�ͬ����  �ʲ���С��֮ǰ���������ʲ���С���ÿ�
     UPDATE RWA_DEV.RWA_EI_CONTRACT SET AssetType=NULL,AssetSubType=NULL
     WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
     AND SSYSID NOT IN('XYK','ABS','DZ')
     ;
     COMMIT;


    --ͨ����¶���ʲ���С����º�ͬ���ʲ���С��
    MERGE INTO (SELECT ContractID
    									,ASSETTYPE
    									,ASSETSUBTYPE
    							FROM RWA_DEV.RWA_EI_CONTRACT
    						 WHERE DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
    							 AND SSYSID NOT IN ('XYK','ABS','DZ')) T1
    USING (SELECT CONTRACTID, MIN(ASSETTYPE) AS ASSETTYPE,MIN(ASSETSUBTYPE) AS ASSETSUBTYPE
            FROM RWA_DEV.RWA_EI_EXPOSURE
            WHERE DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
            AND SSYSID NOT IN('XYK','ABS','DZ')
            GROUP BY CONTRACTID) T2
    ON (T1.ContractID=T2.ContractID)
    WHEN MATCHED THEN
    UPDATE SET T1.ASSETTYPE=T2.ASSETTYPE,T1.ASSETSUBTYPE=T2.ASSETSUBTYPE
    ;
    COMMIT;
    UPDATE RWA_EI_CONTRACT SET
    ASSETTYPE='121',
    ASSETSUBTYPE='12101'
    WHERE DATANO=p_data_dt_str AND ASSETSUBTYPE IS NULL;
    
    COMMIT;


    --ͳ��û�и��µ���¶����
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD') AND AssetType IS NULL;
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;


    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�����ʲ�����(PRO_RWA_CD_SUBJECT_ASSET)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_SUBJECT_ASSET;
/

