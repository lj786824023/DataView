CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_OFF_EXPOSURE_TYPE(
                                 p_data_dt_str  IN   VARCHAR2,   --��������
                                 p_po_rtncode    OUT  VARCHAR2,   --���ر��
                                 p_po_rtnmsg    OUT  VARCHAR2     --��������
                                 )
AS
/*
    �洢��������:PRO_RWA_CD_SUBJECT_ASSET
    ʵ�ֹ���:���±�¶���б���ҵ�����ͺͱ���ҵ������ϸ��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-08-26
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :���÷��ձ�¶��
    Դ  ��2  :���÷��պ�ͬ��
    Դ  ��3  :���������
    Ŀ���  :RWA_CD_OFF_EXPOSURE_TYPE
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

  --�ʲ�С�����
  ASSET_SUB_TYPE VARCHAR2(50);
  --ԭʼ����
  ORIGINAL_MATURITY VARCHAR2(100);
  --����ҵ������
  OFFBUSINESSTYPE VARCHAR2(50);
  --����ҵ������ϸ��
  OFFBUSINESSSDVSSTD VARCHAR2(50);

BEGIN

  DECLARE
   --ͬ���α��ȡ��Ҫ���ж�����
  CURSOR c_cursor IS
    SELECT
    CASE WHEN ASSET_SUB_TYPE IS NOT NULL
         THEN ' AND AssetSubType= '''||ASSET_SUB_TYPE||''' '
         ELSE ''
    END  AS ASSET_SUB_TYPE    --�ʲ�С��
    ,CASE WHEN ORIGINAL_MATURITY='01'
          THEN ' AND OriginalMaturity<=1'
          WHEN ORIGINAL_MATURITY='02'
          THEN ' AND OriginalMaturity>1'
          ELSE ''
     END AS ORIGINAL_MATURITY     --ԭʼ����
    ,OFFBUSINESSTYPE   --����ҵ������
    ,OFFBUSINESSSDVSSTD  --����ҵ������ϸ��
    FROM RWA_CD_OFF_EXPOSURE_TYPE
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
        ASSET_SUB_TYPE
       ,ORIGINAL_MATURITY
       ,OFFBUSINESSTYPE
       ,OFFBUSINESSSDVSSTD
       ;
      --���α������ɺ��˳��α�
      EXIT WHEN c_cursor%NOTFOUND;
      v_update_sql:='UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.OFFBUSINESSTYPE='''||OFFBUSINESSTYPE||''', T.OFFBUSINESSSDVSSTD='''||OFFBUSINESSSDVSSTD||''' WHERE T.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T.OFFBUSINESSSDVSSTD IS NULL ';

      --�ϲ�sql��ƴ��where����
      v_update_sql := v_update_sql
      ||ASSET_SUB_TYPE
      ||ORIGINAL_MATURITY
      ;

      --ִ��sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --����ѭ��
    END LOOP;
      --�ر��α�
    CLOSE c_cursor;
  END;
    --ͳ��û�и��µ�����
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD') AND OFFBUSINESSSDVSSTD IS NOT NULL;
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;

    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���±���ҵ�����ͼ�����ҵ������ϸ��(PRO_RWA_CD_OFF_EXPOSURE_TYPE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_OFF_EXPOSURE_TYPE;
/

