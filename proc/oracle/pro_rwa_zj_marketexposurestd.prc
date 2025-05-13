CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZJ_MARKETEXPOSURESTD(
                            p_data_dt_str IN  VARCHAR2,   --�������� yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg   OUT VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZJ_MARKETEXPOSURESTD
    ʵ�ֹ���:�г�����-�ʽ�ϵͳ-��׼�����ձ�¶��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :CHENGANG
    ��дʱ��:2019-04-18
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_ZJ_BONDINFO|ծ����Ϣ��
    Դ  ��2 :RWA_DEV.RWA_ZJ_TRADBONDPOSITION|ծȯͷ����Ϣ��
    Դ  ��3 :RWA.RWA_WP_COUNTRYRATING |����������Ϣ��
    Դ  ��4 :RWA.ORG_INFO|������Ϣ��
    Դ  ��5 :RWA_DEV.BRD_BOND|ծ����
    Դ  ��6 :RWA_DEV.BRD_SECURITY_POSI|ծȯͷ����Ϣ��
     �����¼(�޸���|�޸�ʱ��|�޸�����):
     pxl 2019/09/05  ��׼�����ձ�¶���߼�����
     
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZJ_MARKETEXPOSURESTD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD';

    --2.���������������ݴ�Դ����뵽Ŀ�����
  INSERT INTO RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD
      ( DATADATE,          --��������
        DATANO,          --������ˮ��
        EXPOSUREID,          --���ձ�¶ID
        BOOKTYPE,          --�˻����
        INSTRUMENTSID,          --���ڹ���ID
        INSTRUMENTSTYPE,          --���ڹ�������
        ORGSORTNO,          --�������������
        ORGID,          --��������ID
        ORGNAME,          --������������
        ORGTYPE,          --������������
        MARKETRISKTYPE,          --�г���������
        INTERATERISKTYPE,          --���ʷ�������
        EQUITYRISKTYPE,          --��Ʊ��������
        EXCHANGERISKTYPE,          --����������
        COMMODITYNAME,          --��Ʒ��������
        OPTIONRISKTYPE,          --��Ȩ��������
        ISSUERID,          --������ID
        ISSUERNAME,          --����������
        ISSUERTYPE,          --�����˴���
        ISSUERSUBTYPE,          --������С��
        ISSUERREGISTSTATE,          --������ע�����
        ISSUERRCERATING,          --�����˾���ע����ⲿ����
        SMBFLAG,          --С΢��ҵ��ʶ
        UNDERBONDFLAG,          --�Ƿ����ծȯ
        PAYMENTDATE,          --�ɿ���
        SECURITIESTYPE,          --֤ȯ���
        BONDISSUEINTENT,          --ծȯ����Ŀ��
        CLAIMSLEVEL,          --ծȨ����
        REABSFLAG,          --���ʲ�֤ȯ����ʶ
        ORIGINATORFLAG,          --�Ƿ������
        SECURITIESERATING,          --֤ȯ�ⲿ����
        STOCKCODE,          --��Ʊ/��ָ����
        STOCKMARKET,          --�����г�
        EXCHANGEAREA,          --���׵���
        STRUCTURALEXPOFLAG,          --�Ƿ�ṹ�Գ���
        OPTIONUNDERLYINGFLAG,          --�Ƿ���Ȩ��������
        OPTIONUNDERLYINGNAME,          --��Ȩ������������
        OPTIONID,          --��Ȩ����ID
        VOLATILITY,          --������
        STARTDATE,          --��ʼ����
        DUEDATE,          --��������
        ORIGINALMATURITY,          --ԭʼ����
        RESIDUALM,          --ʣ������
        NEXTREPRICEDATE,          --�´��ض�����
        NEXTREPRICEM,          --�´��ض�������
        RATETYPE,          --��������
        COUPONRATE,          --Ʊ������
        MODIFIEDDURATION,          --��������
        POSITIONTYPE,          --ͷ������
        POSITION,          --ͷ��
        CURRENCY,          --����
        OPTIONUNDERLYINGTYPE          --��Ȩ������������
       )
     SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'),    --��������
            p_data_dt_str,   --������ˮ��
            T1.POSITIONID,    --���ձ�¶ID
            '02',   --�˻����
            T1.BONDID,    --���ڹ���ID
            T1.INSTRUMENTSTYPE,   --���ڹ�������
            T2.SORTNO,    --�������������
            T1.ACCORGID,    --��������ID
            T2.ORGNAME,   --������������
            '01',   --������������
            '01',   --�г���������
            '01',   --���ʷ�������
            '',   --��Ʊ��������
            '',   --����������
            '',   --��Ʒ��������
            '',   --��Ȩ��������
            T3.ISSUERID,    --������ID
            T3.ISSUERNAME,    --����������
            T3.ISSUERTYPE,    --�����˴���
            T3.ISSUERSUBTYPE,   --������С��
            T3.ISSUERREGISTSTATE,   --������ע�����
            T4.RATINGRESULT,    --�����˾���ע����ⲿ����
            T3.ISSUERSMBFLAG,   --С΢��ҵ��ʶ
            '0',    --�Ƿ����ծȯ
            '',   --�ɿ���
            T3.BONDTYPE,    --֤ȯ���
            T3.BONDISSUEINTENT,   --ծȯ����Ŀ��
            CASE
              WHEN T5.BOND_TYPE IN ('OBB',
                                 'XYBS' --���������дμ�ծ����ҵ���дμ�ծ
                                 ) THEN
               '01'
              ELSE
               '02'
            END CLAIMSLEVEL,   --ծȨ����
            T3.REABSFLAG,   --���ʲ�֤ȯ����ʶ
            T3.ORIGINATORFLAG,    --�Ƿ������
            T3.ERATING,   --֤ȯ�ⲿ����
            ''  ,   --��Ʊ/��ָ����
            ''  ,   --�����г�
            ''  ,   --���׵���
            ''  ,   --�Ƿ�ṹ�Գ���
            '0' ,   --�Ƿ���Ȩ��������
            ''  ,   --��Ȩ������������
            ''  ,   --��Ȩ����ID
            ''  ,   --������
            T3.STARTDATE  ,   --��ʼ����
            T3.DUEDATE  ,   --��������
            T3.ORIGINALMATURITY ,   --ԭʼ����
            T3.RESIDUALM  ,   --ʣ������
            T3.NEXTREPRICEDATE  ,   --�´��ض�����
            T3.NEXTREPRICEM ,   --�´��ض�������
            T3.RATETYPE ,   --��������
            T3.EXECUTIONRATE  ,   --Ʊ������
            T3.MODIFIEDDURATION ,   --��������
            '01'  ,   --ͷ������ Ĭ�� 01  ��ͷ
            T1.BOOKBALANCE  ,   --ͷ��
            T1.CURRENCY ,   --����
            ''      --��Ȩ������������
   FROM RWA_ZJ_TRADBONDPOSITION T1
      INNER JOIN RWA_ZJ_BONDINFO T3
         ON T1.BONDID = T3.BONDID
        AND T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
       LEFT JOIN RWA.ORG_INFO T2
         ON T1.ACCORGID = T2.ORGID
        AND T2.STATUS = '1'
       LEFT JOIN RWA.RWA_WP_COUNTRYRATING T4
         ON T3.ISSUERREGISTSTATE = T4.COUNTRYCODE
        AND T4.ISINUSE = '1'
       INNER JOIN BRD_BOND T5
         ON T3.SECURITY_REFERENCE = T5.BOND_ID  --֤ȯΨһ��ʾ
        AND T5.BELONG_GROUP = '4' --�ʽ�ϵͳ
        AND T5.DATANO = p_data_dt_str
   WHERE T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') 
    ;
    
    COMMIT;

 --2.����Ʒ��¶��
 /*
    INSERT INTO RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD
      ( DATADATE,          --��������
        DATANO,          --������ˮ��
        EXPOSUREID,          --���ձ�¶ID
        BOOKTYPE,          --�˻����
        INSTRUMENTSID,          --���ڹ���ID
        INSTRUMENTSTYPE,          --���ڹ�������
        ORGSORTNO,          --�������������
        ORGID,          --��������ID
        ORGNAME,          --������������
        ORGTYPE,          --������������
        MARKETRISKTYPE,          --�г���������
        INTERATERISKTYPE,          --���ʷ�������
        EQUITYRISKTYPE,          --��Ʊ��������
        EXCHANGERISKTYPE,          --����������
        COMMODITYNAME,          --��Ʒ��������
        OPTIONRISKTYPE,          --��Ȩ��������
        ISSUERID,          --������ID
        ISSUERNAME,          --����������
        ISSUERTYPE,          --�����˴���
        ISSUERSUBTYPE,          --������С��
        ISSUERREGISTSTATE,          --������ע�����
        ISSUERRCERATING,          --�����˾���ע����ⲿ����
        SMBFLAG,          --С΢��ҵ��ʶ
        UNDERBONDFLAG,          --�Ƿ����ծȯ
        PAYMENTDATE,          --�ɿ���
        SECURITIESTYPE,          --֤ȯ���
        BONDISSUEINTENT,          --ծȯ����Ŀ��
        CLAIMSLEVEL,          --ծȨ����
        REABSFLAG,          --���ʲ�֤ȯ����ʶ
        ORIGINATORFLAG,          --�Ƿ������
        SECURITIESERATING,          --֤ȯ�ⲿ����
        STOCKCODE,          --��Ʊ/��ָ����
        STOCKMARKET,          --�����г�
        EXCHANGEAREA,          --���׵���
        STRUCTURALEXPOFLAG,          --�Ƿ�ṹ�Գ���
        OPTIONUNDERLYINGFLAG,          --�Ƿ���Ȩ��������
        OPTIONUNDERLYINGNAME,          --��Ȩ������������
        OPTIONID,          --��Ȩ����ID
        VOLATILITY,          --������
        STARTDATE,          --��ʼ����
        DUEDATE,          --��������
        ORIGINALMATURITY,          --ԭʼ����
        RESIDUALM,          --ʣ������
        NEXTREPRICEDATE,          --�´��ض�����
        NEXTREPRICEM,          --�´��ض�������
        RATETYPE,          --��������
        COUPONRATE,          --Ʊ������
        MODIFIEDDURATION,          --��������
        POSITIONTYPE,          --ͷ������
        POSITION,          --ͷ��
        CURRENCY,          --����
        OPTIONUNDERLYINGTYPE          --��Ȩ������������
       )
     SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������
            p_data_dt_str, --������ˮ��
            T1.POSITIONID, --���ձ�¶ID
            CASE
              WHEN SUBSTR(H.COST, 1�� 4) = 'E' THEN
               '01'
              ELSE
               '02'
            END, --�˻����
            T1.BONDID, --���ڹ���ID
            T1.INSTRUMENTSTYPE, --���ڹ�������
            T2.SORTNO, --�������������
            T1.TRANORGID, --��������ID
            T2.ORGNAME, --������������
            CASE
              WHEN C.Ccode = 'CN' THEN
               '01'
              ELSE
               '02'
            END, --������������
            '01', --�г���������
            '02', --���ʷ�������
            '', --��Ʊ��������
            '', --����������
            '', --��Ʒ��������
            '', --��Ȩ��������
            T3.ISSUERID, --������ID
            T3.ISSUERNAME, --����������
            T3.ISSUERTYPE, --�����˴���
            T3.ISSUERSUBTYPE, --������С��
            T3.ISSUERREGISTSTATE, --������ע�����
            '', --�����˾���ע����ⲿ����
            T3.ISSUERSMBFLAG, --С΢��ҵ��ʶ
            '0', --�Ƿ����ծȯ
            '', --�ɿ���
            '', --֤ȯ���
            T3.BONDISSUEINTENT, --ծȯ����Ŀ��
            '01', --ծȨ����
            T3.REABSFLAG, --���ʲ�֤ȯ����ʶ
            T3.ORIGINATORFLAG, --�Ƿ������
            T3.ERATING, --֤ȯ�ⲿ����
            '', --��Ʊ/��ָ����
            '', --�����г�
            '', --���׵���
            '0', --�Ƿ�ṹ�Գ���
            '0', --�Ƿ���Ȩ��������
            '', --��Ȩ������������
            '', --��Ȩ����ID
            '', --������
            T3.STARTDATE, --��ʼ����
            T3.DUEDATE, --��������
            T3.ORIGINALMATURITY, --ԭʼ����
            T3.RESIDUALM, --ʣ������
            T3.NEXTREPRICEDATE, --�´��ض�����
            T3.NEXTREPRICEM, --�´��ض�������
            T3.RATETYPE, --��������
            T3.EXECUTIONRATE, --Ʊ������
            T3.MODIFIEDDURATION, --��������
            CASE
              WHEN T.PAYRECIND = 'P' THEN
               '01' --��ͷ  
              WHEN T.PAYRECIND = 'R' THEN
               '02' --��ͷ
              ELSE
               '01' --��ͷ
            END, --ͷ������
            T1.Bookbalance, --ͷ��
            T1.CURRENCY, --����
            '' --��Ȩ������������
       FROM  RWA_ZJ_TRADBONDPOSITION T1 
       INNER JOIN OPI_SWDT T 
         ON T1.POSITIONID = T.DEALNO || T.SEQ
        AND T1.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
       LEFT JOIN OPI_SWDH H 
         ON T.DEALNO = H.DEALNO
        AND H.DATANO = p_data_dt_str
       LEFT JOIN OPI_CUST C 
         ON H.CNO = C.CNO
        AND C.DATANO = p_data_dt_str
       INNER JOIN OPI_SWDT T 
         ON T1.POSITIONID = T.DEALNO || T.SEQ
        AND T1.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
       LEFT JOIN RWA.ORG_INFO T2
         ON T1.TRANORGID = T2.ORGID
       LEFT JOIN RWA_ZJ_BONDINFO T3
         ON T1.BONDID = T3.BONDID
        AND T1.DATADATE = T3.DATADATE
      WHERE T.DATANO = p_data_dt_str;
            
            commit;
        */    
            
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZJ_MARKETEXPOSURESTD',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZJ_MARKETEXPOSURESTD;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '��׼�����ձ�¶��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZJ_MARKETEXPOSURESTD;
/

