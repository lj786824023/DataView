CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_MARKETEXPOSURESTD(
                                                   p_data_dt_str    IN    VARCHAR2,        --�������� yyyyMMdd
                                                   p_po_rtncode    OUT    VARCHAR2,        --���ر�� 1 �ɹ�,0 ʧ��
                                                   p_po_rtnmsg     OUT    VARCHAR2         --��������
                )
  /*
    �洢��������:RWA_DEV.PRO_RWA_YSP_MARKETEXPOSURESTD
    ʵ�ֹ���:OPTIC-�г�����-��׼����¶��(������Դ����ֻ�ͷ���ȫ������RWA�г����չ���ӿڱ�����׼����¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0l
    ��д��  :YUSJ
    ��дʱ��:2019-12-25
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_WH_FESPOTPOSITION|����ֻ�ͷ���
    Դ  ��2 :RWA.ORG_INFO|RWA������
    Դ  ��3 :RWA_DEV.RWA_WH_FEFORWARDSSWAP|���Զ�ڵ��ڱ����ڣ�
    Ŀ���  :RWA_DEV.RWA_WH_MARKETEXPOSURESTD|����ϵͳ����׼����¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_YSP_MARKETEXPOSURESTD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_MARKETEXPOSURESTD';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    
    --���ʻ���ҵ��       
     INSERT INTO RWA_DEV.RWA_YSP_MARKETEXPOSURESTD(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,EXPOSUREID                            --���ձ�¶ID
                ,BOOKTYPE                              --�˻����
                ,INSTRUMENTSID                         --���ڹ���ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ORGID                                 --��������ID
                ,ORGNAME                               --������������
                ,ORGTYPE                               --������������
                ,MARKETRISKTYPE                        --�г���������
                ,INTERATERISKTYPE                      --���ʷ�������
                ,EQUITYRISKTYPE                        --��Ʊ��������
                ,EXCHANGERISKTYPE                      --����������
                ,COMMODITYNAME                         --��Ʒ��������
                ,OPTIONRISKTYPE                        --��Ȩ��������
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERRCERATING                       --�����˾���ע����ⲿ����
                ,SMBFLAG                               --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                         --�Ƿ����ծȯ
                ,PAYMENTDATE                           --�ɿ���
                ,SECURITIESTYPE                        --֤ȯ���
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,CLAIMSLEVEL                           --ծȨ����
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,SECURITIESERATING                     --֤ȯ�ⲿ����
                ,STOCKCODE                             --��Ʊ/��ָ����
                ,STOCKMARKET                           --�����г�
                ,EXCHANGEAREA                          --���׵���
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,OPTIONUNDERLYINGFLAG                  --�Ƿ���Ȩ��������
                ,OPTIONUNDERLYINGTYPE                  --��Ȩ������������
                ,OPTIONID                              --��Ȩ����ID
                ,VOLATILITY                            --������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,RATETYPE                              --��������
                ,COUPONRATE                            --Ʊ������
                ,MODIFIEDDURATION                      --��������
                ,POSITIONTYPE                          --ͷ������
                ,POSITION                              --ͷ��
                ,CURRENCY                              --����
                ,OPTIONUNDERLYINGNAME                  --��Ȩ������������
                ,ORGSORTNO                             --���������

    )WITH TEMP11 AS (
    SELECT T1.DEALNO AS DEALNO
    ,T1.SEQ AS SEQ
    ,CASE WHEN T1.PAYRECIND='P' THEN TO_DATE(T1.Matdate,'YYYYMMDD')-TO_DATE(p_data_dt_str, 'YYYYMMDD')
          WHEN T1.PAYRECIND='R' AND T2.PORT='SWZS' THEN 
                        CASE WHEN  SUBSTR(T1.RATECODE,-6,1)='M' THEN 30*SUBSTR(RATECODE,1,LENGTH(RATECODE)-6)
                             WHEN  SUBSTR(T1.RATECODE,-6,1)='Y' THEN 365*SUBSTR(RATECODE,1,LENGTH(RATECODE)-6)
                             WHEN  SUBSTR(T1.RATECODE,-6,1)='W' THEN 7*SUBSTR(RATECODE,1,LENGTH(RATECODE)-6)
                             WHEN  SUBSTR(T1.RATECODE,-6,1)='D' THEN 1*SUBSTR(RATECODE,1,LENGTH(RATECODE)-6)
                        END
          WHEN T1.PAYRECIND='R' AND T5.GCRQ IS NULL THEN 
                                              CASE WHEN TO_DATE(T5.GCRZ,'YYYY-MM-DD')> TO_DATE(p_data_dt_str, 'YYYYMMDD')
                                                   THEN TO_DATE(T5.GCRZ,'YYYY-MM-DD')-TO_DATE(p_data_dt_str, 'YYYYMMDD')
                                                   ELSE TO_DATE(T1.Matdate,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD')
                                              END
         WHEN T1.PAYRECIND='R' AND T5.GCRQ IS NOT NULL  
                               AND TO_DATE(T5.GCRZ,'YYYY-MM-DD')>TO_DATE(p_data_dt_str, 'YYYYMMDD')
                               THEN 1       --�������Ĭ��Ϊ1��            
         ELSE TO_DATE(T1.Matdate,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD')
      END   AS RESIDUALM                                 --ʣ�����ޣ�ת��Ϊ��ģ�������������Ҫ
    FROM RWA_DEV.OPI_SWDT T1 --��������
    inner JOIN RWA_DEV.OPI_SWDH T2 --������ͷ 
    ON T1.DEALNO = T2.DEALNO
    AND T2.DATANO = p_data_dt_str
    AND T2.PORT<>'SWDK'        --�ų��ṹ�Դ��ҵ��
    LEFT JOIN RWA.RWA_WS_GCR_BL T5
    ON T1.DEALNO=T5.DEALNO
    AND T5.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    WHERE T1.DATANO = p_data_dt_str
    AND SUBSTR(T2.COST, 1, 1) = '3' --��һλ=3  --����Ϊ����/���ҵ���ҵ��
    --AND SUBSTR(T2.COST, 4, 1) <> '3' --ȡ�����˻��£�̨���ǰ�����������ҵ��ģ������������ȥ����
    AND SUBSTR(T2.COST, 6, 1) IN ('1','2','3') --����λ=1  --���ʵ���
    AND T2.VERIND = 1 
    AND TRIM(T2.REVDATE) IS NULL
    AND TO_DATE(T1.MATDATE,'YYYYMMDD')>=TO_DATE(p_data_dt_str,'YYYYMMDD')  
    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str                                AS DATANO                   --������ˮ��
                ,'YSP'||T1.DEALNO||T1.SEQ||T1.PAYRECIND       AS EXPOSUREID               --���ձ�¶ID
                ,'02'                                         AS BOOKTYPE                 --�˻����
                ,T1.DEALNO                                    AS INSTRUMENTSID            --���ڹ���ID
                ,'0201'                                       AS INSTRUMENTSTYPE          --���ڹ�������
                ,'6001'                                       AS ORGID                    --��������ID
                ,'�������йɷ����޹�˾����ҵ��'             AS ORGNAME                  --������������
                ,'01'                                         AS ORGTYPE                  --������������                                 Ĭ�ϣ����ڻ���(01)
                ,'01'                                         AS MARKETRISKTYPE           --�г���������                                 Ĭ�ϣ�������(03)
                ,'02'                                         AS INTERATERISKTYPE         --���ʷ�������                                 Ĭ�ϣ���
                ,''                                           AS EQUITYRISKTYPE           --��Ʊ��������                                 Ĭ�ϣ���
                ,''                                           AS EXCHANGERISKTYPE         --����������                                 �������<>����ң�������ݱ���ӳ�䣻����Ҳ���ӳ��
                ,''                                           AS COMMODITYNAME            --��Ʒ��������                                 Ĭ�ϣ���
                ,''                                           AS OPTIONRISKTYPE           --��Ȩ��������                                 Ĭ�ϣ���
                ,'OPI'||TRIM(T3.Cno)                                       AS ISSUERID          --������ID                                     Ĭ�ϣ���
                ,T3.Cfn1                                        AS ISSUERNAME               --����������                                   Ĭ�ϣ���
                ,''                                           AS ISSUERTYPE               --�����˴���                                   Ĭ�ϣ���
                ,''                                           AS ISSUERSUBTYPE            --������С��                                   Ĭ�ϣ���
                ,CASE WHEN T3.CCODE='CN' THEN '01' 
                ELSE '02' END                                 AS ISSUERREGISTSTATE        --������ע�����                               Ĭ�ϣ���
                ,''                                           AS ISSUERRCERATING          --�����˾���ע����ⲿ����                     Ĭ�ϣ���
                ,'0'                                          AS SMBFLAG                  --С΢��ҵ��ʶ                                 Ĭ�ϣ���
                ,''                                           AS UNDERBONDFLAG            --�Ƿ����ծȯ                                 Ĭ�ϣ���
                ,''                                           AS PAYMENTDATE              --�ɿ���                                       Ĭ�ϣ���
                ,''                                           AS SECURITIESTYPE           --֤ȯ���                                     Ĭ�ϣ���
                ,''                                           AS BONDISSUEINTENT          --ծȯ����Ŀ��                                 Ĭ�ϣ���
                ,''                                           AS CLAIMSLEVEL              --ծȨ����                                     Ĭ�ϣ���
                ,''                                           AS REABSFLAG                --���ʲ�֤ȯ����ʶ                             Ĭ�ϣ���
                ,''                                           AS ORIGINATORFLAG           --�Ƿ������                                 Ĭ�ϣ���
                ,''                                           AS SECURITIESERATING        --֤ȯ�ⲿ����                                 Ĭ�ϣ���
                ,''                                           AS STOCKCODE                --��Ʊ/��ָ����                                Ĭ�ϣ���
                ,''                                           AS STOCKMARKET              --�����г�                                     Ĭ�ϣ���
                ,''                                           AS EXCHANGEAREA             --���׵���                                     Ĭ�ϣ���
                ,'0'                                          AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
                ,'0'                                          AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������                             Ĭ�ϣ���(0)
                ,''                                           AS OPTIONUNDERLYINGTYPE     --��Ȩ������������                             Ĭ�ϣ���
                ,''                                           AS OPTIONID                 --��Ȩ����ID                                   Ĭ�ϣ���
                ,NULL                                         AS VOLATILITY               --������                                       Ĭ�ϣ���
                ,T1.Startdate                                 AS STARTDATE                --��ʼ����                                     Ĭ�ϣ���
                ,T1.MATDATE                                   AS DUEDATE                  --��������                                     Ĭ�ϣ���
                ,(TO_DATE(T1.Matdate , 'YYYYMMDD')-TO_DATE(T1.Startdate, 'YYYYMMDD')) / 365 AS ORIGINALMATURITY         --ԭʼ����                                     Ĭ�ϣ���
                ,T5.RESIDUALM/365                             AS RESIDUALM                --ʣ������                                     Ĭ�ϣ���
                ,CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                      ELSE NULL                      --��������Ϊ�������ʲ���Ҫ�ض�����
                 END                                AS NEXTREPRICEDATE          --�´��ض�����                                 Ĭ�ϣ���
                ,CASE WHEN (TO_DATE(CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                                         ELSE NULL                  
                                     END, 'YYYY-MM-DD') -TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN  0
                      ELSE (TO_DATE(CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                                         ELSE NULL                  
                                    END, 'YYYY-MM-DD')- TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
                  END                                       AS NEXTREPRICEM             --�´��ض�������                               Ĭ�ϣ���
                ,CASE WHEN T1.FIXFLOATIND='L' THEN '02'
                      ELSE '01'
                 END                                          AS RATETYPE                 --��������                                     Ĭ�ϣ���
                ,T1.INTRATE_8                                 AS COUPONRATE               --Ʊ������                                     Ĭ�ϣ���
                ,''                                           AS MODIFIEDDURATION         --��������                                     Ĭ�ϣ���
                ,CASE WHEN T1.PAYRECIND = 'R' THEN '01' --��ͷ  
                      WHEN T1.PAYRECIND = 'P' THEN '02' --��ͷ
                 ELSE '01' --��ͷ
                 END                                          AS POSITIONTYPE             --ͷ������
                ,ABS(T1.NOTCCYAMT)*NVL(T6.JZRAT/100,1)*CALCULATE(T5.RESIDUALM,T1.NOTCCY,p_data_dt_str)+NVL(ABS(T4.PVAMT),0)        AS POSITION                 --ͷ��
                ,'CNY'                                       AS CURRENCY                 --����,ͷ�����Ѿ����Ի��ʣ�����Ĭ�������
                ,''                                           AS OPTIONUNDERLYINGNAME    --��Ȩ������������
                ,'1290'                                       AS ORGSORTNO                 --���������
       FROM RWA_DEV.OPI_SWDT T1 --��������
       inner JOIN RWA_DEV.OPI_SWDH T2 --������ͷ 
        ON T1.DEALNO = T2.DEALNO
       AND T2.DATANO = p_data_dt_str
       AND T2.PORT<>'SWDK'        --�ų��ṹ�Դ��ҵ��
       INNER JOIN TEMP11  T5  --������ʱ��
       ON T1.DEALNO=T5.DEALNO
       AND T1.SEQ=T5.SEQ
       LEFT JOIN RWA_DEV.OPI_CUST T3 --�ͻ���Ϣ
       ON trim(T2.CNO) = trim(T3.CNO)
       AND T3.DATANO = p_data_dt_str
       LEFT JOIN (
            --��ȡ�´��ض�������
            SELECT DEALNO,SEQ,PAYRECIND, TO_DATE(MAX(RATEREVDTE)) RATEREVDTE,SUM(PVAMT) PVAMT  --���ʻ�����ͷ��֧ȡ��Ϣ����ֵ
            FROM OPI_SWDS
            WHERE DATANO = p_data_dt_str
            --AND RATEREVDTE IS NOT NULL 
            GROUP BY DEALNO,SEQ,PAYRECIND
            ) T4
       ON T1.DEALNO = T4.DEALNO 
       AND T1.SEQ = T4.SEQ
       AND T1.PAYRECIND=T4.PAYRECIND
       LEFT JOIN RWA_DEV.NNS_JT_EXRATE T6
       ON T1.NOTCCY=T6.CCY
       AND T6.DATANO=p_data_dt_str
       WHERE T1.DATANO = p_data_dt_str
       AND SUBSTR(T2.COST, 1, 1) = '3' --��һλ=3  --����Ϊ����/���ҵ���ҵ��
       --AND SUBSTR(T2.COST, 4, 1) <> '3' --ȡ�����˻��£�̨���ǰ�����������ҵ��ģ������������ȥ����
       AND SUBSTR(T2.COST, 6, 1) IN ('1','2','3') --����λ=1  --���ʵ���,2,3Ϊ����Զ����ҵ��
       AND T2.VERIND = 1 
       AND TRIM(T2.REVDATE) IS NULL
       AND TO_DATE(T1.MATDATE,'YYYYMMDD')>=TO_DATE(p_data_dt_str,'YYYYMMDD')
       ;
    COMMIT;
    
    /*--���һ���ҵ��       
     INSERT INTO RWA_DEV.RWA_YSP_MARKETEXPOSURESTD(
                DATADATE                               --��������
                ,DATANO                                --������ˮ��
                ,EXPOSUREID                            --���ձ�¶ID
                ,BOOKTYPE                              --�˻����
                ,INSTRUMENTSID                         --���ڹ���ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ORGID                                 --��������ID
                ,ORGNAME                               --������������
                ,ORGTYPE                               --������������
                ,MARKETRISKTYPE                        --�г���������
                ,INTERATERISKTYPE                      --���ʷ�������
                ,EQUITYRISKTYPE                        --��Ʊ��������
                ,EXCHANGERISKTYPE                      --����������
                ,COMMODITYNAME                         --��Ʒ��������
                ,OPTIONRISKTYPE                        --��Ȩ��������
                ,ISSUERID                              --������ID
                ,ISSUERNAME                            --����������
                ,ISSUERTYPE                            --�����˴���
                ,ISSUERSUBTYPE                         --������С��
                ,ISSUERREGISTSTATE                     --������ע�����
                ,ISSUERRCERATING                       --�����˾���ע����ⲿ����
                ,SMBFLAG                               --С΢��ҵ��ʶ
                ,UNDERBONDFLAG                         --�Ƿ����ծȯ
                ,PAYMENTDATE                           --�ɿ���
                ,SECURITIESTYPE                        --֤ȯ���
                ,BONDISSUEINTENT                       --ծȯ����Ŀ��
                ,CLAIMSLEVEL                           --ծȨ����
                ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
                ,ORIGINATORFLAG                        --�Ƿ������
                ,SECURITIESERATING                     --֤ȯ�ⲿ����
                ,STOCKCODE                             --��Ʊ/��ָ����
                ,STOCKMARKET                           --�����г�
                ,EXCHANGEAREA                          --���׵���
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,OPTIONUNDERLYINGFLAG                  --�Ƿ���Ȩ��������
                ,OPTIONUNDERLYINGTYPE                  --��Ȩ������������
                ,OPTIONID                              --��Ȩ����ID
                ,VOLATILITY                            --������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,NEXTREPRICEDATE                       --�´��ض�����
                ,NEXTREPRICEM                          --�´��ض�������
                ,RATETYPE                              --��������
                ,COUPONRATE                            --Ʊ������
                ,MODIFIEDDURATION                      --��������
                ,POSITIONTYPE                          --ͷ������
                ,POSITION                              --ͷ��
                ,CURRENCY                              --����
                ,OPTIONUNDERLYINGNAME                  --��Ȩ������������
                ,ORGSORTNO                             --���������

    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str                                AS DATANO                   --������ˮ��
                ,'YSP'||T1.DEALNO||T1.SEQ||T1.PAYRECIND              AS EXPOSUREID               --���ձ�¶ID
                ,'02'                                         AS BOOKTYPE                 --�˻����
                ,T1.DEALNO                                   AS INSTRUMENTSID            --���ڹ���ID
                ,'0201'                                       AS INSTRUMENTSTYPE          --���ڹ�������
                ,'6001'                                       AS ORGID                    --��������ID
                ,'�������йɷ����޹�˾����ҵ��'             AS ORGNAME                  --������������
                ,'01'                                         AS ORGTYPE                  --������������                                 Ĭ�ϣ����ڻ���(01)
                ,'01'                                         AS MARKETRISKTYPE           --�г���������                                 Ĭ�ϣ�������(03)
                ,'02'                                         AS INTERATERISKTYPE         --���ʷ�������                                 Ĭ�ϣ���
                ,''                                           AS EQUITYRISKTYPE           --��Ʊ��������                                 Ĭ�ϣ���
                ,''                                           AS EXCHANGERISKTYPE         --����������                                 �������<>����ң�������ݱ���ӳ�䣻����Ҳ���ӳ��
                ,''                                           AS COMMODITYNAME            --��Ʒ��������                                 Ĭ�ϣ���
                ,''                                           AS OPTIONRISKTYPE           --��Ȩ��������                                 Ĭ�ϣ���
                ,'OPI'||TRIM(T3.Cno)                                       AS ISSUERID          --������ID                                     Ĭ�ϣ���
                ,T3.Cfn1                                        AS ISSUERNAME               --����������                                   Ĭ�ϣ���
                ,''                                           AS ISSUERTYPE               --�����˴���                                   Ĭ�ϣ���
                ,''                                           AS ISSUERSUBTYPE            --������С��                                   Ĭ�ϣ���
                ,CASE WHEN T3.CCODE='CN' THEN '01' 
                ELSE '02' END                                 AS ISSUERREGISTSTATE        --������ע�����                               Ĭ�ϣ���
                ,''                                           AS ISSUERRCERATING          --�����˾���ע����ⲿ����                     Ĭ�ϣ���
                ,'0'                                          AS SMBFLAG                  --С΢��ҵ��ʶ                                 Ĭ�ϣ���
                ,''                                           AS UNDERBONDFLAG            --�Ƿ����ծȯ                                 Ĭ�ϣ���
                ,''                                           AS PAYMENTDATE              --�ɿ���                                       Ĭ�ϣ���
                ,''                                           AS SECURITIESTYPE           --֤ȯ���                                     Ĭ�ϣ���
                ,''                                           AS BONDISSUEINTENT          --ծȯ����Ŀ��                                 Ĭ�ϣ���
                ,''                                           AS CLAIMSLEVEL              --ծȨ����                                     Ĭ�ϣ���
                ,''                                           AS REABSFLAG                --���ʲ�֤ȯ����ʶ                             Ĭ�ϣ���
                ,''                                           AS ORIGINATORFLAG           --�Ƿ������                                 Ĭ�ϣ���
                ,''                                           AS SECURITIESERATING        --֤ȯ�ⲿ����                                 Ĭ�ϣ���
                ,''                                           AS STOCKCODE                --��Ʊ/��ָ����                                Ĭ�ϣ���
                ,''                                           AS STOCKMARKET              --�����г�                                     Ĭ�ϣ���
                ,''                                           AS EXCHANGEAREA             --���׵���                                     Ĭ�ϣ���
                ,'0'                                          AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
                ,'0'                                          AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������                             Ĭ�ϣ���(0)
                ,''                                           AS OPTIONUNDERLYINGTYPE     --��Ȩ������������                             Ĭ�ϣ���
                ,''                                           AS OPTIONID                 --��Ȩ����ID                                   Ĭ�ϣ���
                ,NULL                                         AS VOLATILITY               --������                                       Ĭ�ϣ���
                ,T1.Startdate                                 AS STARTDATE                --��ʼ����                                     Ĭ�ϣ���
                ,CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') 
                      ELSE T1.Matdate 
                 END                                          AS DUEDATE                  --��������                                     Ĭ�ϣ���
                ,CASE WHEN (TO_DATE(CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') ELSE T1.Matdate END, 'YYYY-MM-DD') -
                  TO_DATE(T1.Startdate, 'YYYY-MM-DD')) / 365 < 0 THEN 0
                ELSE (TO_DATE(CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') ELSE T1.Matdate END, 'YYYY-MM-DD')
                -TO_DATE(T1.Startdate, 'YYYY-MM-DD')) / 365
                END                                           AS ORIGINALMATURITY         --ԭʼ����                                     Ĭ�ϣ���
                ,CASE WHEN (TO_DATE(CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') ELSE T1.Matdate END, 'YYYY-MM-DD') -
                  TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN  0
                ELSE (TO_DATE(CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') 
                  ELSE T1.Matdate END, 'YYYY-MM-DD') - TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
                END                                           AS RESIDUALM                --ʣ������                                     Ĭ�ϣ���
                ,CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                      ELSE NULL                      --��������Ϊ�������ʲ���Ҫ�ض�����
                 END                                AS NEXTREPRICEDATE          --�´��ض�����                                 Ĭ�ϣ���
                ,CASE WHEN (TO_DATE(CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                                         ELSE NULL                  
                                         END, 'YYYY-MM-DD') -
                  TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN  0
                ELSE (TO_DATE(CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                                         ELSE NULL                  
                                         END, 'YYYY-MM-DD') -
                TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
                END                                       AS NEXTREPRICEM             --�´��ض�������                               Ĭ�ϣ���
                ,CASE WHEN T1.FIXFLOATIND='L' THEN '02'
                      ELSE '01'
                 END                                          AS RATETYPE                 --��������                                     Ĭ�ϣ���
                ,T1.INTRATE_8                                 AS COUPONRATE               --Ʊ������                                     Ĭ�ϣ���
                ,''                                           AS MODIFIEDDURATION         --��������                                     Ĭ�ϣ���
                ,CASE WHEN T1.PAYRECIND = 'R' THEN '01' --��ͷ  
                      WHEN T1.PAYRECIND = 'P' THEN '02' --��ͷ
                 ELSE '01' --��ͷ
                 END                                          AS POSITIONTYPE             --ͷ������
                ,NVL(T4.PVAMT,0)                              AS POSITION                 --ͷ��
                ,T1.INTCCY                                    AS CURRENCY                 --����
                ,''                                           AS OPTIONUNDERLYINGNAME    --��Ȩ������������
                ,'1290'                                       AS ORGSORTNO                 --���������
       FROM RWA_DEV.OPI_SWDT T1 --��������
       inner JOIN RWA_DEV.OPI_SWDH T2 --������ͷ 
        ON T1.DEALNO = T2.DEALNO
       AND T2.DATANO = p_data_dt_str
       AND T2.PORT<>'SWDK'        --�ų��ṹ�Դ��ҵ��
       LEFT JOIN RWA_DEV.OPI_CUST T3 --�ͻ���Ϣ
        ON trim(T2.CNO) = trim(T3.CNO)
       AND T3.DATANO = p_data_dt_str
       LEFT JOIN (
            --��ȡ�´��ض�������
            SELECT DEALNO, SEQ,PAYRECIND, TO_DATE(MAX(RATEREVDTE)) RATEREVDTE,SUM(PVAMT) PVAMT   --��Ϣ��ֵ
            FROM OPI_SWDS
            WHERE RATEREVDTE IS NOT NULL
            AND  DATANO = p_data_dt_str
            GROUP BY DEALNO,SEQ
            ) T4
       ON T1.DEALNO = T4.DEALNO 
       AND T1.SEQ = T4.SEQ
       AND T1.PAYRECIND=T4.PAYRECIND
      WHERE T1.DATANO = p_data_dt_str
        AND SUBSTR(T2.COST, 1, 1) = '3' --��һλ=3  --����Ϊ����/���ҵ���ҵ��
        --AND SUBSTR(T2.COST, 4, 1) <> '3' ---ȡ�����˻��£�̨���ǰ�����������ҵ��ģ������������ȥ����
        AND SUBSTR(T2.COST, 6, 1) IN( '2','3') --����λ=2  --���ҵ��ڣ�Զ��ҵ��
        AND T2.VERIND = 1   --���ߺ�ſ�
        AND TRIM(T2.REVDATE) IS NULL  --���ߺ�ſ� 
           ;

    COMMIT;*/
           
   --���  Զ��ҵ�� 
   INSERT INTO RWA_DEV.RWA_YSP_MARKETEXPOSURESTD(
              DATADATE                               --��������
              ,DATANO                                --������ˮ��
              ,EXPOSUREID                            --���ձ�¶ID
              ,BOOKTYPE                              --�˻����
              ,INSTRUMENTSID                         --���ڹ���ID
              ,INSTRUMENTSTYPE                       --���ڹ�������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,ORGTYPE                               --������������
              ,MARKETRISKTYPE                        --�г���������
              ,INTERATERISKTYPE                      --���ʷ�������
              ,EQUITYRISKTYPE                        --��Ʊ��������
              ,EXCHANGERISKTYPE                      --����������
              ,COMMODITYNAME                         --��Ʒ��������
              ,OPTIONRISKTYPE                        --��Ȩ��������
              ,ISSUERID                              --������ID
              ,ISSUERNAME                            --����������
              ,ISSUERTYPE                            --�����˴���
              ,ISSUERSUBTYPE                         --������С��
              ,ISSUERREGISTSTATE                     --������ע�����
              ,ISSUERRCERATING                       --�����˾���ע����ⲿ����
              ,SMBFLAG                               --С΢��ҵ��ʶ
              ,UNDERBONDFLAG                         --�Ƿ����ծȯ
              ,PAYMENTDATE                           --�ɿ���
              ,SECURITIESTYPE                        --֤ȯ���
              ,BONDISSUEINTENT                       --ծȯ����Ŀ��
              ,CLAIMSLEVEL                           --ծȨ����
              ,REABSFLAG                             --���ʲ�֤ȯ����ʶ
              ,ORIGINATORFLAG                        --�Ƿ������
              ,SECURITIESERATING                     --֤ȯ�ⲿ����
              ,STOCKCODE                             --��Ʊ/��ָ����
              ,STOCKMARKET                           --�����г�
              ,EXCHANGEAREA                          --���׵���
              ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
              ,OPTIONUNDERLYINGFLAG                  --�Ƿ���Ȩ��������
              ,OPTIONUNDERLYINGTYPE                  --��Ȩ������������
              ,OPTIONID                              --��Ȩ����ID
              ,VOLATILITY                            --������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,NEXTREPRICEDATE                       --�´��ض�����
              ,NEXTREPRICEM                          --�´��ض�������
              ,RATETYPE                              --��������
              ,COUPONRATE                            --Ʊ������
              ,MODIFIEDDURATION                      --��������
              ,POSITIONTYPE                          --ͷ������
              ,POSITION                              --ͷ��
              ,CURRENCY                              --����
              ,OPTIONUNDERLYINGNAME                  --��Ȩ������������
              ,ORGSORTNO                             --���������

  )
  WITH OPI_FXDH_TEMP AS(
    
         SELECT         
            W.DEALNO || 'P' AS DEALNO, --��ˮ��       
            W.COST AS COST, --�ɱ�����        
            W.PORT AS PORT, --��Ʒ        
            W.CUST AS CUST, --�ͻ�        
            'P' AS PS, --����        
            W.DEALDATE AS DEALDATE, --��������        
            W.VDATE AS VDATE, --���׵�������       
            W.SWAPDEAL AS SWAPDEAL, --���ڡ��ǵ��ڱ�ʶ        
            W.TENOR AS TENOR, --����Զ�ڱ�ʶ                
            W.CCY AS CCY, --�������       
            ABS(W.CCYAMT) AS AMT, --������ֽ��        
            W.CCYRATE_8 AS RATE, --�������              
            W.CCYNPVAMT + W.CTRNPVAMT AS CCYNPVAMT, --���м�ֵ
            W.CCYNPVAMT AS AMT1,      --������ֵ               
            CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END AS BOOKTYPE --�˻�����
            --NULL BUYZERORATE,                           --���������Ϣ����
            --NVL(NPVR.CCYDISCNPVFACTOR_10, 1) BUYDISCOUNTRATE,    --������������
            --NULL SELLZERORATE,                          --����������Ϣ����
            --NULL SELLDISCOUNTRATE                       --������������
       FROM RWA_DEV.OPI_FXDH W --�����Ϣ      
      WHERE SUBSTR(W.COST, 1, 1) = '2' --���ҵ��   
        AND SUBSTR(W.COST, 4, 1) <> '3' --����λ<>3 --ȡ�����˻���    
        AND SUBSTR(W.COST, 6, 1) = '2' --Զ��
        --AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') ֻ���ڼ���������ʱ  �����ǽ��ۻ�ҵ�� ������Ҷ���һ���Ҷ������        
        AND W.VDATE >= p_data_dt_str  --δ��������        
        AND W.DATANO = p_data_dt_str 
        AND W.VERIND = '1'   --���ߺ�ſ�������
        AND TRIM(W.REVDATE) IS NULL  --���ߺ�ſ�������
          
        UNION ALL
          
         SELECT         
            W.DEALNO || 'S' AS DEALNO, --��ˮ��       
            W.COST AS COST, --�ɱ�����        
            W.PORT AS  PORT,  --��Ʒ        
            W.CUST AS CUST, --�ͻ�        
            'S' AS PS, --����        
            W.DEALDATE AS DEALDATE, --��������        
            W.VDATE AS VDATE, --���׵�������       
            W.SWAPDEAL AS SWAPDEAL, --���ڡ��ǵ��ڱ�ʶ        
            W.TENOR AS TENOR, --����Զ�ڱ�ʶ                    
            W.CTRCCY AS CCY, --��������        
            ABS(W.CTRAMT) AS AMT, --�������       
            W.CTRBRATE_8 AS RATE, --����             
            W.CCYNPVAMT+W.CTRNPVAMT AS CCYNPVAMT, --���м�ֵ
            W.CTRNPVAMT  AS AMT1,     --������ֵ        
            CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END AS  BOOKTYPE --�˻�����
            --NULL BUYZERORATE,                           --���������Ϣ����
            --NULL BUYDISCOUNTRATE,    --������������
            --NULL SELLZERORATE,                          --����������Ϣ����
            --NVL(NPVR.CTRDISCNPVFACTOR_10, 1) SELLDISCOUNTRATE                       --������������
       FROM RWA_DEV.OPI_FXDH W --�����Ϣ       
      WHERE SUBSTR(W.COST, 1, 1) = '2' --���ҵ��   
        AND SUBSTR(W.COST, 4, 1) <> '3' --����λ<>3 --ȡ�����˻���    
        AND SUBSTR(W.COST, 6, 1) = '2' --Զ��
        --AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') ֻ���ڼ���������ʱ  �����ǽ��ۻ�ҵ�� ������Ҷ���һ���Ҷ������        
        AND W.VDATE >= p_data_dt_str  --δ��������        
        AND W.DATANO = p_data_dt_str 
        AND W.VERIND = '1'   --���ߺ�ſ�������
        AND TRIM(W.REVDATE) IS NULL  --���ߺ�ſ�������    
  )
      SELECT
               TO_DATE(p_data_dt_str,'YYYYMMDD')               AS DATADATE                 --��������
              ,p_data_dt_str                                   AS DATANO                   --������ˮ��
              ,'YSP'||T1.DEALNO                             AS EXPOSUREID               --���ձ�¶ID
              ,'02'                                         AS BOOKTYPE                 --�˻����
              ,T1.DEALNO                                    AS INSTRUMENTSID            --���ڹ���ID
              ,'0501'                                       AS INSTRUMENTSTYPE          --���ڹ�������
              ,'6001'                                       AS ORGID                    --��������ID
              ,'�������йɷ����޹�˾����ҵ��'             AS ORGNAME                  --������������
              ,'01'                                         AS ORGTYPE                  --������������                                 Ĭ�ϣ����ڻ���(01)
              ,'01'                                         AS MARKETRISKTYPE           --�г���������                                 Ĭ�ϣ�������(03)
              ,'02'                                         AS INTERATERISKTYPE         --���ʷ�������                                 Ĭ�ϣ���
              ,''                                           AS EQUITYRISKTYPE           --��Ʊ��������                                 Ĭ�ϣ���
              ,''                                           AS EXCHANGERISKTYPE         --����������                                 �������<>����ң�������ݱ���ӳ�䣻����Ҳ���ӳ��
              ,''                                           AS COMMODITYNAME            --��Ʒ��������                                 Ĭ�ϣ���
              ,''                                           AS OPTIONRISKTYPE           --��Ȩ��������                                 Ĭ�ϣ���
              ,'OPI'||TRIM(T2.CNO)                                AS ISSUERID                 --������ID                                     Ĭ�ϣ���
              ,T2.CFN1                                        AS ISSUERNAME               --����������                                   Ĭ�ϣ���
              ,''                                           AS ISSUERTYPE               --�����˴���                                   Ĭ�ϣ���
              ,''                                           AS ISSUERSUBTYPE            --������С��                                   Ĭ�ϣ���
              ,CASE WHEN T2.CCODE='CN' THEN '01' 
              ELSE '02' END                                 AS ISSUERREGISTSTATE        --������ע�����                               Ĭ�ϣ���
              ,''                                           AS ISSUERRCERATING          --�����˾���ע����ⲿ����                     Ĭ�ϣ���
              ,'0'                                           AS SMBFLAG                  --С΢��ҵ��ʶ                                 Ĭ�ϣ���
              ,''                                           AS UNDERBONDFLAG            --�Ƿ����ծȯ                                 Ĭ�ϣ���
              ,''                                           AS PAYMENTDATE              --�ɿ���                                       Ĭ�ϣ���
              ,''                                           AS SECURITIESTYPE           --֤ȯ���                                     Ĭ�ϣ���
              ,''                                           AS BONDISSUEINTENT          --ծȯ����Ŀ��                                 Ĭ�ϣ���
              ,''                                           AS CLAIMSLEVEL              --ծȨ����                                     Ĭ�ϣ���
              ,''                                           AS REABSFLAG                --���ʲ�֤ȯ����ʶ                             Ĭ�ϣ���
              ,''                                           AS ORIGINATORFLAG           --�Ƿ������                                 Ĭ�ϣ���
              ,''                                           AS SECURITIESERATING        --֤ȯ�ⲿ����                                 Ĭ�ϣ���
              ,''                                           AS STOCKCODE                --��Ʊ/��ָ����                                Ĭ�ϣ���
              ,''                                           AS STOCKMARKET              --�����г�                                     Ĭ�ϣ���
              ,''                                           AS EXCHANGEAREA             --���׵���                                     Ĭ�ϣ���
              ,'0'                                          AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
              ,'0'                                          AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������                             Ĭ�ϣ���(0)
              ,''                                           AS OPTIONUNDERLYINGTYPE     --��Ȩ������������                             Ĭ�ϣ���
              ,''                                           AS OPTIONID                 --��Ȩ����ID                                   Ĭ�ϣ���
              ,NULL                                         AS VOLATILITY               --������                                       Ĭ�ϣ���
              ,T1.DEALDATE                                 AS STARTDATE                --��ʼ����                                     Ĭ�ϣ���
              ,T1.VDATE                                   AS DUEDATE                  --��������                                     Ĭ�ϣ���
              ,CASE WHEN (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
                TO_DATE(T1.DEALDATE, 'YYYY-MM-DD')) / 365 < 0 THEN 0
              ELSE (TO_DATE(T1.VDATE, 'YYYY-MM-DD')
              -TO_DATE(T1.DEALDATE, 'YYYY-MM-DD')) / 365
              END                                           AS ORIGINALMATURITY         --ԭʼ����                                     Ĭ�ϣ���
              ,CASE WHEN (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
                TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN  0
              ELSE (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
              END                                           AS RESIDUALM                --ʣ������                                     Ĭ�ϣ���
              ,''                                           AS NEXTREPRICEDATE          --�´��ض�����                                 Ĭ�ϣ���
              ,''                                           AS NEXTREPRICEM             --�´��ض�������                               Ĭ�ϣ���
              ,'01'                                           AS RATETYPE                 --��������                                     Ĭ�ϣ���
              ,T1.RATE                                       AS COUPONRATE               --Ʊ������                                     Ĭ�ϣ���
              ,''                                           AS MODIFIEDDURATION         --��������                                     Ĭ�ϣ���
              ,CASE 
                WHEN T1.PS = 'P' THEN '01' --��ͷ  
                WHEN T1.PS = 'S' THEN '02' --��ͷ
                ELSE '01' --��ͷ
              END                                           AS POSITIONTYPE             --ͷ������
              ,T1.AMT1                                       AS POSITION                 --ͷ��
              ,T1.CCY                                       AS CURRENCY                 --����
              ,''                                           AS OPTIONUNDERLYINGNAME    --��Ȩ������������
              ,'1290'                                       AS ORGSORTNO                 --���������
            FROM OPI_FXDH_TEMP T1 --�����Ϣ  
            LEFT JOIN RWA_DEV.OPI_CUST T2
            ON TRIM(T1.CUST)=TRIM(T2.CNO)  
            AND T2.DATANO=p_data_dt_str
            ;
                        
    COMMIT;
    
    /* */
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_YSP_MARKETEXPOSURESTD',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_YSP_MARKETEXPOSURESTD;
    --Dbms_output.Put_line('RWA_DEV.RWA_WH_MARKETEXPOSURESTD��ǰ����Ĺ���ϵͳ-���(�г�����)-��׼����¶��¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
      p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
        --�����쳣
        EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := '�г����ձ�׼����¶��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_YSP_MARKETEXPOSURESTD;
/

