CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WH_FEFORWARDSSWAP(
                                                               p_data_dt_str IN  VARCHAR2,--�������� yyyyMMdd
                                                               p_po_rtncode  OUT VARCHAR2,--���ر�� 1 �ɹ�,0 ʧ��
                                                               p_po_rtnmsg   OUT VARCHAR2 --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_WH_FEFORWARDSSWAP
    ʵ�ֹ���:����ϵͳ-�г�����-���Զ�ڵ���(�����ݲ���ϵͳ��ҵ�������Ϣȫ������RWA�г��������ӿڱ����Զ�ڵ��ڱ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-08-02
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.NSS_FI_FOREXCHANGEINFO|����ϵͳ����������ױ�
    Դ  ��2 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Դ  ��3 :RWA.RWA_WS_FOREIGN_EXCHANGE|���������¼��
    Ŀ���1 :RWA_DEV.RWA_WH_FEFORWARDSSWAP|���Զ�ڵ��ڱ����ڣ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    xlp  20190909  �ṹ�Գ���  
    xlp  20191121  ��������Ʒҵ��  ���Զ�ڡ����� �г����ռ������� 
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WH_FEFORWARDSSWAP';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_WH_FEFORWARDSSWAP';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ��������  ��ȡ�ṹ�Դ��
    INSERT INTO RWA_DEV.RWA_WH_FEFORWARDSSWAP(
                 DATADATE                              --��������
                ,TRANID                                --����ID
                ,TRANORGID                             --���׻���ID
                ,ACCORGID                              --�������ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,BOOKTYPE                              --�˻����
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,BUYCURRENCY                           --�������
                ,BUYAMOUNT                             --������
                ,SELLCURRENCY                          --��������
                ,SELLAMOUNT                            --�������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,BUYZERORATE                           --���������Ϣ����
                ,BUYDISCOUNTRATE                       --������������
                ,SELLZERORATE                          --����������Ϣ����
                ,SELLDISCOUNTRATE                      --������������

    )
    SELECT
                 DATADATE                              --��������
                ,p_data_dt_str||lpad(rownum, 10, '0')
                                            AS TRANID  --����ID
                ,TRANORGID                             --���׻���ID
                ,ACCORGID                              --�������ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,BOOKTYPE                              --�˻����
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,BUYCURRENCY                           --�������
                ,BUYAMOUNT                             --������
                ,SELLCURRENCY                          --��������
                ,SELLAMOUNT                            --�������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                      --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,BUYZERORATE                           --���������Ϣ����
                ,BUYDISCOUNTRATE                       --������������
                ,SELLZERORATE                          --����������Ϣ����
                ,SELLDISCOUNTRATE                      --������������
    FROM (
        SELECT
                    TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                    ,'9998'                                  AS TRANORGID                --���׻���ID       Ĭ��Ϊ��10000000-���С�
                    ,'9998'                                  AS ACCORGID                 --�������ID       Ĭ��Ϊ��10000000-���С�
                    ,'0506'                                      AS INSTRUMENTSTYPE          --���ڹ�������     Ĭ��Ϊ��0506-��㼴�ڡ�
                    ,T1.ACCSUBJECTS                              AS ACCSUBJECTS              --��ƿ�Ŀ
                    ,T1.BOOKTYPE                                 AS BOOKTYPE                 --�˻����
                    ,T1.STRUCTURALEXPOFLAG                       AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
                    ,T1.BUYCURRENCY                              AS BUYCURRENCY              --�������
                    ,T1.BUYAMOUNT                                AS BUYAMOUNT                --������
                    ,T1.SELLCURRENCY                             AS SELLCURRENCY             --��������
                    ,T1.SELLAMOUNT                               AS SELLAMOUNT               --�������
                    ,TO_CHAR(TO_DATE(p_data_dt_str,'yyyyMMdd'),'yyyy-MM-dd')
                                                                 AS STARTDATE                --��ʼ����
                    ,TO_CHAR(TO_DATE(p_data_dt_str,'yyyyMMdd'),'yyyy-MM-dd')
                                                                 AS DUEDATE                  --��������
                    ,0                                           AS ORIGINALMATURITY         --ԭʼ����
                    ,0                                           AS RESIDUALM                --ʣ������
                    ,NULL                                        AS BUYZERORATE              --���������Ϣ����
                    ,NULL                                        AS BUYDISCOUNTRATE          --������������
                    ,NULL                                        AS SELLZERORATE             --����������Ϣ����
                    ,NULL                                        AS SELLDISCOUNTRATE         --������������
        FROM (
              SELECT
                     T.SUBJECT_NO                                AS ACCSUBJECTS              --��ƿ�Ŀ
                    ,CASE WHEN T.SUBJECT_NO LIKE '1101%' THEN '02'
                          ELSE '01' END                          AS BOOKTYPE                 --�˻����         ͨ����ƿ�Ŀӳ�䣬����Ŀ��Ϊ1101-�����Խ����ʲ�Ϊ�����˻�(02)������Ĭ��Ϊ�����˻�(01)
                    ,CASE WHEN T.SUBJECT_NO = '10030102' OR T.SUBJECT_NO LIKE '4001%' THEN '1'
                     ELSE '0' END                                AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���   ͨ����ƿ�Ŀӳ�䣬����Ŀ��Ϊ4001-�ɱ�����Ŀ10030102-����������п���-����������з���׼����Ϊ�ṹ�Գ���(1)������Ĭ��Ϊ��(0)
                    ,T.CURRENCY_CODE                             AS BUYCURRENCY              --�������
                    ,CASE WHEN CL.ATTRIBUTE8='D-C' AND T.BALANCE_D - T.BALANCE_C>0 THEN T.BALANCE_D - T.BALANCE_C
                          WHEN CL.ATTRIBUTE8='C-D' AND T.BALANCE_C - T.BALANCE_D>0 THEN T.BALANCE_C - T.BALANCE_D
                          ELSE 0 END                             AS BUYAMOUNT                --������         >0
                    ,T.CURRENCY_CODE                             AS SELLCURRENCY             --��������          <0
                    ,CASE WHEN CL.ATTRIBUTE8='D-C' AND T.BALANCE_D - T.BALANCE_C<0 THEN T.BALANCE_D - T.BALANCE_C
                          WHEN CL.ATTRIBUTE8='C-D' AND T.BALANCE_C - T.BALANCE_D<0 THEN T.BALANCE_C - T.BALANCE_D
                          ELSE 0 END                             AS SELLAMOUNT               --�������
              FROM  RWA_DEV.FNS_GL_BALANCE T
              LEFT JOIN RWA.CODE_LIBRARY CL
              ON    CL.CODENO='NewSubject'
              AND   T.SUBJECT_NO=CL.ITEMNO
              AND   CL.ISINUSE='1'
              WHERE T.CURRENCY_CODE IS NOT NULL
              AND   T.CURRENCY_CODE <> 'RMB'
              AND   T.CURRENCY_CODE <> 'CNY'
              AND   T.DATANO = p_data_dt_str
              AND   (T.SUBJECT_NO = '10030102' OR T.SUBJECT_NO LIKE '4001%')
             ) T1
             WHERE T1.BUYAMOUNT <> 0 OR T1.SELLAMOUNT <> 0
   )
  ;

    COMMIT;    
    
    /*  ���� G4C-1(e)���г����ձ�׼���ʱ�Ҫ������������գ����˵�� �й涨 
    ��ͷ����ÿһ���ֵĶ�ͷͷ��Ϳ�ͷͷ�����������������ھ�ͷ�硢Զ�ھ�ͷ�����Ȩ��Լ�ö�����Delta������޷������ı�֤��
    ����Ҽ�ֵ������֮�͡�
    
    ֻ��Ҫ�������ھ�ͷ�硢Զ�ھ�ͷ�����Ȩ��Լ�ö�����Delta������
    
    --2.1 OPICSϵͳ  ��ȡ������ 
    INSERT INTO RWA_DEV.RWA_WH_FEFORWARDSSWAP(
                 DATADATE                              --��������
                ,TRANID                                --����ID
                ,TRANORGID                             --���׻���ID
                ,ACCORGID                              --�������ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,BOOKTYPE                              --�˻����
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,BUYCURRENCY                           --�������
                ,BUYAMOUNT                             --������
                ,SELLCURRENCY                          --��������
                ,SELLAMOUNT                            --�������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                               --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,BUYZERORATE                           --���������Ϣ����
                ,BUYDISCOUNTRATE                       --������������
                ,SELLZERORATE                          --����������Ϣ����
                ,SELLDISCOUNTRATE                      --������������

    )
    WITH YSP_WHDQ AS (                
             SELECT         
                  PS || DEALNO AS DEALNO, --��ˮ��       
                  COST AS COST, --�ɱ�����        
                  PORT AS PORT, --��Ʒ        
                  CUST AS CUST, --�ͻ�        
                  PS AS PS, --����        
                  DEALDATE AS DEALDATE, --��������        
                  VDATE AS VDATE, --���׵�������       
                  SWAPDEAL AS SWAPDEAL, --���ڡ��ǵ��ڱ�ʶ        
                  TENOR AS TENOR, --����Զ�ڱ�ʶ                
                  CCY AS CCY, --����/��������       
                  ABS(CCYAMT) AS AMT, --����/�������ֽ��        
                  CCYRATE_8 AS RATE, --����/��������                        
                  CTRCCY AS CTRCCY, --��������        
                  ABS(CTRAMT) AS CTRAMT, --�������       
                  CTRBRATE_8 AS CTRBRATE_8, --����
                  ABS(CCYNPVAMT + CTRNPVAMT) AS CCYNPVAMT, --���м�ֵ   
                  CASE WHEN SUBSTR(COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --�˻�����
             FROM RWA_DEV.OPI_FXDH W --�����Ϣ
            WHERE SUBSTR(W.COST, 1, 1) = '2' --���ҵ��
              AND SUBSTR(W.COST, 6, 1) = '3' --����
              AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --�����ǽ��ۻ�ҵ�� ������Ҷ���һ���Ҷ������        
              AND W.VDATE >= p_data_dt_str  --δ��������        
              AND W.DATANO = p_data_dt_str             
    )         
        SELECT
                    TO_DATE(p_data_dt_str,'YYYYMMDD')               AS DATADATE                 --��������
                    ,DEALNO                                         AS TRANID  --����ID
                    ,'9998'                                      AS TRANORGID                --���׻���ID       Ĭ��Ϊ��10000000-���С�
                    ,'9998'                                      AS ACCORGID                 --�������ID       Ĭ��Ϊ��10000000-���С�
                    ,'0503'                                      AS INSTRUMENTSTYPE          --���ڹ�������     Ĭ��Ϊ��0503	�����ڡ�
                    ,''                                          AS ACCSUBJECTS              --��ƿ�Ŀ
                    ,T1.BOOKTYPE                                 AS BOOKTYPE                 --�˻����
                    ,'0'                                         AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
                    ,T1.CCY                                      AS BUYCURRENCY              --�������
                    ,T1.AMT                                      AS BUYAMOUNT                --������
                    ,T1.CTRCCY                                   AS SELLCURRENCY             --��������
                    ,T1.CTRAMT                                   AS SELLAMOUNT               --�������
                    ,TO_CHAR(TO_DATE(T1.DEALDATE,'yyyyMMdd'),'yyyy-MM-dd')
                                                                 AS STARTDATE                --��ʼ����
                    ,TO_CHAR(TO_DATE(T1.VDATE,'yyyyMMdd'),'yyyy-MM-dd')
                                                                 AS DUEDATE                  --��������
                    ,CASE WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(T1.DEALDATE,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(T1.DEALDATE,'YYYY-MM-DD')) / 365
                            END   --ԭʼ����
                    ,CASE WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                            END   --ʣ������
                    ,NULL                                        AS BUYZERORATE              --���������Ϣ����
                    ,NULL                                        AS BUYDISCOUNTRATE          --������������
                    ,NULL                                        AS SELLZERORATE             --����������Ϣ����
                    ,NULL                                        AS SELLDISCOUNTRATE         --������������
        FROM YSP_WHDQ T1                
        ;
        
    COMMIT;*/
    
    
               
    --2.2 OPICSϵͳ  ��ȡ��㼴��ҵ�� �޳����ۻ�ҵ��  Ĭ����������Ϊ1 ��ΪΪOPICSϵͳ��
    INSERT INTO RWA_DEV.RWA_WH_FEFORWARDSSWAP(
                 DATADATE                              --��������
                ,TRANID                                --����ID
                ,TRANORGID                             --���׻���ID
                ,ACCORGID                              --�������ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,BOOKTYPE                              --�˻����
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,BUYCURRENCY                           --�������
                ,BUYAMOUNT                             --������
                ,SELLCURRENCY                          --��������
                ,SELLAMOUNT                            --�������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                               --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,BUYZERORATE                           --���������Ϣ����
                ,BUYDISCOUNTRATE                       --������������
                ,SELLZERORATE                          --����������Ϣ����
                ,SELLDISCOUNTRATE                      --������������

    )
    WITH YSP_WHYQ AS (

      SELECT         
          W.DEALNO || 'P' AS DEALNO, --��ˮ��       
          COST AS COST,   --�ɱ�����        
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
          CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --�˻�����
          --NULL BUYZERORATE,                           --���������Ϣ����
          --1 BUYDISCOUNTRATE,    --������������
          --NULL SELLZERORATE,                          --����������Ϣ����
          --NULL SELLDISCOUNTRATE                       --������������
     FROM RWA_DEV.OPI_FXDH W --�����Ϣ      
    WHERE SUBSTR(W.COST, 1, 1) = '2' --���ҵ��   
      AND SUBSTR(W.COST, 4, 1) <> '3' --����λ<>3 --ȡ�����˻���    
      AND SUBSTR(W.COST, 6, 1) = '1' --����
      AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --�����ǽ��ۻ�ҵ�� ������Ҷ���һ���Ҷ������        
      AND W.VDATE >= p_data_dt_str  --δ��������        
      AND W.DATANO = p_data_dt_str 
      AND W.VERIND = '1'   
      AND TRIM(W.REVDATE) IS NULL  
      
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
          W.CCYNPVAMT + W.CTRNPVAMT AS CCYNPVAMT, --���м�ֵ
          W.CTRNPVAMT  AS AMT1,     --������ֵ        
          CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --�˻�����
          --NULL BUYZERORATE,                           --���������Ϣ����
          --NULL BUYDISCOUNTRATE,    --������������
          --NULL SELLZERORATE,                          --����������Ϣ����
          --1 SELLDISCOUNTRATE                       --������������
     FROM RWA_DEV.OPI_FXDH W --�����Ϣ    
    WHERE SUBSTR(W.COST, 1, 1) = '2' --���ҵ��   
      AND SUBSTR(W.COST, 4, 1) <> '3' --����λ<>3 --ȡ�����˻���    
      AND SUBSTR(W.COST, 6, 1) = '1' --����
      AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --�����ǽ��ۻ�ҵ�� ������Ҷ���һ���Ҷ������        
      AND W.VDATE >= p_data_dt_str  --δ��������        
      AND W.DATANO = p_data_dt_str 
      AND W.VERIND = '1'   
      AND TRIM(W.REVDATE) IS NULL  
      
    )         
        SELECT
                    TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                    ,DEALNO      AS TRANID  --����ID
                    ,'9998'                                  AS TRANORGID                --���׻���ID       Ĭ��Ϊ��10000000-���С�
                    ,'9998'                                  AS ACCORGID                 --�������ID       Ĭ��Ϊ��10000000-���С�
                    ,'0502'                                      AS INSTRUMENTSTYPE          --���ڹ�������     Ĭ��Ϊ��0503  ���Զ�ڡ�
                    ,''                                          AS ACCSUBJECTS              --��ƿ�Ŀ
                    ,T1.BOOKTYPE                                 AS BOOKTYPE                 --�˻����
                    ,'0'                                         AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
                    ,CASE 
                       WHEN T1.PS = 'P' THEN T1.CCY
                       ELSE NULL   
                     END                                     AS BUYCURRENCY              --�������
                    ,CASE 
                       WHEN T1.PS = 'P' THEN T1.AMT1
                       ELSE NULL   
                     END                                     AS BUYAMOUNT                --������
                    ,CASE 
                       WHEN T1.PS = 'S' THEN T1.CCY
                       ELSE NULL   
                     END                                     AS SELLCURRENCY             --��������
                    ,CASE 
                       WHEN T1.PS = 'S' THEN T1.AMT1
                       ELSE NULL   
                     END                                     AS SELLAMOUNT               --�������
                    ,TO_CHAR(TO_DATE(p_data_dt_str,'yyyyMMdd'),'yyyy-MM-dd') AS STARTDATE                --��ʼ����
                    ,TO_CHAR(TO_DATE(T1.VDATE,'yyyyMMdd'),'yyyy-MM-dd') AS DUEDATE                  --��������
                    ,CASE 
                          WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYY-MM-DD')) / 365
                      END   --ԭʼ����
                    ,CASE WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                      END   --ʣ������
                    ,1              --���������Ϣ����   ����Ľ��ֱ�Ӿ�����ֵ��������Щֱ�Ӹ�ֵ1
                    ,1          --������������
                    ,1             --����������Ϣ����
                    ,1         --������������
        FROM YSP_WHYQ T1 
        ;
  
    COMMIT;
  
    --2.3 OPICSϵͳ  ��ȡ���Զ��ҵ�� �޳����ۻ�ҵ��  ȡ����ʱĬ����������Ϊ1 
    INSERT INTO RWA_DEV.RWA_WH_FEFORWARDSSWAP(
                 DATADATE                              --��������
                ,TRANID                                --����ID
                ,TRANORGID                             --���׻���ID
                ,ACCORGID                              --�������ID
                ,INSTRUMENTSTYPE                       --���ڹ�������
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,BOOKTYPE                              --�˻����
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ���
                ,BUYCURRENCY                           --�������
                ,BUYAMOUNT                             --������
                ,SELLCURRENCY                          --��������
                ,SELLAMOUNT                            --�������
                ,STARTDATE                             --��ʼ����
                ,DUEDATE                               --��������
                ,ORIGINALMATURITY                               --ԭʼ����
                ,RESIDUALM                             --ʣ������
                ,BUYZERORATE                           --���������Ϣ����
                ,BUYDISCOUNTRATE                       --������������
                ,SELLZERORATE                          --����������Ϣ����
                ,SELLDISCOUNTRATE                      --������������

    )
    WITH YSP_WHYQ AS (

      SELECT         
          W.DEALNO || 'P' AS DEALNO, --��ˮ��       
          COST AS COST,   --�ɱ�����        
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
          CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --�˻�����
          --NULL BUYZERORATE,                           --���������Ϣ����
          --1 BUYDISCOUNTRATE,    --������������
          --NULL SELLZERORATE,                          --����������Ϣ����
          --NULL SELLDISCOUNTRATE                       --������������
     FROM RWA_DEV.OPI_FXDH W --�����Ϣ      
    WHERE SUBSTR(W.COST, 1, 1) = '2' --���ҵ��   
      AND SUBSTR(W.COST, 4, 1) <> '3' --����λ<>3 --ȡ�����˻���    
      AND SUBSTR(W.COST, 6, 1) = '2' --Զ��
      AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --�����ǽ��ۻ�ҵ�� ������Ҷ���һ���Ҷ������        
      AND W.VDATE >= p_data_dt_str  --δ��������        
      AND W.DATANO = p_data_dt_str 
      AND W.VERIND = '1'   
      AND TRIM(W.REVDATE) IS NULL  
      
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
          W.CCYNPVAMT + W.CTRNPVAMT AS CCYNPVAMT, --���м�ֵ
          W.CTRNPVAMT  AS AMT1,     --������ֵ        
          CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --�˻�����
          --NULL BUYZERORATE,                           --���������Ϣ����
          --NULL BUYDISCOUNTRATE,    --������������
          --NULL SELLZERORATE,                          --����������Ϣ����
          --1 SELLDISCOUNTRATE                       --������������
     FROM RWA_DEV.OPI_FXDH W --�����Ϣ    
    WHERE SUBSTR(W.COST, 1, 1) = '2' --���ҵ��   
      AND SUBSTR(W.COST, 4, 1) <> '3' --����λ<>3 --ȡ�����˻���    
      AND SUBSTR(W.COST, 6, 1) = '2' --Զ��
      AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --�����ǽ��ۻ�ҵ�� ������Ҷ���һ���Ҷ������        
      AND W.VDATE >= p_data_dt_str  --δ��������        
      AND W.DATANO = p_data_dt_str 
      AND W.VERIND = '1'   
      AND TRIM(W.REVDATE) IS NULL  
      
    )         
        SELECT
                    TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                    ,DEALNO      AS TRANID  --����ID
                    ,'9998'                                  AS TRANORGID                --���׻���ID       Ĭ��Ϊ��10000000-���С�
                    ,'9998'                                  AS ACCORGID                 --�������ID       Ĭ��Ϊ��10000000-���С�
                    ,'0502'                                      AS INSTRUMENTSTYPE          --���ڹ�������     Ĭ��Ϊ��0503  ���Զ�ڡ�
                    ,''                                          AS ACCSUBJECTS              --��ƿ�Ŀ
                    ,T1.BOOKTYPE                                 AS BOOKTYPE                 --�˻����
                    ,'0'                                         AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
                    ,CASE 
                       WHEN T1.PS = 'P' THEN T1.CCY
                       ELSE NULL   
                     END                                     AS BUYCURRENCY              --�������
                    ,CASE 
                       WHEN T1.PS = 'P' THEN T1.AMT1
                       ELSE NULL   
                     END                                     AS BUYAMOUNT                --������
                    ,CASE 
                       WHEN T1.PS = 'S' THEN T1.CCY
                       ELSE NULL   
                     END                                     AS SELLCURRENCY             --��������
                    ,CASE 
                       WHEN T1.PS = 'S' THEN T1.AMT1
                       ELSE NULL   
                     END                                     AS SELLAMOUNT               --�������
                    ,TO_CHAR(TO_DATE(p_data_dt_str,'yyyyMMdd'),'yyyy-MM-dd') AS STARTDATE                --��ʼ����
                    ,TO_CHAR(TO_DATE(T1.VDATE,'yyyyMMdd'),'yyyy-MM-dd') AS DUEDATE                  --��������
                    ,CASE 
                          WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYY-MM-DD')) / 365
                      END   --ԭʼ����
                    ,CASE WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                      END   --ʣ������
                    ,1              --���������Ϣ����
                    ,NULL          --������������
                    ,1             --����������Ϣ����
                    ,NULL         --������������
        FROM YSP_WHYQ T1 
        ;
        
    COMMIT;
    
   /* */
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_WH_FEFORWARDSSWAP',cascade => true);


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_WH_FEFORWARDSSWAP;

  p_po_rtncode := '1';
  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
--�����쳣
EXCEPTION
    WHEN OTHERS THEN
 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '���Զ�ڵ���('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WH_FEFORWARDSSWAP;
/

