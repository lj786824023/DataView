CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XYK_EXPOSURE(
                             p_data_dt_str  IN  VARCHAR2,    --�������� YYYYMMDD
                             P_PO_RTNCODE  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                            P_PO_RTNMSG    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_XYK_EXPOSURE
    ʵ�ֹ���:���ÿ�ϵͳ-���÷��ձ�¶(������Դ����ϵͳ��ҵ�������Ϣȫ������RWA���ÿ��ӿڱ���ձ�¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-04-22
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CSS_CUSTR|Դϵͳ�ͻ��������ϱ�
    Դ  ��2 :RWA_DEV.CCS_ACCT|Դϵͳ����Ҵ����ʻ���
    Դ  ��3 :RWA_DEV.CCS_ACCA|Դϵͳ�˻����Ӽ�¼��
    Դ  ��4 :RWA_DEV.NCM_CUSTOMER_INFO|�ͻ���Ϣ��
    Դ  ��5 :RWA_DEV.CCS_CARD|��Ƭ���ϱ�
    Ŀ���  :RWA_DEV.RWA_XYK_EXPOSURE|���ÿ�ϵͳ���÷��ձ�¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    XLP 20191016  �������üӹ��߼�
    XLP 20191026  �������ÿ� ���ӷֲ�Ʒ  ��¶�������  Ĭ�� 150% ����ס����Ѻ׷�Ӵ���  
    XLP 20191109  �������ÿ��ͻ���Ϣ�ӹ���ʽ�����ݼ��������ӿͻ����ֶ�  RWAϵͳ������ͨ��֤���Ź����ͻ���
    XLP 20191120  ���Ӻϸ����δʹ�ö��20%�߼�����
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XYK_EXPOSURE';
  --�����쳣����
  V_RAISE EXCEPTION;
  --���嵱ǰ����ļ�¼��
  V_COUNT INTEGER;

  BEGIN
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || V_PRO_NAME || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));


     /*�����ȫ�����ݼ����������ʱ��-�˻���ʱ��*/
     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_XYK_CARD';

     --���������˻��µĿ�Ƭ��Ϣ
     INSERT INTO RWA_DEV.RWA_TEMP_XYK_CARD(
                XACCOUNT         --01�˺�
                ,CLOSE_CODE      --02�ʻ�״̬�����ʴ��룩
                ,CANCL_CODE      --03��Ƭע������
                ,MTHS_ODUE       --04��ǰ��������
               )
     SELECT A.XACCOUNT          --01�˺�
            ,A.CLOSE_CODE       --02�ʻ�״̬�����ʴ��룩
            ,B.CANCL_CODE       --03��Ƭע������
            ,A.MTHS_ODUE        --04��ǰ��������
     FROM CCS_ACCT A         --����Ҵ����ʻ�
     INNER JOIN CCS_CARD B   --��Ƭ���ϱ�
     ON A.XACCOUNT = B.XACCOUNT
     AND B.DATANO=p_data_dt_str
     WHERE A.DATANO=p_data_dt_str
     ;
     COMMIT;

     DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',TABNAME => 'RWA_TEMP_XYK_CARD',CASCADE => TRUE);

     --���¿�Ƭ״̬
     --����                                                                                          
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='202'                                                      
      where mths_odue>0;    
                                                                    
                                                                                                                                                                                                                                                                       
      --����                                                                                        
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='204'                                                      
      where trim(cancl_code) in('D' ,'F', 'X' ,'BA','B1','V', 'Y','M', 'N','P', 'S', 'K', 'I','H','B','NX','B2','1H','2H','3H','3B','L','O', 'Z','3X','8H','B4', '4H');                                                                       
                                                                                             
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='204'                                                      
      where trim(close_code) in('D' ,'F', 'X' ,'BA', 'C2','1H','2H','3H','H', 'Z','3X', '1R', '4H');   
        
      --����     
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='203'                                                      
      where trim(cancl_code) is null;                                                                             
                                                                                                        
      --�ر�                                                                                            
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='207'                                                      
      where trim(cancl_code)  in ('T', 'C', 'E','Q', 'PQ');  

      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='207'                                                      
      where trim(close_code)  in ('C','Q','PQ');                                                            
                                                                                                        
      --δ����                                                                                           
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='205'                                                      
      where trim(cancl_code)  ='A';                                           
      commit;  
       
        --˯��                                                                                           
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='206'                                                      
      where trim(cancl_code)  ='U';                                           
      commit; 

       
      --����ͣ��Ϣ��
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='208'                                                      
      where trim(close_code) in ('1S');  
      commit;  

      --ί�����
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='209'                                                      
      where trim(close_code) in ('7H','8H');  
      commit; 

       --����
      update RWA_DEV.RWA_TEMP_XYK_CARD T set cancl_code='201'                                                      
      where trim(close_code) in ('W1','W2','W','WQ');  
      commit; 


     /*�����ȫ�����ݼ����������ʱ��-�˻���ʱ��*/
     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_XYK_ACCT';

     --�����˻���ʱ������ ÿ���˺ŵ���С����
     INSERT INTO RWA_DEV.RWA_TEMP_XYK_ACCT(
            XACCOUNT,
            CANCL_CODE
            )
      SELECT XACCOUNT, MIN(CANCL_CODE) AS CANCL_CODE
        FROM RWA_TEMP_XYK_CARD
       GROUP BY XACCOUNT
      ;
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='����'
      WHERE CANCL_CODE='201';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='����ͣ��Ϣ��'
      WHERE CANCL_CODE='208';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='����'
      WHERE CANCL_CODE='202';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='����'
      WHERE CANCL_CODE='203';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='����'
      WHERE CANCL_CODE='204';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE = 'δ����' WHERE CANCL_CODE = '205';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='˯��' WHERE CANCL_CODE='206';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='�ر�' WHERE CANCL_CODE='207';
      COMMIT;

      UPDATE RWA_TEMP_XYK_ACCT SET CANCL_CODE='ί�����' WHERE CANCL_CODE='209';
      COMMIT;

    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',TABNAME => 'RWA_TEMP_XYK_ACCT',CASCADE => TRUE);


    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.TMP_XYK_YE';
    --����ÿ���˻���Ӧ��������Ϣ ADD20190827
    INSERT INTO TMP_XYK_YE(
         XACCOUNT, --�˺�
         CLOSE_CODE,--�ʻ�״̬�����ʴ��룩
         CRED_LIMIT, --�ʻ����ö��
         TEMP_LIMIT, --��ʱ���
         MP_L_LMT,   --�����ڸ�����
         TLMT_BEG,   --��ʱ�����Ч����
         TLMT_END,   --��ʱ���ʧЧ����
         DK,         --���
         DKLX,       --��Ϣ
         FY          --����
         )
         SELECT A.XACCOUNT, --�˺�
                 A.CLOSE_CODE,--�ʻ�״̬�����ʴ��룩
                 nvl(A.CRED_LIMIT, 0) CRED_LIMIT, --�ʻ����ö��
                 nvl(A.TEMP_LIMIT, 0) TEMP_LIMIT, --��ʱ���
                 nvl(B.MP_L_LMT, 0) MP_L_LMT,--�����ڸ�����
                 A.TLMT_BEG,--��ʱ�����Ч����
                 A.TLMT_END,--��ʱ���ʧЧ����
               (NVL(A.BAL_FREE,0)+NVL(A.STM_BALFRE,0)+NVL(A.BAL_INT,0)+NVL(A.STM_BALINT,0) --������δ���˵���ɣ� + �ʵ�������� + �ռ�Ϣ��δ���˵���ɣ� + �ʵ��ռ�Ϣ���
                + NVL(A.BAL_MP,0)+NVL(A.STM_BALMP,0) -- ���ڸ���δ���ʵ����+���ڸ����ѳ��ʵ����
                + NVL(A.MP_REM_PPL,0)+ NVL(B.MP_REM_PPL,0)) AS DK, --���ڸ���Ŀǰʣ�౾��+������ʣ�౾��
               (NVL(BAL_ORINT, 0)+NVL(BAL_CMPINT, 0)+NVL(STM_BALORI, 0)) AS DKLX, --��Ϣ = ��Ϣ��δ���˵���ɣ� + ������� + �ʵ���Ϣ���
               (NVL(A.BAL_NOINT, 0)+NVL(A.STM_NOINT,0)) AS FY --���� = ����Ϣ��δ���˵���ɣ� + �ʵ���Ϣ��� 
             FROM CCS_ACCT A
        LEFT JOIN CCS_ACCA B
             ON A.XACCOUNT=B.XACCOUNT
             AND A.DATANO=B.DATANO
           WHERE A.DATANO=p_data_dt_str
            --����ɿ�
           AND  TO_NUMBER(BAL_INTFLAG || TO_CHAR(BAL_INT)) +TO_NUMBER(STMBALINTFLAG || TO_CHAR(STM_BALINT)) >= 0
      UNION ALL
        SELECT A.XACCOUNT,--�˺�
               A.CLOSE_CODE,--�ʻ�״̬�����ʴ��룩
               nvl(A.CRED_LIMIT, 0) CRED_LIMIT,--�ʻ����ö��
               nvl(A.TEMP_LIMIT, 0) TEMP_LIMIT,--��ʱ���
               nvl(B.MP_L_LMT, 0) MP_L_LMT,--�����ڸ�����
               A.TLMT_BEG,--��ʱ�����Ч����
               A.TLMT_END,--��ʱ���ʧЧ����
               (NVL(A.MP_REM_PPL,0)+NVL(B.MP_REM_PPL,0)) DK,  --����     �����ٳ���100 ���ڸ���Ŀǰʣ�౾�� + ������ʣ�౾��
               (NVL(BAL_ORINT,0)+NVL(BAL_CMPINT,0)+NVL(STM_BALORI,0))  DKLX, --������Ϣ  ��Ϣ��δ���˵���ɣ� + ������� + �ʵ���Ϣ���
               (NVL(A.BAL_NOINT, 0)+NVL(A.STM_NOINT,0)) AS FY --���� = ����Ϣ��δ���˵���ɣ� + �ʵ���Ϣ��� 
          FROM CCS_ACCT A
        LEFT JOIN CCS_ACCA B
        ON A.XACCOUNT=B.XACCOUNT
       AND A.DATANO=B.DATANO
      WHERE A.DATANO=p_data_dt_str
           --����ɿ�
        AND TO_NUMBER(BAL_INTFLAG || TO_CHAR(BAL_INT)) +TO_NUMBER(STMBALINTFLAG || TO_CHAR(STM_BALINT))<0
      ;
      COMMIT;

      --�����˻�����Ϣ��0  20191026 ����
      UPDATE TMP_XYK_YE SET DK=0, DKLX = 0, FY = 0 WHERE NVL(TRIM(CLOSE_CODE), 'XXX') IN ('W');
      COMMIT;

     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.tmp_xyk_ysy';

     --������ʹ�ö����ʱ��
     INSERT INTO TMP_XYK_YSY(
              XACCOUNT,--�˺�
              CANCL_CODE,--��Ƭע������
              ZED,     --�ܶ��
              DK,      --����ʹ�ö�ȣ�
              DKLX,    --��Ϣ
              FY       --����
     )
     SELECT A.XACCOUNT,
              A.CANCL_CODE,
              B.ZED,
              B.DK,
              B.DKLX,
              B.FY           
         FROM RWA_TEMP_XYK_ACCT A
         LEFT JOIN (
              SELECT A.XACCOUNT,
                     NVL(A.CRED_LIMIT,0)+NVL(A.MP_L_LMT,0) AS ZED, --�ʻ����ö�� + �����ڸ����� = �ܶ��
                     A.DK,
                     A.DKLX,
                     A.FY
                     FROM TMP_XYK_YE A
              WHERE (A.TLMT_BEG>TO_CHAR(TO_DATE(p_data_dt_str, 'yyyymmdd')-1,'yyyymmdd') OR A.TLMT_END<TO_CHAR(TO_DATE(p_data_dt_str, 'yyyymmdd')-1,'yyyymmdd'))  --��ʱ�����Ч���ڣ� ��ʱ��ȵ�������
              UNION ALL
              SELECT A.XACCOUNT,
                     NVL(A.TEMP_LIMIT,0)+NVL(A.MP_L_LMT,0) AS ZED, --��ʱ��� + �����ڸ����� = �ܶ��
                     A.DK,
                     A.DKLX,
                     A.FY
                FROM TMP_XYK_YE A
              WHERE (A.TLMT_BEG<=TO_CHAR(TO_DATE(p_data_dt_str, 'yyyymmdd')-1,'yyyymmdd') AND A.TLMT_END>=TO_CHAR(TO_DATE(p_data_dt_str, 'yyyymmdd')-1,'yyyymmdd')) --��ʱ�����Ч���ڣ� ��ʱ��ȵ�������
         )B
        ON A.XACCOUNT=B.XACCOUNT ;

        COMMIT;


    
     --������ÿ������弶��������
     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.TMP_XYK_WJFL_VIEW';

     --������ÿ������弶�������� 
     INSERT INTO RWA_DEV.TMP_XYK_WJFL_VIEW
        (MTHS_ODUE, CUSTR_NBR, DK, DKLX, FY)
      WITH WJFL_VIEW AS (
        SELECT CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 WHEN B.CUSTR_NBR IS NOT NULL OR B.CUSTR_NBR <> '' THEN B.MTHS_ODUE
                 WHEN C.CUSTR_NBR IS NOT NULL OR C.CUSTR_NBR <> '' THEN C.MTHS_ODUE
                 ELSE T3.MTHS_ODUE
               END  MTHS_ODUE,
               T3.CUSTR_NBR ,
               CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 ELSE T1.ZED
               END ZED,
               CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 ELSE T1.DK
               END DK,	
               CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 ELSE T1.DKLX
               END DKLX,
               CASE
                 WHEN A.CUSTR_NBR IS NOT NULL OR A.CUSTR_NBR <> '' THEN 0
                 ELSE T1.FY
               END FY 
        FROM TMP_XYK_YSY T1 
        LEFT JOIN CCS_ACCT T3 
                ON T1.XACCOUNT = T3.XACCOUNT 
               AND T3.DATANO = p_data_dt_str
        LEFT JOIN (
             SELECT DISTINCT A.CUSTR_NBR
              FROM CCS_ACCT A
             WHERE A.DATANO = p_data_dt_str
               AND TRIM(A.CLOSE_CODE) = 'W'  --���⴦��1
        ) A ON T3.CUSTR_NBR = A.CUSTR_NBR 
        LEFT JOIN (
             SELECT 
            CASE
               WHEN MAX(A.MTHS_ODUE) <= 4 THEN 5
               ELSE  MAX(A.MTHS_ODUE) 
             END MTHS_ODUE,
             A.CUSTR_NBR
          FROM CCS_ACCT A
         WHERE A.DATANO = p_data_dt_str
           AND TRIM(A.CLOSE_CODE) = 'F'  --���⴦��2
         GROUP BY  A.CUSTR_NBR   
        ) B ON T3.CUSTR_NBR = B.CUSTR_NBR 
        LEFT JOIN (
             SELECT CASE
               WHEN MAX(A.MTHS_ODUE) <= 3 THEN 4
               ELSE  MAX(A.MTHS_ODUE) 
             END MTHS_ODUE,
             A.CUSTR_NBR
            FROM CCS_ACCT A
           WHERE A.DATANO = p_data_dt_str
             AND TRIM(A.CLOSE_CODE) = 'C2' --���⴦��3
           GROUP BY  A.CUSTR_NBR
        ) C ON T3.CUSTR_NBR = C.CUSTR_NBR 

      )
             --���ܿͻ����µ����������
             SELECT MAX(MTHS_ODUE) MTHS_ODUE,
                    CUSTR_NBR,
                    SUM(DK) DK,
                    SUM(DKLX) DKLX,
                    SUM(FY) FY
               FROM WJFL_VIEW
              GROUP BY CUSTR_NBR

      ;



    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XYK_EXPOSURE';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ���ÿ�ϵͳ-���÷��ձ�¶-��ʹ�ö��
    INSERT INTO RWA_DEV.RWA_XYK_EXPOSURE(
                DATADATE                        --��������
               ,DATANO                          --������ˮ��
               ,EXPOSUREID                      --���ձ�¶ID
               ,DUEID                           --ծ��ID
               ,SSYSID                          --ԴϵͳID
               ,CONTRACTID                      --��ͬID
               ,CLIENTID                        --��������ID
               ,SORGID                          --Դ����ID
               ,SORGNAME                        --Դ��������
               ,ORGSORTNO                       --�������������
               ,ORGID                           --��������ID
               ,ORGNAME                         --������������
               ,ACCORGID                        --�������ID
               ,ACCORGNAME                      --�����������
               ,INDUSTRYID                      --������ҵ����
               ,INDUSTRYNAME                    --������ҵ����
               ,BUSINESSLINE                    --ҵ������
               ,ASSETTYPE                       --�ʲ�����
               ,ASSETSUBTYPE                    --�ʲ�С��
               ,BUSINESSTYPEID                  --ҵ��Ʒ�ִ���
               ,BUSINESSTYPENAME                --ҵ��Ʒ������
               ,CREDITRISKDATATYPE              --���÷�����������
               ,ASSETTYPEOFHAIRCUTS             --�ۿ�ϵ����Ӧ�ʲ����
               ,BUSINESSTYPESTD                 --Ȩ�ط�ҵ������
               ,EXPOCLASSSTD                    --Ȩ�ط���¶����
               ,EXPOSUBCLASSSTD                 --Ȩ�ط���¶С��
               ,EXPOCLASSIRB                    --��������¶����
               ,EXPOSUBCLASSIRB                 --��������¶С��
               ,EXPOBELONG                      --��¶������ʶ
               ,BOOKTYPE                        --�˻����
               ,REGUTRANTYPE                    --��ܽ�������
               ,REPOTRANFLAG                    --�ع����ױ�ʶ
               ,REVAFREQUENCY                   --�ع�Ƶ��
               ,CURRENCY                        --����
               ,NORMALPRINCIPAL                 --�����������
               ,OVERDUEBALANCE                  --�������
               ,NONACCRUALBALANCE               --��Ӧ�����
               ,ONSHEETBALANCE                  --�������
               ,NORMALINTEREST                  --������Ϣ
               ,ONDEBITINTEREST                 --����ǷϢ
               ,OFFDEBITINTEREST                --����ǷϢ
               ,EXPENSERECEIVABLE               --Ӧ�շ���
               ,ASSETBALANCE                    --�ʲ����
               ,ACCSUBJECT1                     --��Ŀһ
               ,ACCSUBJECT2                     --��Ŀ��
               ,ACCSUBJECT3                     --��Ŀ��
               ,STARTDATE                       --��ʼ����
               ,DUEDATE                         --��������
               ,ORIGINALMATURITY                --ԭʼ����
               ,RESIDUALM                       --ʣ������
               ,RISKCLASSIFY                    --���շ���
               ,EXPOSURESTATUS                  --���ձ�¶״̬
               ,OVERDUEDAYS                     --��������
               ,SPECIALPROVISION                --ר��׼����
               ,GENERALPROVISION                --һ��׼����
               ,ESPECIALPROVISION               --�ر�׼����
               ,WRITTENOFFAMOUNT                --�Ѻ������
               ,OFFEXPOSOURCE                   --���Ⱪ¶��Դ
               ,OFFBUSINESSTYPE                 --����ҵ������
               ,OFFBUSINESSSDVSSTD              --Ȩ�ط�����ҵ������ϸ��
               ,UNCONDCANCELFLAG                --�Ƿ����ʱ����������
               ,CCFLEVEL                        --����ת��ϵ������
               ,CCFAIRB                         --�߼�������ת��ϵ��
               ,CLAIMSLEVEL                     --ծȨ����
               ,BONDFLAG                        --�Ƿ�Ϊծȯ
               ,BONDISSUEINTENT                 --ծȯ����Ŀ��
               ,NSUREALPROPERTYFLAG             --�Ƿ�����ò�����
               ,REPASSETTERMTYPE                --��ծ�ʲ���������
               ,DEPENDONFPOBFLAG                --�Ƿ�����������δ��ӯ��
               ,IRATING                         --�ڲ�����
               ,PD                              --ΥԼ����
               ,LGDLEVEL                        --ΥԼ��ʧ�ʼ���
               ,LGDAIRB                         --�߼���ΥԼ��ʧ��
               ,MAIRB                           --�߼�����Ч����
               ,EADAIRB                         --�߼���ΥԼ���ձ�¶
               ,DEFAULTFLAG                     --ΥԼ��ʶ
               ,BEEL                            --��ΥԼ��¶Ԥ����ʧ����
               ,DEFAULTLGD                      --��ΥԼ��¶ΥԼ��ʧ��
               ,EQUITYEXPOFLAG                  --��Ȩ��¶��ʶ
               ,EQUITYINVESTTYPE                --��ȨͶ�ʶ�������
               ,EQUITYINVESTCAUSE               --��ȨͶ���γ�ԭ��
               ,SLFLAG                          --רҵ�����ʶ
               ,SLTYPE                          --רҵ��������
               ,PFPHASE                         --��Ŀ���ʽ׶�
               ,REGURATING                      --�������
               ,CBRCMPRATINGFLAG                --������϶������Ƿ��Ϊ����
               ,LARGEFLUCFLAG                   --�Ƿ񲨶��Խϴ�
               ,LIQUEXPOFLAG                    --�Ƿ���������з��ձ�¶
               ,PAYMENTDEALFLAG                 --�Ƿ����Ը�ģʽ
               ,DELAYTRADINGDAYS                --�ӳٽ�������
               ,SECURITIESFLAG                  --�м�֤ȯ��ʶ
               ,SECUISSUERID                    --֤ȯ������ID
               ,RATINGDURATIONTYPE              --������������
               ,SECUISSUERATING                 --֤ȯ���еȼ�
               ,SECURESIDUALM                   --֤ȯʣ������
               ,SECUREVAFREQUENCY               --֤ȯ�ع�Ƶ��
               ,CCPTRANFLAG                     --�Ƿ����뽻�׶�����ؽ���
               ,CCPID                           --���뽻�׶���ID
               ,QUALCCPFLAG                     --�Ƿ�ϸ����뽻�׶���
               ,BANKROLE                        --���н�ɫ
               ,CLEARINGMETHOD                  --���㷽ʽ
               ,BANKASSETFLAG                   --�Ƿ������ύ�ʲ�
               ,MATCHCONDITIONS                 --�����������
               ,SFTFLAG                         --֤ȯ���ʽ��ױ�ʶ
               ,MASTERNETAGREEFLAG              --���������Э���ʶ
               ,MASTERNETAGREEID                --���������Э��ID
               ,SFTTYPE                         --֤ȯ���ʽ�������
               ,SECUOWNERTRANSFLAG              --֤ȯ����Ȩ�Ƿ�ת��
               ,OTCFLAG                         --�����������߱�ʶ
               ,VALIDNETTINGFLAG                --��Ч�������Э���ʶ
               ,VALIDNETAGREEMENTID             --��Ч�������Э��ID
               ,OTCTYPE                         --����������������
               ,DEPOSITRISKPERIOD               --��֤������ڼ�
               ,MTM                             --���óɱ�
               ,MTMCURRENCY                     --���óɱ�����
               ,BUYERORSELLER                   --������
               ,QUALROFLAG                      --�ϸ�����ʲ���ʶ
               ,ROISSUERPERFORMFLAG             --�����ʲ��������Ƿ�����Լ
               ,BUYERINSOLVENCYFLAG             --���ñ������Ƿ��Ʋ�
               ,NONPAYMENTFEES                  --��δ֧������
               ,RETAILEXPOFLAG                  --���۱�¶��ʶ
               ,RETAILCLAIMTYPE                 --����ծȨ����
               ,MORTGAGETYPE                    --ס����Ѻ��������
               ,EXPONUMBER                      --���ձ�¶����
               ,LTV                             --�����ֵ��
               ,AGING                           --����
               ,NEWDEFAULTDEBTFLAG              --����ΥԼծ���ʶ
               ,PDPOOLMODELID                   --PD�ֳ�ģ��ID
               ,LGDPOOLMODELID                  --LGD�ֳ�ģ��ID
               ,CCFPOOLMODELID                  --CCF�ֳ�ģ��ID
               ,PDPOOLID                        --����PD��ID
               ,LGDPOOLID                       --����LGD��ID
               ,CCFPOOLID                       --����CCF��ID
               ,ABSUAFLAG                       --�ʲ�֤ȯ�������ʲ���ʶ
               ,ABSPOOLID                       --֤ȯ���ʲ���ID
               ,GROUPID                         --������
               ,DEFAULTDATE                     --ΥԼʱ��
               ,ABSPROPORTION                   --�ʲ�֤ȯ������
               ,DEBTORNUMBER                    --����˸���
               ,CLIENTNAME                      --�ͻ�����
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.DATANO                                   AS DATANO                   --������ˮ��
                ,T1.XACCOUNT                                 AS EXPOSUREID               --���ձ�¶ID
                ,T1.XACCOUNT                                 AS DUEID                    --ծ��ID
                ,'XYK'                                       AS SSYSID                   --Դϵͳ����                Ĭ�ϣ����ÿ�(XYK)
                ,T1.XACCOUNT                                 AS CONTRACTID               --��ͬID
                ,NVL(T1.CUST_NO, T1.CUSTR_NBR)             AS CLIENTID                 --��������ID                �����������˺����ͻ���
                ,'9997'                                  AS SORGID                   --Դ����ID
                ,'�������йɷ����޹�˾���ÿ�����'                               AS SORGNAME                  --Դ��������
                ,'1600'                                   AS ORGSORTNO                --�������������
                ,'9997'                                  AS ORGID                    --������������
                ,'�������йɷ����޹�˾���ÿ�����'                               AS ORGNAME                   --������������
                ,'9997'                                  AS ACCORGID                 --�������ID
                ,'�������йɷ����޹�˾���ÿ�����'                               AS ACCORGNAME               --�����������
                ,'999999'                                    AS INDUSTRYID               --������ҵ����             Ĭ�ϣ���
                ,'δ֪'                                      AS INDUSTRYNAME             --������ҵ����             Ĭ�ϣ���
                ,'0301'                                      AS BUSINESSLINE             --����                     Ĭ�ϣ���������(2)
                ,'115'                                       AS ASSETTYPE                --�ʲ�����                  Ĭ��
                ,'11501'                                     AS ASSETSUBTYPE             --�ʲ�С��                  Ĭ��
                ,'11106010'                                  AS BUSINESSTYPEID           --ҵ��Ʒ�ִ���              ���ÿ�ҵ��
                ,'���ÿ����'                                 AS BUSINESSTYPENAME         --ҵ��Ʒ������              �̶�ֵ�����ÿ�
                ,'02'                                        AS CREDITRISKDATATYPE       --���÷�����������          Ĭ�ϣ�һ������(02)
                ,'01'                                        AS ASSETTYPEOFHAIRCUTS      --�ۿ�ϵ����Ӧ�ʲ����     Ĭ��: �ֽ��ֽ�ȼ���(01)
                ,'04'                                        AS BUSINESSTYPESTD          --Ȩ�ط�ҵ������           �̶�ֵ������
                ,'0108'                                      AS EXPOCLASSSTD              --Ȩ�ط���¶����           �̶�ֵ���Ը���ծȨ
                ,CASE 
                   WHEN T1.CATEGORY = '40' THEN  '010802' --���ÿ��˻���� 40 Ϊ ���ӷֲ�Ʒ  ֱ�Ӱ� 010802	����ס����Ѻ׷�Ӵ���  150%����Ȩ�ؼ���
                   ELSE '010803'  --010803	�Ը�������ծȨ 75%����
                 END                                         AS EXPOSUBCLASSSTD          --Ȩ�ط���¶С��          
                ,''                                          AS EXPOCLASSIRB              --��������¶����           �̶�ֵ�����۷��ձ�¶
                ,''                                          AS EXPOSUBCLASSIRB          --��������¶С��           �̶�ֵ���ϸ�ѭ�����ձ�¶
                ,'01'                                        AS EXPOBELONG                --��¶������ʶ              �̶�ֵ������(01)
                ,'01'                                        AS BOOKTYPE                  --�˻����                  �̶�ֵ��01-�����˻�
                ,'03'                                        AS REGUTRANTYPE             --��ܽ�������              �̶�ֵ����Ѻ����(03)
                ,'0'                                         AS REPOTRANFLAG             --�ع����ױ�ʶ               �̶�ֵ����(0)
                ,1                                           AS REVAFREQUENCY            --�ع�Ƶ��                  Ĭ�ϣ� 1
                ,'CNY'                                       AS CURRENCY                 --����                         �̶�ֵ�������
                , YSY.DK                  AS NORMALPRINCIPAL          --�����������             ��ǰ���
                ,0                                           AS OVERDUEBALANCE            --�������                  Ĭ�ϣ�0
                ,0                                           AS NONACCRUALBALANCE        --��Ӧ�����                Ĭ�ϣ�0
                ,YSY.DK                   AS ONSHEETBALANCE           --�������                   �������=�����������
                ,0                                            AS NORMALINTEREST           --������Ϣ                  Ĭ�ϣ�0
                ,0                                            AS ONDEBITINTEREST          --����ǷϢ                  �ʵ���Ϣ���+��Ϣ�������  NVL(T1.STM_BALORI,0)+NVL(T1.BAL_ORINT,0)
                ,0                                            AS OFFDEBITINTEREST         --����ǷϢ NVL(T1.BAL_CMPINT,0)
                ,0                                            AS EXPENSERECEIVABLE        --Ӧ�շ���                  ����Ϣ���+�˵���Ϣ��� NVL(T1.BAL_NOINT,0)+NVL(T1.STM_NOINT,0)
                ,YSY.DK                   AS ASSETBALANCE             --�ʲ����                 �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ
                ,CASE WHEN T2.MTHS_ODUE=0
                           THEN '130704'	--���-���ÿ����

                      WHEN T2.MTHS_ODUE>0 AND T2.MTHS_ODUE<=3
                           THEN '131001'	--���ڴ���-���ÿ��������                       
                      WHEN T2.MTHS_ODUE>3
                           THEN '131001'	--���ڴ���-���ÿ��������                       
                    ELSE '130704'	--���-���ÿ����
                END                                          AS ACCSUBJECT1               --��Ŀһ
                ,''                                           AS ACCSUBJECT2               --��Ŀ��
                ,''                                            AS ACCSUBJECT3               --��Ŀ��
                ,T1.DAY_OPENED                                AS STARTDATE                 --��ʼ����
                ,p_data_dt_str                                AS DUEDATE                    --��������
                ,0                                            AS ORIGINALMATURITY         --ԭʼ����
                ,0                                           AS RESIDUALM                 --ʣ������
                ,CASE WHEN T2.MTHS_ODUE=0 THEN '01'
                      WHEN T2.MTHS_ODUE>=1 AND T2.MTHS_ODUE<=3 THEN '02'
                      WHEN T2.MTHS_ODUE=4 THEN '03'
                      WHEN T2.MTHS_ODUE>=5 AND T2.MTHS_ODUE<=6 THEN '04'
                      ELSE '05'
                      END                                     AS RISKCLASSIFY             --���շ���                  �������������жϣ�����������£�0��������1-3����ע��4���μ���5-6�����ɣ�>=7����ʧ
                ,CASE WHEN T2.MTHS_ODUE=0 THEN '01'
                      WHEN T2.MTHS_ODUE>=1 AND T2.MTHS_ODUE<=3 THEN '02'
                      ELSE '03'
                END                                          AS EXPOSURESTATUS           --���ձ�¶״̬             �������������жϣ�����������£�0��������1-3�����ڣ�>=4������
                ,T2.MTHS_ODUE                                AS OVERDUEDAYS              --��������
                ,0                                           AS SPECIALPROVISION         --ר��׼����               RWA����
                ,0                                           AS GENERALPROVISION         --һ��׼����               RWA����
                ,0                                           AS ESPECIALPROVISION        --�ر�׼����               RWA����
                ,0                                           AS WRITTENOFFAMOUNT         --�Ѻ������               Ĭ�ϣ�0
                ,''                                          AS OFFEXPOSOURCE            --���Ⱪ¶��Դ             Ĭ�ϣ���
                ,''                                          AS OFFBUSINESSTYPE          --����ҵ������             Ĭ�ϣ���
                ,''                                          AS OFFBUSINESSSDVSSTD       --Ȩ�ط�����ҵ������ϸ��   Ĭ�ϣ���
                ,'0'                                         AS UNCONDCANCELFLAG         --�Ƿ����ʱ����������     Ĭ�ϣ�'��'
                ,''                                          AS CCFLEVEL                 --����ת��ϵ������         Ĭ�ϣ���
                ,T4.CCFVALUE                                 AS CCFAIRB                  --�߼�������ת��ϵ��       Ĭ�ϣ���
                ,'01'                                        AS CLAIMSLEVEL              --ծȨ����                 Ĭ�ϣ��߼�ծȨ(01)
                ,'0'                                         AS BONDFLAG                 --�Ƿ�Ϊծȯ               Ĭ�ϣ���(0)
                ,'02'                                        AS BONDISSUEINTENT          --ծȯ����Ŀ��             Ĭ�ϣ���(0)
                ,'0'                                         AS NSUREALPROPERTYFLAG      --�Ƿ�����ò�����         Ĭ�ϣ���(0)
                ,''                                          AS REPASSETTERMTYPE         --��ծ�ʲ���������         Ĭ�ϣ���
                ,'0'                                         AS DEPENDONFPOBFLAG         --�Ƿ�����������δ��ӯ��   Ĭ�ϣ���(0)
                ,''                                          AS IRATING                  --�ڲ�����                 ����
                ,T4.PDVALUE                                  AS PD                       --ΥԼ����                 ����
                ,''                                          AS LGDLEVEL                 --ΥԼ��ʧ�ʼ���           ����
                ,T4.LGDVALUE                                 AS LGDAIRB                  --�߼���ΥԼ��ʧ��         ����
                ,NULL                                        AS MAIRB                    --�߼�����Ч����           ����
                ,YSY.DK                                        AS EADAIRB                  --�߼���ΥԼ���ձ�¶
                ,CASE WHEN T4.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                         AS DEFAULTFLAG              --ΥԼ��ʶ
                ,NVL(T4.BEELVALUE,0)                                AS BEEL                     --��ΥԼ��¶Ԥ����ʧ����
                ,NVL(T4.LGDVALUE,0)                          AS DEFAULTLGD               --��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                         AS EQUITYEXPOFLAG           --��Ȩ��¶��ʶ
                ,''                                          AS EQUITYINVESTTYPE         --��ȨͶ�ʶ�������
                ,''                                          AS EQUITYINVESTCAUSE        --��ȨͶ���γ�ԭ��
                ,'1'                                         AS SLFLAG                   --רҵ�����ʶ
                ,'02030302'                                  AS SLTYPE                   --רҵ��������
                ,''                                          AS PFPHASE                   --��Ŀ���ʽ׶�
                ,'01'                                        AS REGURATING               --�������
                ,'0'                                         AS CBRCMPRATINGFLAG         --������϶������Ƿ��Ϊ����
                ,'0'                                         AS LARGEFLUCFLAG            --�Ƿ񲨶��Խϴ�
                ,'0'                                         AS LIQUEXPOFLAG             --�Ƿ���������з��ձ�¶
                ,'0'                                         AS PAYMENTDEALFLAG          --�Ƿ����Ը�ģʽ
                ,0                                           AS DELAYTRADINGDAYS         --�ӳٽ�������
                ,'0'                                         AS SECURITIESFLAG           --�м�֤ȯ��ʶ
                ,''                                          AS SECUISSUERID             --֤ȯ������ID
                ,''                                          AS RATINGDURATIONTYPE       --������������
                ,''                                          AS SECUISSUERATING          --֤ȯ���еȼ�
                ,0                                           AS SECURESIDUALM            --֤ȯʣ������
                ,1                                           AS SECUREVAFREQUENCY        --֤ȯ�ع�Ƶ��
                ,'0'                                         AS CCPTRANFLAG              --�Ƿ����뽻�׶�����ؽ���
                ,''                                          AS CCPID                    --���뽻�׶���ID
                ,'0'                                         AS QUALCCPFLAG              --�Ƿ�ϸ����뽻�׶���
                ,'02'                                        AS BANKROLE                 --���н�ɫ
                ,'02'                                        AS CLEARINGMETHOD           --���㷽ʽ
                ,'0'                                         AS BANKASSETFLAG            --�Ƿ������ύ�ʲ�
                ,'01'                                        AS MATCHCONDITIONS          --�����������
                ,'0'                                         AS SFTFLAG                  --֤ȯ���ʽ��ױ�ʶ
                ,'0'                                         AS MASTERNETAGREEFLAG       --���������Э���ʶ
                ,''                                          AS MASTERNETAGREEID         --���������Э��ID
                ,'01'                                        AS SFTTYPE                  --֤ȯ���ʽ�������
                ,'0'                                         AS SECUOWNERTRANSFLAG       --֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                         AS OTCFLAG                  --�����������߱�ʶ
                ,'0'                                         AS VALIDNETTINGFLAG         --��Ч�������Э���ʶ
                ,''                                          AS VALIDNETAGREEMENTID      --��Ч�������Э��ID
                ,'01'                                        AS OTCTYPE                  --����������������
                ,0                                           AS DEPOSITRISKPERIOD        --��֤������ڼ�
                ,0                                           AS MTM                      --���óɱ�
                ,'01'                                        AS MTMCURRENCY              --���óɱ�����
                ,'01'                                        AS BUYERORSELLER            --������
                ,'0'                                         AS QUALROFLAG               --�ϸ�����ʲ���ʶ
                ,'0'                                         AS ROISSUERPERFORMFLAG      --�����ʲ��������Ƿ�����Լ
                ,'0'                                         AS BUYERINSOLVENCYFLAG      --���ñ������Ƿ��Ʋ�
                ,0                                           AS NONPAYMENTFEES           --��δ֧������
                ,'1'                                         AS RETAILEXPOFLAG           --���۱�¶��ʶ             Ĭ�ϣ���(1)
                ,'020403'                                    AS RETAILCLAIMTYPE          --����ծȨ����             Ĭ�ϣ���
                ,'0'                                         AS MORTGAGETYPE             --ס����Ѻ��������         Ĭ�ϣ���(0)
                ,1                                           AS EXPONUMBER               --���ձ�¶����             Ĭ�ϣ���
                ,0.8                                         AS LTV                     --�����ֵ��  ͳһ����
                ,T4.MOB                                      AS AGING                   --����
                ,T1.NEWDEFAULTFLAG                           AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,T4.PDMODELCODE                              AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,T4.LGDMODELCODE                             AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,T4.CCFMODELCODE                             AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,T4.PDCODE                                   AS PDPOOLID                -- ����PD��ID
                ,T4.LGDCODE                                  AS LGDPOOLID               -- ����LGD��ID
                ,T4.CCFCODE                                  AS CCFPOOLID               -- ����CCF��ID
                ,'0'                                         AS ABSUAFLAG                --�ʲ�֤ȯ�������ʲ���ʶ   Ĭ�ϣ���(0)
                ,''                                          AS ABSPOOLID                --֤ȯ���ʲ���ID           Ĭ�ϣ���
                ,''                                          AS GROUPID                  --������                 RWAϵͳ��ֵ
                ,CASE WHEN T4.DEFAULTFLAG='1' THEN TO_DATE(T4.UPDATETIME,'YYYYMMDD')
                      ELSE NULL
                 END                                          AS DEFAULTDATE              --ΥԼʱ��
                ,NULL                                        AS ABSPROPORTION            --�ʲ�֤ȯ������
                ,NULL                                        AS DEBTORNUMBER             --����˸���
                ,''                             AS CLIENTNAME               --�ͻ�����
            FROM        RWA_DEV.TMP_XYK_YSY YSY --��ʹ����ʱ��
            LEFT JOIN   RWA_DEV.CCS_ACCT T1  --����Ҵ����ʻ�
                 ON     YSY.XACCOUNT = T1.XACCOUNT
                 AND    T1.DATANO= p_data_dt_str
            LEFT JOIN   RWA_DEV.RWA_TEMP_LGDLEVEL T4
                 ON     T1.XACCOUNT=T4.BUSINESSID
            AND         T4.BUSINESSTYPE='CREDITCARD'
            LEFT JOIN   RWA_DEV.TMP_XYK_WJFL_VIEW T2
                   ON   T1.CUSTR_NBR = T2.CUSTR_NBR
            WHERE YSY.DK > 0 --ֻ������������
            ;
            
     COMMIT;




    --2.1 ���ÿ�ϵͳ-���÷��ձ�¶-δʹ�ö��
    INSERT INTO RWA_DEV.RWA_XYK_EXPOSURE(
                DATADATE                        --��������
               ,DATANO                          --������ˮ��
               ,EXPOSUREID                      --���ձ�¶ID
               ,DUEID                           --ծ��ID
               ,SSYSID                          --ԴϵͳID
               ,CONTRACTID                      --��ͬID
               ,CLIENTID                        --��������ID
               ,SORGID                          --Դ����ID
               ,SORGNAME                        --Դ��������
               ,ORGSORTNO                       --�������������
               ,ORGID                           --��������ID
               ,ORGNAME                         --������������
               ,ACCORGID                        --�������ID
               ,ACCORGNAME                      --�����������
               ,INDUSTRYID                      --������ҵ����
               ,INDUSTRYNAME                    --������ҵ����
               ,BUSINESSLINE                    --ҵ������
               ,ASSETTYPE                       --�ʲ�����
               ,ASSETSUBTYPE                    --�ʲ�С��
               ,BUSINESSTYPEID                  --ҵ��Ʒ�ִ���
               ,BUSINESSTYPENAME                --ҵ��Ʒ������
               ,CREDITRISKDATATYPE              --���÷�����������
               ,ASSETTYPEOFHAIRCUTS             --�ۿ�ϵ����Ӧ�ʲ����
               ,BUSINESSTYPESTD                 --Ȩ�ط�ҵ������
               ,EXPOCLASSSTD                    --Ȩ�ط���¶����
               ,EXPOSUBCLASSSTD                 --Ȩ�ط���¶С��
               ,EXPOCLASSIRB                    --��������¶����
               ,EXPOSUBCLASSIRB                 --��������¶С��
               ,EXPOBELONG                      --��¶������ʶ
               ,BOOKTYPE                        --�˻����
               ,REGUTRANTYPE                    --��ܽ�������
               ,REPOTRANFLAG                    --�ع����ױ�ʶ
               ,REVAFREQUENCY                   --�ع�Ƶ��
               ,CURRENCY                        --����
               ,NORMALPRINCIPAL                 --�����������
               ,OVERDUEBALANCE                  --�������
               ,NONACCRUALBALANCE               --��Ӧ�����
               ,ONSHEETBALANCE                  --�������
               ,NORMALINTEREST                  --������Ϣ
               ,ONDEBITINTEREST                 --����ǷϢ
               ,OFFDEBITINTEREST                --����ǷϢ
               ,EXPENSERECEIVABLE               --Ӧ�շ���
               ,ASSETBALANCE                    --�ʲ����
               ,ACCSUBJECT1                     --��Ŀһ
               ,ACCSUBJECT2                     --��Ŀ��
               ,ACCSUBJECT3                     --��Ŀ��
               ,STARTDATE                       --��ʼ����
               ,DUEDATE                         --��������
               ,ORIGINALMATURITY                --ԭʼ����
               ,RESIDUALM                       --ʣ������
               ,RISKCLASSIFY                    --���շ���
               ,EXPOSURESTATUS                  --���ձ�¶״̬
               ,OVERDUEDAYS                     --��������
               ,SPECIALPROVISION                --ר��׼����
               ,GENERALPROVISION                --һ��׼����
               ,ESPECIALPROVISION               --�ر�׼����
               ,WRITTENOFFAMOUNT                --�Ѻ������
               ,OFFEXPOSOURCE                   --���Ⱪ¶��Դ
               ,OFFBUSINESSTYPE                 --����ҵ������
               ,OFFBUSINESSSDVSSTD              --Ȩ�ط�����ҵ������ϸ��
               ,UNCONDCANCELFLAG                --�Ƿ����ʱ����������
               ,CCFLEVEL                        --����ת��ϵ������
               ,CCFAIRB                         --�߼�������ת��ϵ��
               ,CLAIMSLEVEL                     --ծȨ����
               ,BONDFLAG                        --�Ƿ�Ϊծȯ
               ,BONDISSUEINTENT                 --ծȯ����Ŀ��
               ,NSUREALPROPERTYFLAG             --�Ƿ�����ò�����
               ,REPASSETTERMTYPE                --��ծ�ʲ���������
               ,DEPENDONFPOBFLAG                --�Ƿ�����������δ��ӯ��
               ,IRATING                         --�ڲ�����
               ,PD                              --ΥԼ����
               ,LGDLEVEL                        --ΥԼ��ʧ�ʼ���
               ,LGDAIRB                         --�߼���ΥԼ��ʧ��
               ,MAIRB                           --�߼�����Ч����
               ,EADAIRB                         --�߼���ΥԼ���ձ�¶
               ,DEFAULTFLAG                     --ΥԼ��ʶ
               ,BEEL                            --��ΥԼ��¶Ԥ����ʧ����
               ,DEFAULTLGD                      --��ΥԼ��¶ΥԼ��ʧ��
               ,EQUITYEXPOFLAG                  --��Ȩ��¶��ʶ
               ,EQUITYINVESTTYPE                --��ȨͶ�ʶ�������
               ,EQUITYINVESTCAUSE               --��ȨͶ���γ�ԭ��
               ,SLFLAG                          --רҵ�����ʶ
               ,SLTYPE                          --רҵ��������
               ,PFPHASE                         --��Ŀ���ʽ׶�
               ,REGURATING                      --�������
               ,CBRCMPRATINGFLAG                --������϶������Ƿ��Ϊ����
               ,LARGEFLUCFLAG                   --�Ƿ񲨶��Խϴ�
               ,LIQUEXPOFLAG                    --�Ƿ���������з��ձ�¶
               ,PAYMENTDEALFLAG                 --�Ƿ����Ը�ģʽ
               ,DELAYTRADINGDAYS                --�ӳٽ�������
               ,SECURITIESFLAG                  --�м�֤ȯ��ʶ
               ,SECUISSUERID                    --֤ȯ������ID
               ,RATINGDURATIONTYPE              --������������
               ,SECUISSUERATING                 --֤ȯ���еȼ�
               ,SECURESIDUALM                   --֤ȯʣ������
               ,SECUREVAFREQUENCY               --֤ȯ�ع�Ƶ��
               ,CCPTRANFLAG                     --�Ƿ����뽻�׶�����ؽ���
               ,CCPID                           --���뽻�׶���ID
               ,QUALCCPFLAG                     --�Ƿ�ϸ����뽻�׶���
               ,BANKROLE                        --���н�ɫ
               ,CLEARINGMETHOD                  --���㷽ʽ
               ,BANKASSETFLAG                   --�Ƿ������ύ�ʲ�
               ,MATCHCONDITIONS                 --�����������
               ,SFTFLAG                         --֤ȯ���ʽ��ױ�ʶ
               ,MASTERNETAGREEFLAG              --���������Э���ʶ
               ,MASTERNETAGREEID                --���������Э��ID
               ,SFTTYPE                         --֤ȯ���ʽ�������
               ,SECUOWNERTRANSFLAG              --֤ȯ����Ȩ�Ƿ�ת��
               ,OTCFLAG                         --�����������߱�ʶ
               ,VALIDNETTINGFLAG                --��Ч�������Э���ʶ
               ,VALIDNETAGREEMENTID             --��Ч�������Э��ID
               ,OTCTYPE                         --����������������
               ,DEPOSITRISKPERIOD               --��֤������ڼ�
               ,MTM                             --���óɱ�
               ,MTMCURRENCY                     --���óɱ�����
               ,BUYERORSELLER                   --������
               ,QUALROFLAG                      --�ϸ�����ʲ���ʶ
               ,ROISSUERPERFORMFLAG             --�����ʲ��������Ƿ�����Լ
               ,BUYERINSOLVENCYFLAG             --���ñ������Ƿ��Ʋ�
               ,NONPAYMENTFEES                  --��δ֧������
               ,RETAILEXPOFLAG                  --���۱�¶��ʶ
               ,RETAILCLAIMTYPE                 --����ծȨ����
               ,MORTGAGETYPE                    --ס����Ѻ��������
               ,EXPONUMBER                      --���ձ�¶����
               ,LTV                             --�����ֵ��
               ,AGING                           --����
               ,NEWDEFAULTDEBTFLAG              --����ΥԼծ���ʶ
               ,PDPOOLMODELID                   --PD�ֳ�ģ��ID
               ,LGDPOOLMODELID                  --LGD�ֳ�ģ��ID
               ,CCFPOOLMODELID                  --CCF�ֳ�ģ��ID
               ,PDPOOLID                        --����PD��ID
               ,LGDPOOLID                       --����LGD��ID
               ,CCFPOOLID                       --����CCF��ID
               ,ABSUAFLAG                       --�ʲ�֤ȯ�������ʲ���ʶ
               ,ABSPOOLID                       --֤ȯ���ʲ���ID
               ,GROUPID                         --������
               ,DEFAULTDATE                     --ΥԼʱ��
               ,ABSPROPORTION                   --�ʲ�֤ȯ������
               ,DEBTORNUMBER                    --����˸���
               ,CLIENTNAME                      --�ͻ�����
    )
   WITH TMPE_HGFLAG AS (
          --4.3.1���ϱ�׼��δʹ�ö��
          --1.  �˻����=�����˿���
          --2.  ������ʶ=���޵�����
          --ע�����Ҫ����������������������У���1�����Ŷ���Ϊ��Ȼ�ˣ����ŷ�ʽΪ�޵���ѭ������
          --                                    ��2����ͬһ�ֿ��˵����Ŷ�Ȳ�����100�������
          SELECT T1.XACCOUNT, --�˺�
                 T5.FLAG1,    --�Ƿ񵣱�
                 T5.FLAG2,    --�Ƿ�ѭ��
                 T4.ZED       --�ͻ������Ŷ��  �Կͻ���Ϊ���Ļ���
          FROM TMP_XYK_YSY T1
          LEFT JOIN CCS_ACCT T3
            ON T1.XACCOUNT = T3.XACCOUNT
           AND T3.DATANO = p_data_dt_str
          LEFT JOIN (               
                     SELECT SUM(T1.ZED) ZED, T2.DOCUMENT_ID
                       FROM RWA_DEV.TMP_XYK_YSY T1
                       LEFT JOIN RWA_DEV.CCS_GUAR T2
                         ON T1.XACCOUNT = T2.ACCT_NO
                      WHERE T2.DATANO = p_data_dt_str
                      GROUP BY T2.DOCUMENT_ID
                     
                     ) T4 --���ʹ�����
            ON T3.CUSTR_NBR = T4.DOCUMENT_ID
          LEFT JOIN RWA_DEV.CCS_GUAR T5
            ON T1.XACCOUNT = T5.ACCT_NO
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,T1.DATANO                                   AS DATANO                   --������ˮ��
                ,'BW_'||T1.XACCOUNT                          AS EXPOSUREID               --���ձ�¶ID
                ,T1.XACCOUNT                              AS DUEID                    --ծ��ID
                ,'XYK'                                       AS SSYSID                   --Դϵͳ����                Ĭ�ϣ����ÿ�(XYK)
                ,'BW_'||T1.XACCOUNT                          AS CONTRACTID               --��ͬID
                ,NVL(T1.CUST_NO, T1.CUSTR_NBR)             AS CLIENTID                 --��������ID              �������ϵ����˺����ͻ���
                ,'9997'                                  AS SORGID                    --Դ����ID
                ,'�������йɷ����޹�˾���ÿ�����'        AS SORGNAME                 --Դ��������
                ,'1600'                                   AS ORGSORTNO                --�������������
                ,'9997'                                  AS ORGID                    --������������
                ,'�������йɷ����޹�˾���ÿ�����'        AS ORGNAME                  --������������
                ,'9997'                                  AS ACCORGID                 --�������ID
                ,'�������йɷ����޹�˾���ÿ�����'        AS ACCORGNAME               --�����������
                ,'999999'                                          AS INDUSTRYID               --������ҵ����             Ĭ�ϣ���
                ,'δ֪'                                          AS INDUSTRYNAME             --������ҵ����             Ĭ�ϣ���
                ,'0301'                                      AS BUSINESSLINE             --����                     Ĭ�ϣ���������(2)
                ,'215'                                       AS ASSETTYPE                --�ʲ�����
                ,'21503'                                     AS ASSETSUBTYPE             --�ʲ�С��
                ,'11106020'                                  AS BUSINESSTYPEID           --ҵ��Ʒ�ִ���              ���ÿ�ҵ��
                ,'���ÿ����_δʹ�ö��'                      AS BUSINESSTYPENAME         --ҵ��Ʒ������              �̶�ֵ�����ÿ�
                ,'02'                                        AS CREDITRISKDATATYPE       --���÷�����������          Ĭ�ϣ�һ������(02)
                ,'01'                                        AS ASSETTYPEOFHAIRCUTS      --�ۿ�ϵ����Ӧ�ʲ����     Ĭ��: �ֽ��ֽ�ȼ���(01)
                ,'04'                                        AS BUSINESSTYPESTD          --Ȩ�ط�ҵ������           �̶�ֵ������
                ,'0108'                                      AS EXPOCLASSSTD              --Ȩ�ط���¶����           �̶�ֵ���Ը���ծȨ
                ,'010803'                                    AS EXPOSUBCLASSSTD          --Ȩ�ط���¶С��           �̶�ֵ���Ը�������ծȨ
                ,''                                          AS EXPOCLASSIRB              --��������¶����           �̶�ֵ�����۷��ձ�¶
                ,''                                          AS EXPOSUBCLASSIRB          --��������¶С��           �̶�ֵ���ϸ�ѭ�����ձ�¶
                ,'02'                                        AS EXPOBELONG                --��¶������ʶ              �̶�ֵ������(02)
                ,'01'                                        AS BOOKTYPE                  --�˻����                  �̶�ֵ��01-�����˻�
                ,'03'                                        AS REGUTRANTYPE             --��ܽ�������              �̶�ֵ����Ѻ����(03)
                ,'0'                                         AS REPOTRANFLAG             --�ع����ױ�ʶ               �̶�ֵ����(0)
                ,1                                           AS REVAFREQUENCY            --�ع�Ƶ��                  Ĭ�ϣ� 1
                ,'CNY'                                       AS CURRENCY                 --����                         �̶�ֵ�������
                ,WSY.WSYED                                   AS NORMALPRINCIPAL          --�����������             ��ǰ���
                ,0                                           AS OVERDUEBALANCE            --�������                  Ĭ�ϣ�0
                ,0                                           AS NONACCRUALBALANCE        --��Ӧ�����                Ĭ�ϣ�0
                ,WSY.WSYED                                       AS ONSHEETBALANCE           --�������                   �������=�����������+�������+��Ӧ�����
                ,0                                            AS NORMALINTEREST           --������Ϣ                  Ĭ�ϣ�0
                ,0                                           AS ONDEBITINTEREST          --����ǷϢ                  �ʵ���Ϣ���+��Ϣ�������
                ,0                                           AS OFFDEBITINTEREST         --����ǷϢ                  �������
                ,0                                           AS EXPENSERECEIVABLE        --Ӧ�շ���                  Ĭ�ϣ�0
                ,WSY.WSYED                                       AS ASSETBALANCE             --�ʲ����                 �����ʲ������=�������+Ӧ�շ���+������Ϣ+����ǷϢ
                ,'71190000'                                  AS ACCSUBJECT1               --��Ŀһ
                ,''                                           AS ACCSUBJECT2               --��Ŀ��
                ,''                                            AS ACCSUBJECT3               --��Ŀ��
                ,T1.DAY_OPENED                                AS STARTDATE                 --��ʼ����
                ,p_data_dt_str                                AS DUEDATE                    --��������
                ,0                                            AS ORIGINALMATURITY         --ԭʼ����
                ,0                                           AS RESIDUALM                 --ʣ������
                ,CASE WHEN T3.MTHS_ODUE=0 THEN '01'
                      WHEN T3.MTHS_ODUE>=1 AND T3.MTHS_ODUE<=3 THEN '02'
                      WHEN T3.MTHS_ODUE=4 THEN '03'
                      WHEN T3.MTHS_ODUE>=5 AND T3.MTHS_ODUE<=6 THEN '04'
                      ELSE '05'
                      END                                     AS RISKCLASSIFY             --���շ���                  �������������жϣ�����������£�0��������1-3����ע��4���μ���5-6�����ɣ�>=7����ʧ
                ,CASE WHEN T3.MTHS_ODUE=0 THEN '01'
                      WHEN T3.MTHS_ODUE>=1 AND T3.MTHS_ODUE<=3 THEN '02'
                      ELSE '03'
                END                                          AS EXPOSURESTATUS           --���ձ�¶״̬              �������������жϣ�����������£�0��������1-3�����ڣ�>=4������
                ,T3.MTHS_ODUE                                AS OVERDUEDAYS              --��������
                ,0                                           AS SPECIALPROVISION         --ר��׼����               RWA����
                ,0                                           AS GENERALPROVISION         --һ��׼����               RWA����
                ,0                                           AS ESPECIALPROVISION        --�ر�׼����               RWA����
                ,0                                           AS WRITTENOFFAMOUNT         --�Ѻ������               Ĭ�ϣ�0
                ,'01'                                        AS OFFEXPOSOURCE            --���Ⱪ¶��Դ             Ĭ�ϣ���
                ,CASE WHEN T2.FLAG1 = '0' AND T2.FLAG2 = '1' AND T2.ZED < 1000000 THEN '04'  --04  ���ϱ�׼��δʹ�õ����ÿ����Ŷ��
                      ELSE '03'  --03 һ��δʹ�õ����ÿ����Ŷ��
                 END                                         AS OFFBUSINESSTYPE          --����ҵ������
                ,CASE WHEN T2.FLAG1 = '0' AND T2.FLAG2 = '1' AND T2.ZED < 1000000 THEN '0401' --0401  ���ϱ�׼��δʹ�õ����ÿ����Ŷ��
                      ELSE '0301'  --0301  �������ÿ����Ŷ��
                 END                                         AS OFFBUSINESSSDVSSTD       --Ȩ�ط�����ҵ������ϸ��
                ,'0'                                         AS UNCONDCANCELFLAG         --�Ƿ����ʱ����������     Ĭ�ϣ�'��'
                ,''                                          AS CCFLEVEL                 --����ת��ϵ������         Ĭ�ϣ���
                ,T5.CCFVALUE                                 AS CCFAIRB                  --�߼�������ת��ϵ��       Ĭ�ϣ���
                ,'01'                                        AS CLAIMSLEVEL              --ծȨ����                 Ĭ�ϣ��߼�ծȨ(01)
                ,'0'                                         AS BONDFLAG                 --�Ƿ�Ϊծȯ               Ĭ�ϣ���(0)
                ,'0'                                         AS BONDISSUEINTENT          --ծȯ����Ŀ��             Ĭ�ϣ���(0)
                ,'0'                                         AS NSUREALPROPERTYFLAG      --�Ƿ�����ò�����         Ĭ�ϣ���(0)
                ,''                                          AS REPASSETTERMTYPE         --��ծ�ʲ���������         Ĭ�ϣ���
                ,'0'                                         AS DEPENDONFPOBFLAG         --�Ƿ�����������δ��ӯ��   Ĭ�ϣ���(0)
                ,''                                          AS IRATING                  --�ڲ�����                 ����
                ,T5.PDVALUE                                  AS PD                       --ΥԼ����                 ����
                ,''                                          AS LGDLEVEL                 --ΥԼ��ʧ�ʼ���           ����
                ,T5.LGDVALUE                                 AS LGDAIRB                  --�߼���ΥԼ��ʧ��         ����
                ,NULL                                        AS MAIRB                    --�߼�����Ч����           ����
                ,WSY.WSYED*NVL(T5.CCFVALUE,1)                   AS EADAIRB                  --�߼���ΥԼ���ձ�¶
                ,CASE WHEN T5.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                         AS DEFAULTFLAG              --ΥԼ��ʶ                 ����
                ,NVL(T5.BEELVALUE,0)                                AS BEEL                     --��ΥԼ��¶Ԥ����ʧ����   ����
                ,NVL(T5.LGDVALUE,0)                          AS DEFAULTLGD               --��ΥԼ��¶ΥԼ��ʧ��     ����
                ,'0'                                         AS EQUITYEXPOFLAG           --��Ȩ��¶��ʶ
                ,''                                          AS EQUITYINVESTTYPE         --��ȨͶ�ʶ�������
                ,''                                          AS EQUITYINVESTCAUSE        --��ȨͶ���γ�ԭ��
                ,'1'                                         AS SLFLAG                   --רҵ�����ʶ
                ,'02030302'                                  AS SLTYPE                   --רҵ��������
                ,''                                          AS PFPHASE                   --��Ŀ���ʽ׶�
                ,'01'                                        AS REGURATING               --�������
                ,'0'                                         AS CBRCMPRATINGFLAG         --������϶������Ƿ��Ϊ����
                ,'0'                                         AS LARGEFLUCFLAG            --�Ƿ񲨶��Խϴ�
                ,'0'                                         AS LIQUEXPOFLAG             --�Ƿ���������з��ձ�¶
                ,'0'                                         AS PAYMENTDEALFLAG          --�Ƿ����Ը�ģʽ
                ,0                                           AS DELAYTRADINGDAYS         --�ӳٽ�������
                ,'0'                                         AS SECURITIESFLAG           --�м�֤ȯ��ʶ
                ,''                                          AS SECUISSUERID             --֤ȯ������ID
                ,''                                          AS RATINGDURATIONTYPE       --������������
                ,''                                          AS SECUISSUERATING          --֤ȯ���еȼ�
                ,0                                           AS SECURESIDUALM            --֤ȯʣ������
                ,1                                           AS SECUREVAFREQUENCY        --֤ȯ�ع�Ƶ��
                ,'0'                                         AS CCPTRANFLAG              --�Ƿ����뽻�׶�����ؽ���
                ,''                                          AS CCPID                    --���뽻�׶���ID
                ,'0'                                         AS QUALCCPFLAG              --�Ƿ�ϸ����뽻�׶���
                ,'02'                                        AS BANKROLE                 --���н�ɫ
                ,'02'                                        AS CLEARINGMETHOD           --���㷽ʽ
                ,'0'                                         AS BANKASSETFLAG            --�Ƿ������ύ�ʲ�
                ,'01'                                        AS MATCHCONDITIONS          --�����������
                ,'0'                                         AS SFTFLAG                  --֤ȯ���ʽ��ױ�ʶ
                ,'0'                                         AS MASTERNETAGREEFLAG       --���������Э���ʶ
                ,''                                          AS MASTERNETAGREEID         --���������Э��ID
                ,'01'                                        AS SFTTYPE                  --֤ȯ���ʽ�������
                ,'0'                                         AS SECUOWNERTRANSFLAG       --֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                         AS OTCFLAG                  --�����������߱�ʶ
                ,'0'                                         AS VALIDNETTINGFLAG         --��Ч�������Э���ʶ
                ,''                                          AS VALIDNETAGREEMENTID      --��Ч�������Э��ID
                ,'01'                                        AS OTCTYPE                  --����������������
                ,0                                           AS DEPOSITRISKPERIOD        --��֤������ڼ�
                ,0                                           AS MTM                      --���óɱ�
                ,'01'                                        AS MTMCURRENCY              --���óɱ�����
                ,'01'                                        AS BUYERORSELLER            --������
                ,'0'                                         AS QUALROFLAG               --�ϸ�����ʲ���ʶ
                ,'0'                                         AS ROISSUERPERFORMFLAG      --�����ʲ��������Ƿ�����Լ
                ,'0'                                         AS BUYERINSOLVENCYFLAG      --���ñ������Ƿ��Ʋ�
                ,0                                           AS NONPAYMENTFEES           --��δ֧������
                ,'1'                                         AS RETAILEXPOFLAG           --���۱�¶��ʶ             Ĭ�ϣ���(1)
                ,CASE WHEN T2.FLAG1 = '0' AND T2.FLAG2 = '1' AND T2.ZED < 1000000 THEN '020402'  --020402  �ϸ�ѭ�����۷��ձ�¶
                      ELSE '020403'  --020403  �������۷��ձ�¶
                 END                                         AS RETAILCLAIMTYPE          --����ծȨ����             Ĭ�ϣ���
                ,'0'                                         AS MORTGAGETYPE             --ס����Ѻ��������         Ĭ�ϣ���(0)
                ,1                                           AS EXPONUMBER               --���ձ�¶����             Ĭ�ϣ���
                ,0.8                                         AS LTV                     --�����ֵ��  ͳһ����
                ,T5.MOB                                      AS AGING                   --����
                ,T1.NEWDEFAULTFLAG                           AS NEWDEFAULTDEBTFLAG      --����ΥԼծ���ʶ
                ,T5.PDMODELCODE                              AS PDPOOLMODELID           -- PD�ֳ�ģ��ID
                ,T5.LGDMODELCODE                             AS LGDPOOLMODELID          -- LGD�ֳ�ģ��ID
                ,T5.CCFMODELCODE                             AS CCFPOOLMODELID          -- CCF�ֳ�ģ��ID
                ,T5.PDCODE                                   AS PDPOOLID                -- ����PD��ID
                ,T5.LGDCODE                                  AS LGDPOOLID               -- ����LGD��ID
                ,T5.CCFCODE                                  AS CCFPOOLID               -- ����CCF��ID
                ,'0'                                         AS ABSUAFLAG                --�ʲ�֤ȯ�������ʲ���ʶ   Ĭ�ϣ���(0)
                ,''                                          AS ABSPOOLID                --֤ȯ���ʲ���ID           Ĭ�ϣ���
                ,''                                          AS GROUPID                  --������                 RWAϵͳ��ֵ
                ,''                                          AS DEFAULTDATE              --ΥԼʱ��
                ,NULL                                        AS ABSPROPORTION           --�ʲ�֤ȯ������
                ,NULL                                        AS DEBTORNUMBER            --����˸���
                ,''                             AS CLIENTNAME               --�ͻ�����
        FROM (
                SELECT C4.XACCOUNT,
                       0 AS WSYED  --δʹ�ö��
                  FROM TMP_XYK_YSY C4
                 WHERE (C4.ZED - C4.DK - C4.DKLX - C4.FY) < 0
                   AND TRIM(CANCL_CODE) NOT IN ('�ر�', '����') --��Ƭע������
                GROUP BY C4.XACCOUNT
                
                UNION ALL  
                SELECT C4.XACCOUNT, 
                       SUM(C4.ZED - C4.DK - C4.DKLX - C4.FY) AS WSYED --δʹ�ö��
                  FROM TMP_XYK_YSY C4
                 WHERE (C4.ZED - C4.DK - C4.DKLX - C4.FY) >= 0
                   AND TRIM(CANCL_CODE) NOT IN ('�ر�', '����') --��Ƭע������  
                 GROUP BY C4.XACCOUNT
              ) WSY  --���ÿ�δʹ�ö����ϸ����
    LEFT JOIN RWA_DEV.CCS_ACCT T1
           ON WSY.XACCOUNT = T1.XACCOUNT
    LEFT JOIN RWA_DEV.RWA_TEMP_LGDLEVEL T5
    ON T1.XACCOUNT=T5.BUSINESSID
    AND T5.BUSINESSTYPE='CREDITCARD'
    LEFT JOIN TMPE_HGFLAG T2  --���ϱ�׼�ı���ҵ��
         ON T1.XACCOUNT = T2.XACCOUNT
    LEFT JOIN RWA_DEV.TMP_XYK_WJFL_VIEW T3  --�弶��������
         ON  T1.CUSTR_NBR = T3.CUSTR_NBR         
    WHERE T1.DATANO = p_data_dt_str    
      AND WSY.WSYED <> 0
      ;

    COMMIT;

    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',TABNAME => 'RWA_XYK_EXPOSURE',CASCADE => TRUE);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_XYK_EXPOSURE;

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '�ɹ�'||'-'||V_COUNT;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||SQLCODE||';������ϢΪ:'||SQLERRM||';��������Ϊ:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
          ROLLBACK;
          P_PO_RTNCODE := SQLCODE;
          P_PO_RTNMSG  := '���÷��ձ�¶('|| V_PRO_NAME ||')ETLת��ʧ�ܣ�'|| SQLERRM||';��������Ϊ:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         RETURN;
END PRO_RWA_XYK_EXPOSURE;
/

