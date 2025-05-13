CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_CUSTOMER_TYPE0910(p_data_dt_str IN  VARCHAR2, --�������� yyyyMMdd
                                                             p_po_rtncode  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                                             p_po_rtnmsg   OUT VARCHAR2  --��������
                                                            )

/*
�洢��������:PRO_RWA_CD_CUSTOMER_TYPE
ʵ�ֹ���:���㱩¶����.�ͻ����ʹ�С�����
��  ��  :V1.0.0
��д��  :QHJIANG
��дʱ��:2016-05-26
��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
Դ  ��1 :RWA_DEV.RWA_EI_CLIENT
Դ  ��2 :RWA_DEV.RWA_CD_CUSNAMELIST
Դ  ��3 :RWA_DEV.BL_CUSTOMER_INFO
Դ  ��4 :RWA_DEV.NCM_CUSTOMER_INFO
Ŀ���1 :RWA_DEV.RWA_CD_CUSTOMER_TYPE
������  :��
�����¼(�޸���|�޸�ʱ��|�޸�����):
        QHJIANG|2017/03/30|���ڿ���_�������Ŵ��Ŀͻ����ݵĿͻ����ʹ�С���Ŵ������ɣ�����RWAֻ�������Ŵ�δ���ɿͻ����ʹ�С�������
        QHJIANG|2017/05/02|���ڿ���_�����С��ҵ�Ŀͻ�����
*/

AS
    --����һ����������
    PRAGMA AUTONOMOUS_TRANSACTION;

    /*��������*/
    --����洢�������Ʋ���ֵ
    v_pro_name VARCHAR2(200) := 'PRO_RWA_CD_CUSTOMER_TYPE';
    --�����쳣����
    v_raise EXCEPTION;
    --���嵱ǰ����ļ�¼��
    v_count INTEGER;



BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --1 ��Ȩ�ͻ�ƥ��
    --1.1 ��տͻ�������Ϣ��ʱ��
    DELETE FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

		--3 ���˿ͻ��϶࣬�ȷֳ����˿ͻ�
    --3.1 ���ȷֳ���¼��ĸ��˿ͻ�
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'04'                    AS CUSTYPE         --�����������
           ,'����'                  AS CUSTYPE_NAME    --���������������
           ,'0401'                  AS CUSSUBTYPE      --��������С��
           ,'���ˣ���Ȼ�ˣ�'        AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str        AS DATANO --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND		REC.SSYSID = 'XYK'						 --���ÿ��ͻ�ȫ�Ǹ���
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;
     COMMIT;

    --3 ���˿ͻ��϶࣬�ȷֳ����˿ͻ�
    --3.1 ���ȷֳ���¼��ĸ��˿ͻ�
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'04'                    AS CUSTYPE         --�����������
           ,'����'                  AS CUSTYPE_NAME    --���������������
           ,'0401'                  AS CUSSUBTYPE      --��������С��
           ,'���ˣ���Ȼ�ˣ�'        AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str        AS DATANO --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN  RWA_DEV.BL_CUSTOMER_INFO BCI          --��¼�ͻ���
      ON REC.CLIENTID=BCI.CUSTOMERID
      AND REC.DATANO=BCI.DATANO
      AND (BCI.CUSTOMERTYPE='0321000001' OR BCI.CERTTYPE LIKE 'Ind%')           --���ۿͻ�
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;
     COMMIT;
    --3 ���˿ͻ��϶࣬�ȷֳ����˿ͻ�
    --3.1 Ȼ��ֳ�Դϵͳ��ĸ��˿ͻ�
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'04'                    AS CUSTYPE         --�����������
           ,'����'                  AS CUSTYPE_NAME    --���������������
           ,'0401'                  AS CUSSUBTYPE      --��������С��
           ,'���ˣ���Ȼ�ˣ�'        AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN  RWA_DEV.NCM_CUSTOMER_INFO CCI           --Դϵͳ�ͻ���Ϣ��
      ON CCI.CUSTOMERID=REC.CLIENTID
      AND CCI.DATANO=REC.DATANO
      AND (CCI.CUSTOMERTYPE='0321000001' OR CCI.CERTTYPE LIKE 'Ind%')           --���ۿͻ�
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --��Ȩ��ͻ����嵥ƥ��
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,RCC.ATTRIBUTE1          AS CUSTYPE         --�����������
           ,'��Ȩ'                  AS CUSTYPE_NAME    --���������������
           ,RCC.ATTRIBUTE2          AS CUSSUBTYPE      --��������С��
           ,RCC.LISTNAME            AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN RWA_DEV.RWA_CD_CUSNAMELIST RCC
      ON RCC.CUSTNAME=REC.CLIENTNAME
      AND RCC.ATTRIBUTE1='01'      --��Ȩ��ͻ�ȫ���嵥ȫƥ��
      AND RCC.ISINUSE = '1'
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.2 ��ƥ���ϵĽ��ڻ���.���������еĽ��ڻ���������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID                                             AS CLIENTID        --��������ID
           ,REC.CLIENTNAME                                           AS CLIENTNAME      --������������
           ,'02'                                                     AS CUSTYPE         --�����������
           ,'���ڻ���'                                               AS CUSTYPE_NAME    --���������������
           ,'0201'                                                   AS CUSSUBTYPE      --��������С��
           ,'�й�����������'                                         AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')                        AS DATADATE        --��������
           ,p_data_dt_str                                            AS DATANO          --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      --AND  REC.REGISTSTATE='01'                  --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND (REC.CLIENTNAME LIKE '%���ҿ�������%'     --ũ�������磬���������磬ũ���������
           OR REC.CLIENTNAME LIKE '%������%'
           OR REC.CLIENTNAME LIKE '%�й�����������%'
           OR REC.CLIENTNAME LIKE '%����������%'
           OR REC.CLIENTNAME LIKE '%�й�ũҵ��չ����%'
           OR REC.CLIENTNAME LIKE '%ũ����%'
           )
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2 ���ڻ����ͻ�ƥ��
    --2.1 ���ڿͻ�����ƥ��
    --2.1.1 ��ƥ���ϵĽ��ڻ���.�й���������Ͷ�ʵĽ����ʲ�����˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,RCC.ATTRIBUTE1          AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,case when REC.REGISTSTATE='01'
                 then RCC.ATTRIBUTE2
                 when REC.REGISTSTATE<>'01' and RCC.LISTTYPE='CCEN_AMC'
                 then '0208'
                 else '0206' end    AS CUSSUBTYPE      --��������С��
           ,case when REC.REGISTSTATE='01'
                 then RCC.LISTNAME
                 when REC.REGISTSTATE<>'01' and RCC.LISTTYPE='CCEN_AMC'
                 then '�����������ڻ���'
                 else '������ҵ����' end            AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN RWA_DEV.RWA_CD_CUSNAMELIST RCC
      ON RCC.CUSTNAME=REC.CLIENTNAME
      AND RCC.LISTTYPE ='CCEN_AMC'   --���ڻ���.�й���������Ͷ�ʵĽ����ʲ�����˾
      AND RCC.ISINUSE = '1'
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      --AND REC.REGISTSTATE='01'   --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.2 ��ƥ���ϵĽ��ڻ���.�������չ��ڴ��Ľ��ڻ���������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID                                             AS CLIENTID        --��������ID
           ,REC.CLIENTNAME                                           AS CLIENTNAME      --������������
           ,'02'                                                     AS CUSTYPE         --�����������
           ,'���ڻ���'                                               AS CUSTYPE_NAME    --���������������
           ,'0203'                                                   AS CUSSUBTYPE      --��������С��
           ,'�й�ũ��������С�ũ������������չ��ڴ��Ľ��ڻ���'   AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')                        AS DATADATE        --��������
           ,p_data_dt_str                                            AS DATANO          --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND  REC.REGISTSTATE='01'                  --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND (REC.CLIENTNAME LIKE '%ũ��%������%'     --ũ�������磬���������磬ũ���������
           OR REC.CLIENTNAME LIKE '%����%������%'
           OR REC.CLIENTNAME LIKE '%ũ��%����%'
           OR REC.CLIENTNAME LIKE '%ũ%��%��%'
           OR REC.CLIENTNAME LIKE '%ũ%��%��%'
           OR REC.CLIENTNAME LIKE '%����%'
           OR REC.CLIENTNAME LIKE '%������%')
      AND LENGTHB(CLIENTNAME)>9
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3 ��ƥ���ϵĽ��ڻ���.ũ���ʽ�����|���˾|����Ͷ�ʹ�˾|����˾|�������ڷ���˾|���ѽ��ڹ�˾|֤ȯ��˾|���չ�˾|��ҵ���Ų���˾|�������޹�˾|���Ҿ��͹�˾|�������еĽ��ڻ���������ʱ����
    --2.1.3.1 ��ƥ����ũ���ʽ����������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020501'                AS CUSSUBTYPE      --��������С��
           ,'ũ���ʽ�����'        AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%ũ��%�ʽ�%������%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.2 ��ƥ���ϴ��˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020502'                AS CUSSUBTYPE      --��������С��
           ,'���˾'              AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%��˾%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.3 ��ƥ��������Ͷ�ʹ�˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020503'                AS CUSSUBTYPE      --��������С��
           ,'����Ͷ�ʹ�˾'          AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND (REC.CLIENTNAME LIKE '%����%Ͷ��%' OR REC.CLIENTNAME LIKE '%����%')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

   COMMIT;

   --2.1.3.4 ��ƥ������ҵ���Ų���˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020509'                AS CUSSUBTYPE      --��������С��
           ,'��ҵ���Ų���˾'      AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%����%��˾%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;


    --2.1.3.5 ��ƥ�����������ڷ���˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020505'                AS CUSSUBTYPE      --��������С��
           ,'�������ڷ���˾'      AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%����%��˾%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.6 ��ƥ�������ѽ��ڹ�˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020506'                AS CUSSUBTYPE      --��������С��
           ,'���ѽ��ڹ�˾'          AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%����%��˾%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

   COMMIT;

    --2.1.3.7 ��ƥ����֤ȯ��˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020507'                AS CUSSUBTYPE      --��������С��
           ,'֤ȯ��˾'              AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND (REC.CLIENTNAME NOT LIKE '%����%' AND REC.CLIENTNAME LIKE '%֤ȯ%'��
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.8 ��ƥ���ϱ��չ�˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020508'                AS CUSSUBTYPE      --��������С��
           ,'���չ�˾'              AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%��˾%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.9 ��ƥ���ϲ���˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020504'                AS CUSSUBTYPE      --��������С��
           ,'����˾'              AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%��˾%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.10 ��ƥ���Ͻ������޹�˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020510'                AS CUSSUBTYPE      --��������С��
           ,'�������޹�˾'          AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%����%��˾%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.11 ��ƥ���ϻ��Ҿ��͹�˾������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020511'                AS CUSSUBTYPE      --��������С��
           ,'���Ҿ��͹�˾'          AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%����%��˾%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.12 ��ƥ���ϴ������д�����ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'020512'                AS CUSSUBTYPE      --��������С��
           ,'��������'              AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND REC.CLIENTNAME LIKE '%����%����%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.4 ͨ���ؼ��ʡ����С�ģ��ƥ����ҵ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'0202'                  AS CUSSUBTYPE      --��������С��
           ,'�й���ҵ����'          AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND (REC.CLIENTNAME LIKE '%����%' OR REC.CLIENTNAME LIKE '%֧��%' --OR REC.CLIENTNAME LIKE '%����%'  ���ο�ȷ��ɾ������ؼ���
            OR REC.CLIENTNAME LIKE '%����%' OR REC.CLIENTNAME LIKE '%ũ����%' OR REC.CLIENTNAME LIKE '%����%' OR REC.CLIENTNAME LIKE '%ũ��%'
            OR REC.CLIENTNAME LIKE '%����ŦԼ%' OR REC.CLIENTNAME LIKE '%�������%')
      AND LENGTHB(CLIENTNAME)>9
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.12 ��ƥ���ϴ������д�����ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'0205'                  AS CUSSUBTYPE      --��������С��
           ,'�й��������ڻ���'      AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND (REC.CLIENTNAME LIKE '%�����ʲ�����%' OR REC.CLIENTNAME LIKE '%����%����%'
           OR REC.CLIENTNAME LIKE '%���ͬҵ%' OR REC.CLIENTNAME LIKE '%����%��˾%' OR REC.CLIENTNAME LIKE '%����%')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.5 ���������2.1�������������ڹ��ҵ���Ϊ'�й�'����ͻ����ͱ�ʶΪ�й��������ڻ���
   /* INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(    --������ȷ�ϣ����ɾ��
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'0205'                  AS CUSSUBTYPE      --��������С��
           ,'�й��������ڻ���'      AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN RWA_DEV.NCM_CUSTOMER_INFO CCI
      ON CCI.CUSTOMERID=REC.CLIENTID
      AND REC.DATANO=CCI.DATANO
      AND CCI.CUSTOMERTYPE='0321000003'
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'    --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;  */

    --2.2 ����ͻ�����ƥ��
    --2.2.1 ��ƥ���ϵĽ��ڻ���.��߿������С������������к͹��ʻ��һ�����֯������ʱ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,RCC.ATTRIBUTE1          AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,RCC.ATTRIBUTE2          AS CUSSUBTYPE      --��������С��
           ,RCC.LISTNAME            AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN RWA_DEV.RWA_CD_CUSNAMELIST RCC
      ON REC.CLIENTNAME=RCC.CUSTNAME
      AND RCC.LISTTYPE='MDB'                   --��߿������С������������к͹��ʻ��һ�����֯��MDB������
      AND RCC.ISINUSE = '1'
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      --AND REC.REGISTSTATE='02'
      ;

    COMMIT;

    --2.2.2 ͨ���ؼ��ʡ����С�ģ��ƥ����ҵ����
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'0206'                  AS CUSSUBTYPE      --��������С��
           ,'������ҵ����'          AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='02'                 --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND (REC.CLIENTNAME LIKE '%����%' OR REC.CLIENTNAME LIKE '%����ŦԼ%' OR REC.CLIENTNAME LIKE '%�������%' OR REC.CLIENTNAME LIKE '%����ס��%')      --ͨ���ؼ��ʡ����С�ģ��ƥ����ҵ����
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.2.3 ���������2.1�����������ͻ�����Ϊ'���ڻ���' ���ڹ��ҵ���Ϊ'���й�'����ͻ����ͱ�ʶΪ�����������ڻ���
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'02'                    AS CUSTYPE         --�����������
           ,'���ڻ���'              AS CUSTYPE_NAME    --���������������
           ,'0208'                  AS CUSSUBTYPE      --��������С��
           ,'�����������ڻ���'      AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='02'    --�����ڹ��ҵ�����Ϊ���й��� �й�(01) ���й�(02)
      AND (REC.CLIENTNAME LIKE '%����%'OR REC.CLIENTNAME LIKE '%����%' OR REC.CLIENTNAME LIKE '%����%'
           OR REC.CLIENTNAME LIKE '%֤ȯ%' OR REC.CLIENTNAME LIKE '%����%' OR REC.CLIENTNAME LIKE '%����%' OR REC.CLIENTNAME LIKE '%������%'
           OR REC.CLIENTNAME LIKE '%������%'  OR REC.CLIENTNAME LIKE '%����ŦԼ%' OR REC.CLIENTNAME LIKE '%�������%'
           OR REC.CLIENTNAME LIKE '%ũ��%�ʽ�%������%' OR REC.CLIENTNAME LIKE '%����%����%��˾%' OR REC.CLIENTNAME LIKE '%����%����%��˾%' --add by qhjiang 2017/03/30 ���ڿ��� ��Ӿ����������ڻ���ƥ������
           OR REC.CLIENTNAME LIKE '%����%����%��˾%' OR REC.CLIENTNAME LIKE '%����%����%' --add by qhjiang 2017/03/30 ���ڿ��� ��Ӿ����������ڻ���ƥ������
           OR EXISTS(SELECT 1 --add by qhjiang 2017/03/30 ���ڿ��� ��Ӿ����������ڻ���ƥ������
                       FROM RWA_DEV.NCM_CUSTOMER_INFO CCI
                      WHERE CCI.CUSTOMERID=REC.CLIENTID
                        AND REC.DATANO=CCI.DATANO
                        AND CCI.CUSTOMERTYPE='0321000003')
           )
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;


    --4 ��˾�ͻ����࣬ʣ�µĶ��ֵ���˾�ͻ�
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --��������ID
                ,CLIENTNAME         --������������
                ,RWACUSTYPE         --RWA�����������
                ,RWACUSTYPE_NAME    --RWA���������������
                ,RWACUSSUBTYPE      --RWA��������С��
                ,RWACUSSUBTYPE_NAME --RWA��������С������
                ,DATADATE           --��������
                ,DATANO             --������ˮ��
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --��������ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --������������
           ,'03'                    AS CUSTYPE         --�����������
           ,'��˾'                  AS CUSTYPE_NAME    --���������������
           ,CASE WHEN REC.ANNUALSALE is not null AND REC.ANNUALSALE <=300000000 AND REC.ANNUALSALE > 0
                 THEN '0302'
                 ELSE '0301' END    AS CUSSUBTYPE--��������С�� ��С��ҵ-0302 һ�㹫˾-0301   --add by qhjiang 2017/05/02 �����С��ҵ�Ŀͻ�����
           ,CASE WHEN REC.ANNUALSALE is not null AND REC.ANNUALSALE <=300000000 AND REC.ANNUALSALE > 0
                 THEN '��С��ҵ'
                 ELSE 'һ�㹫˾' END AS CUSSUBTYPE_NAME --��������С������
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --��������
           ,p_data_dt_str                            AS DATANO   --������ˮ��
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_CD_CUSTOMER_TYPE',cascade => true);

----------------------------------------------���²��������Ĳ��������С��----------------------------------------------------------
    MERGE INTO (SELECT CLIENTID,CUSTYPE,CUSTYPE_NAME,CUSSUBTYPE,CUSSUBTYPE_NAME FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')) T
    USING (SELECT T1.CLIENTID,T1.CLIENTNAME,T1.CLIENTTYPE,T2.ITEMNAME AS CUSTYPE_NAME,T1.CLIENTSUBTYPE,T3.ITEMNAME AS CUSSUBTYPE_NAME
             FROM RWA_DEV.RWA_EI_CLIENT T1
        LEFT JOIN RWA.CODE_LIBRARY T2
               ON T1.CLIENTTYPE = T2.ITEMNO
              AND T2.CODENO = 'ClientCategory'
        LEFT JOIN RWA.CODE_LIBRARY T3
               ON T1.CLIENTSUBTYPE = T3.ITEMNO
              AND T3.CODENO = 'ClientCategory'
            WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
              AND T1.CLIENTTYPE IS NOT NULL
              AND T1.CLIENTSUBTYPE IS NOT NULL) TC
       ON (T.CLIENTID = TC.CLIENTID)
     WHEN MATCHED THEN
   UPDATE SET T.CUSTYPE = TC.CLIENTTYPE,T.CUSSUBTYPE=TC.CLIENTSUBTYPE,T.CUSTYPE_NAME=TC.CUSTYPE_NAME,T.CUSSUBTYPE_NAME=TC.CUSSUBTYPE_NAME;
    COMMIT;

    MERGE INTO (SELECT CLIENTID,CLIENTTYPE,CLIENTSUBTYPE FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND CLIENTTYPE IS NULL AND CLIENTSUBTYPE IS NULL) T
    USING (SELECT T1.CLIENTID,T1.CLIENTNAME,T1.RWACUSTYPE,T1.RWACUSSUBTYPE
             FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE T1
            WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
              AND T1.RWACUSTYPE IS NOT NULL
              AND T1.RWACUSSUBTYPE IS NOT NULL) T2
       ON (T.CLIENTID = T2.CLIENTID)
     WHEN MATCHED THEN
   UPDATE SET T.CLIENTTYPE = T2.RWACUSTYPE,T.CLIENTSUBTYPE=T2.RWACUSSUBTYPE;
    COMMIT;

    --ȥ�����ڻ������ڲ�������������������ϵͳ�����ǽ��ڻ��� ��������
    UPDATE RWA_DEV.RWA_EI_CLIENT
		   SET DEFAULTFLAG    = '0',
		       MODELIRATING   = NULL,
		       MODELPD        = NULL,
		       IRATING        = NULL,
		       PD             = NULL,
		       MODELID        = NULL,
		       NEWDEFAULTFLAG = '0',
		       DEFAULTDATE    = NULL
		 WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		   AND CLIENTTYPE = '02'
    ;

    COMMIT;

    --ȥ�����ڻ������ڲ�������������������ϵͳ�����ǽ��ڻ��� ���ձ�¶
    UPDATE RWA_DEV.RWA_EI_EXPOSURE T
		   SET T.IRATING            = NULL,
		       T.PD                 = NULL,
		       T.DEFAULTFLAG        = '0',
		       T.BEEL               = NULL,
		       T.DEFAULTLGD         = 0,
		       T.NEWDEFAULTDEBTFLAG = '0',
		       T.PDPOOLMODELID      = NULL,
		       T.LGDPOOLMODELID     = NULL,
		       T.CCFPOOLMODELID     = NULL,
		       T.PDPOOLID           = NULL,
		       T.LGDPOOLID          = NULL,
		       T.CCFPOOLID          = NULL,
		       T.DefaultDate        = NULL
		 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		   AND EXISTS (SELECT 1
		          FROM RWA_DEV.RWA_EI_CLIENT T1
		         WHERE T.CLIENTID = T1.CLIENTID
		           AND T.DATADATE = T1.DATADATE
		           AND T1.CLIENTTYPE = '02')
		;

		COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_CLIENT����¿ͻ����ʹ�С��������ݼ�¼Ϊ: ' || v_count || ' ��');
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
      --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
      ROLLBACK;
      p_po_rtncode := sqlcode;
      p_po_rtnmsg  := '����ͻ����ͷ���('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_CD_CUSTOMER_TYPE0910;
/

