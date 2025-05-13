CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WH_MARKETEXPOSURESTD(
                                                   p_data_dt_str    IN    VARCHAR2,        --�������� yyyyMMdd
                                                   p_po_rtncode    OUT    VARCHAR2,        --���ر�� 1 �ɹ�,0 ʧ��
                                                   p_po_rtnmsg     OUT    VARCHAR2         --��������
                )
  /*
    �洢��������:RWA_DEV.PRO_RWA_WH_MARKETEXPOSURESTD
    ʵ�ֹ���:����ϵͳ-�г�����-��׼����¶��(������Դ����ֻ�ͷ���ȫ������RWA�г����չ���ӿڱ�����׼����¶����)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-12
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.RWA_WH_FESPOTPOSITION|����ֻ�ͷ���
    Դ  ��2 :RWA.ORG_INFO|RWA������
    Դ  ��3 :RWA_DEV.RWA_WH_FEFORWARDSSWAP|���Զ�ڵ��ڱ����ڣ�
    Ŀ���  :RWA_DEV.RWA_WH_MARKETEXPOSURESTD|����ϵͳ����׼����¶��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl  2019/09/09  �������߼�
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WH_MARKETEXPOSURESTD';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_WH_MARKETEXPOSURESTD';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1 ����ϵͳ-�ֻ�ͷ���ʲ�����ծ������
    INSERT INTO RWA_DEV.RWA_WH_MARKETEXPOSURESTD(
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
                ,OPTIONUNDERLYINGNAME									 --��Ȩ������������
                ,ORGSORTNO														 --���������

    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,p_data_dt_str                                AS DATANO                   --������ˮ��
                ,T1.POSITIONID                                AS EXPOSUREID               --���ձ�¶ID
                ,T1.BOOKTYPE                                  AS BOOKTYPE                 --�˻����
                ,T1.POSITIONID                                AS INSTRUMENTSID            --���ڹ���ID
                ,T1.INSTRUMENTSTYPE                           AS INSTRUMENTSTYPE          --���ڹ�������
                ,T1.ACCORGID                                  AS ORGID                    --��������ID
                ,T3.ORGNAME                                   AS ORGNAME                  --������������
                ,'01'                                         AS ORGTYPE                  --������������                                 Ĭ�ϣ����ڻ���(01)
                ,'03'                                         AS MARKETRISKTYPE           --�г���������                                 Ĭ�ϣ�������(03)
                ,''                                           AS INTERATERISKTYPE         --���ʷ�������                                 Ĭ�ϣ���
                ,''                                           AS EQUITYRISKTYPE           --��Ʊ��������                                 Ĭ�ϣ���
                ,CASE WHEN T1.CURRENCY = 'CNY' THEN ''                                                                                                                                             --���������ӳ��
                            WHEN T1.CURRENCY = 'USD' THEN '01'                                                                                                                                         --��Ԫ(01)
                            WHEN T1.CURRENCY = 'EUR' THEN '02'                                                                                                                                         --ŷԪ(02)
                            WHEN T1.CURRENCY = 'JPY' THEN '03'                                                                                                                                         --��Ԫ(03)
                            WHEN T1.CURRENCY = 'GBP' THEN '04'                                                                                                                                         --Ӣ��(04)
                            WHEN T1.CURRENCY = 'HKD' THEN '05'                                                                                                                                         --��Ԫ(05)
                            WHEN T1.CURRENCY = 'CHF' THEN '06'                                                                                                                                         --��ʿ����(06)
                            WHEN T1.CURRENCY = 'AUD' THEN '07'                                                                                                                                         --�Ĵ�����Ԫ(07)
                            WHEN T1.CURRENCY = 'CAD' THEN '08'                                                                                                                                         --���ô�Ԫ(08)
                            WHEN T1.CURRENCY = 'SGD' THEN '09'                                                                                                                                         --�¼���Ԫ(09)
                            WHEN T1.CURRENCY NOT IN ('CNY','USD','EUR','JPY','GBP','HKD','CHF','AUD','CAD','SGD')
                                     AND T1.POSITIONTYPE = '01' THEN '10'                                                                                                                              --�����ϱ��֣���ͷ������Ϊ��ͷ(01),��ӳ��Ϊ�������ֶ�ͷ(10)
                            WHEN T1.CURRENCY NOT IN ('CNY','USD','EUR','JPY','GBP','HKD','CHF','AUD','CAD','SGD')
                                     AND T1.POSITIONTYPE = '02' THEN '11'                                                                                                                              --�����ϱ��֣���ͷ������Ϊ��ͷ(02),��ӳ��Ϊ�������ֿ�ͷ(11)
                            ELSE '12'                                                                                                                                                                                             --�ƽ�(12)
                   END                                        AS EXCHANGERISKTYPE         --����������                                 �������<>����ң�������ݱ���ӳ�䣻����Ҳ���ӳ��
                ,''                                           AS COMMODITYNAME            --��Ʒ��������                                 Ĭ�ϣ���
                ,''                                           AS OPTIONRISKTYPE           --��Ȩ��������                                 Ĭ�ϣ���
                ,''                                           AS ISSUERID                 --������ID                                     Ĭ�ϣ���
                ,''                                           AS ISSUERNAME               --����������                                   Ĭ�ϣ���
                ,''                                           AS ISSUERTYPE               --�����˴���                                   Ĭ�ϣ���
                ,''                                           AS ISSUERSUBTYPE            --������С��                                   Ĭ�ϣ���
                ,''                                           AS ISSUERREGISTSTATE        --������ע�����                               Ĭ�ϣ���
                ,''                                           AS ISSUERRCERATING          --�����˾���ע����ⲿ����                     Ĭ�ϣ���
                ,''                                           AS SMBFLAG                  --С΢��ҵ��ʶ                                 Ĭ�ϣ���
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
                ,T1.STRUCTURALEXPOFLAG                        AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
                ,'0'                                          AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������                             Ĭ�ϣ���(0)
                ,''                                           AS OPTIONUNDERLYINGTYPE     --��Ȩ������������                             Ĭ�ϣ���
                ,''                                           AS OPTIONID                 --��Ȩ����ID                                   Ĭ�ϣ���
                ,NULL                                         AS VOLATILITY               --������                                       Ĭ�ϣ���
                ,''                                           AS STARTDATE                --��ʼ����                                     Ĭ�ϣ���
                ,''                                           AS DUEDATE                  --��������                                     Ĭ�ϣ���
                ,0                                            AS ORIGINALMATURITY         --ԭʼ����                                     Ĭ�ϣ���
                ,0                                            AS RESIDUALM                --ʣ������                                     Ĭ�ϣ���
                ,''                                           AS NEXTREPRICEDATE          --�´��ض�����                                 Ĭ�ϣ���
                ,NULL                                         AS NEXTREPRICEM             --�´��ض�������                               Ĭ�ϣ���
                ,''                                           AS RATETYPE                 --��������                                     Ĭ�ϣ���
                ,NULL                                         AS COUPONRATE               --Ʊ������                                     Ĭ�ϣ���
                ,''                                           AS MODIFIEDDURATION         --��������                                     Ĭ�ϣ���
                ,T1.POSITIONTYPE                              AS POSITIONTYPE             --ͷ������
                ,ABS(T1.POSITION)                             AS POSITION                 --ͷ��
                ,T1.CURRENCY                                  AS CURRENCY                 --����
                ,''																		 			  AS OPTIONUNDERLYINGNAME		 --��Ȩ������������
                ,T3.SORTNO														 			  AS ORGSORTNO								 --���������

    FROM				RWA_DEV.RWA_WH_FESPOTPOSITION T1	             		 					--����ֻ�ͷ����Ϣ��
    LEFT	JOIN	RWA.ORG_INFO T3
    ON					T1.ACCORGID = T3.ORGID
	  WHERE				T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T1.CURRENCY <> 'CNY'  --����У����򣬽�������ų�
    AND					T1.POSITION <> 0
	  ;

    COMMIT;

    --2.2 OPICS����Ʒϵͳ-������\Զ��\���� �Լ��ṹ�Գ���
    INSERT INTO RWA_DEV.RWA_WH_MARKETEXPOSURESTD(
                 DATADATE                              --��������
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
                ,OPTIONUNDERLYINGNAME									 --��Ȩ������������
                ,ORGSORTNO														 --���������

    )
    SELECT   T1.DATADATE                                 AS DATADATE                 --��������
            ,TO_CHAR(T1.DATADATE,'yyyyMMdd')             AS DATANO                   --������ˮ��
            ,CASE WHEN T1.FLAG='1' AND T1.CURRENCY='CNY'
                  THEN 'LLYBFX'||T1.TRANID||'B'||T1.CURRENCY
                  WHEN T1.FLAG='2' AND T1.CURRENCY='CNY'
                  THEN 'LLYBFX'||T1.TRANID||'S'||T1.CURRENCY
                  WHEN T1.FLAG='1' AND T1.CURRENCY<>'CNY'
                  THEN 'WHFX'||T1.TRANID||'B'||T1.CURRENCY
                  WHEN T1.FLAG='2' AND T1.CURRENCY<>'CNY'
                  THEN 'WHFX'||T1.TRANID||'S'||T1.CURRENCY
                  ELSE '' END                            AS EXPOSUREID               --���ձ�¶ID
            ,T1.BOOKTYPE                                 AS BOOKTYPE                 --�˻����  Ĭ�ϣ������˻�(02)
            ,T1.TRANID                                   AS INSTRUMENTSID            --���ڹ���ID
            ,T1.INSTRUMENTSTYPE                          AS INSTRUMENTSTYPE          --���ڹ�������
            ,T1.TRANORGID                                AS ORGID                    --��������ID
            ,T2.ORGNAME	                                 AS ORGNAME                  --������������
            ,'01'                                        AS ORGTYPE                  --������������            Ĭ�ϣ����ڻ���(01)
            ,DECODE(T1.CURRENCY,'CNY','01','03')         AS MARKETRISKTYPE           --�г���������            CNY Ϊ 01 ���ʷ��� ����Ϊ 03 ������
            ,DECODE(T1.CURRENCY,'CNY','02','')           AS INTERATERISKTYPE         --���ʷ�������            CNY Ϊ 02 ���� ����Ϊ ��
            ,''                                          AS EQUITYRISKTYPE           --��Ʊ��������            Ĭ�ϣ���
            ,CASE WHEN ACCSUBJECTS like '143101%'
                  THEN '12'--�ƽ�
                  WHEN CURRENCY='CNY'
                  THEN ''
                  WHEN CURRENCY='USD'
                  THEN '01'--��Ԫ
                  WHEN CURRENCY='EUR'
                  THEN '02'--ŷԪ
                  WHEN CURRENCY='JPY'
                  THEN '03'--��Ԫ
                  WHEN CURRENCY='GBP'
                  THEN '04'--Ӣ��
                  WHEN CURRENCY='HKD'
                  THEN '05'--��Ԫ
                  WHEN CURRENCY='CHF'
                  THEN '06'--��ʿ����
                  WHEN CURRENCY='AUD'
                  THEN '07'--�Ĵ�����Ԫ
                  WHEN CURRENCY='CAD'
                  THEN '08'--���ô�Ԫ
                  WHEN CURRENCY='SGD'
                  THEN '09'--�¼���Ԫ
                  WHEN FLAG='1'
                  THEN '10'--�������ֶ�ͷ
                  --�������ֿ�ͷ
                  ELSE '11' END                          AS EXCHANGERISKTYPE         --����������
            ,''                                          AS COMMODITYNAME            --��Ʒ��������            Ĭ�ϣ���
            ,''                                          AS OPTIONRISKTYPE           --��Ȩ��������            Ĭ�ϣ���
            ,''                                          AS ISSUERID                 --������ID                Ĭ�ϣ���
            ,''                                          AS ISSUERNAME               --����������              Ĭ�ϣ���
            ,''                                          AS ISSUERTYPE               --�����˴���              Ĭ�ϣ���
            ,''                                          AS ISSUERSUBTYPE            --������С��              Ĭ�ϣ���
            ,''                                          AS ISSUERREGISTSTATE        --������ע�����          Ĭ�ϣ���
            ,''                                          AS ISSUERRCERATING          --�����˾���ע����ⲿ����Ĭ�ϣ���
            ,''                                          AS SMBFLAG                  --С΢��ҵ��ʶ            Ĭ�ϣ���
            ,'0'                                         AS UNDERBONDFLAG            --�Ƿ����ծȯ            Ĭ�ϣ���(0)
            ,''                                          AS PAYMENTDATE              --�ɿ���                  Ĭ�ϣ���
            ,''                                          AS SECURITIESTYPE           --֤ȯ���                Ĭ�ϣ���
            ,''                                          AS BONDISSUEINTENT          --ծȯ����Ŀ��            Ĭ�ϣ���
            ,''                                          AS CLAIMSLEVEL              --ծȨ����                Ĭ�ϣ���
            ,''                                          AS REABSFLAG                --���ʲ�֤ȯ����ʶ        Ĭ�ϣ���
            ,''                                          AS ORIGINATORFLAG           --�Ƿ������            Ĭ�ϣ���
            ,''                                          AS SECURITIESERATING        --֤ȯ�ⲿ����            Ĭ�ϣ���
            ,''                                          AS STOCKCODE                --��Ʊ/��ָ����           Ĭ�ϣ���
            ,''                                          AS STOCKMARKET              --�����г�                Ĭ�ϣ���
            ,''                                          AS EXCHANGEAREA             --���׵���                Ĭ�ϣ���
            ,T1.STRUCTURALEXPOFLAG                       AS STRUCTURALEXPOFLAG       --�Ƿ�ṹ�Գ���
            ,'0'                                         AS OPTIONUNDERLYINGFLAG     --�Ƿ���Ȩ��������        Ĭ�ϣ���(0)
            ,''                                          AS OPTIONUNDERLYINGTYPE     --��Ȩ������������        Ĭ�ϣ���
            ,''                                          AS OPTIONID                 --��Ȩ����ID              Ĭ�ϣ���
            ,''                                          AS VOLATILITY               --������                  Ĭ�ϣ���
            ,T1.STARTDATE                                AS STARTDATE                --��ʼ����
            ,T1.DUEDATE                                  AS DUEDATE                  --��������
            ,T1.ORIGINALMATURITY                         AS ORIGINALMATURITY         --ԭʼ����
            ,T1.RESIDUALM                                AS RESIDUALM                --ʣ������
            ,''                                          AS NEXTREPRICEDATE          --�´��ض�����            Ĭ�ϣ���
            ,''                                          AS NEXTREPRICEM             --�´��ض�������          Ĭ�ϣ���
            ,DECODE(T1.CURRENCY,'CNY','01','')           AS RATETYPE                 --��������                CNY Ϊ 01 �̶����� ����Ϊ ��
            ,''                                          AS COUPONRATE               --Ʊ������                Ĭ�ϣ���
            ,''                                          AS MODIFIEDDURATION         --��������                Ĭ�ϣ���
            ,DECODE(T1.FLAG,'1','01','02')               AS POSITIONTYPE             --ͷ������                Ĭ�ϣ����� Ϊ ��ͷ 01 ���� Ϊ ��ͷ 02
            ,T1.POSITION                                 AS POSITION                 --ͷ��
            ,T1.CURRENCY                                 AS CURRENCY                 --����
            ,''																		 			 AS OPTIONUNDERLYINGNAME		 --��Ȩ������������
            ,T2.SORTNO														 		 	 AS ORGSORTNO								 --���������
    FROM  (
            --���뽻��
            SELECT  '1'                      AS FLAG                  --����������־��1 ���� 2 ����
                   ,DATADATE                 AS DATADATE              --��������
                   ,TRANID                   AS TRANID                --����ID
                   ,BUYCURRENCY              AS CURRENCY              --����
                   ,BOOKTYPE                 AS BOOKTYPE              --�˻����
                   ,INSTRUMENTSTYPE          AS INSTRUMENTSTYPE       --���ڹ�������
                   ,TRANORGID                AS TRANORGID             --���׻���ID
                   ,ACCSUBJECTS              AS ACCSUBJECTS           --��ƿ�Ŀ
                   ,STRUCTURALEXPOFLAG       AS STRUCTURALEXPOFLAG    --�Ƿ�ṹ�Գ���
                   ,STARTDATE                AS STARTDATE             --��ʼ����
                   ,DUEDATE                  AS DUEDATE               --��������
                   ,ORIGINALMATURITY         AS ORIGINALMATURITY      --ԭʼ����
                   ,RESIDUALM                AS RESIDUALM             --ʣ������
                   ,ABS(BUYAMOUNT)           AS POSITION              --������
            FROM RWA_DEV.RWA_WH_FEFORWARDSSWAP
           WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
           	 AND BUYAMOUNT > 0
            UNION ALL
            --��������
            SELECT  '2'                      AS FLAG                  --����������־��1 ���� 2 ����
                   ,DATADATE                 AS DATADATE              --��������
                   ,TRANID                   AS TRANID                --����ID
                   ,SELLCURRENCY             AS CURRENCY              --����
                   ,BOOKTYPE                 AS BOOKTYPE              --�˻����
                   ,INSTRUMENTSTYPE          AS INSTRUMENTSTYPE       --���ڹ�������
                   ,TRANORGID                AS TRANORGID             --���׻���ID
                   ,ACCSUBJECTS              AS ACCSUBJECTS           --��ƿ�Ŀ
                   ,STRUCTURALEXPOFLAG       AS STRUCTURALEXPOFLAG    --�Ƿ�ṹ�Գ���
                   ,STARTDATE                AS STARTDATE             --��ʼ����
                   ,DUEDATE                  AS DUEDATE               --��������
                   ,ORIGINALMATURITY         AS ORIGINALMATURITY      --ԭʼ����
                   ,RESIDUALM                AS RESIDUALM             --ʣ������
                   ,ABS(SELLAMOUNT)          AS POSITION              --�������
            FROM RWA_DEV.RWA_WH_FEFORWARDSSWAP
           WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
           	 AND SELLAMOUNT > 0
           ) T1
    LEFT JOIN RWA.ORG_INFO T2
    ON T1.TRANORGID = T2.ORGID
	;
    COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_WH_MARKETEXPOSURESTD',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_WH_MARKETEXPOSURESTD;
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
END PRO_RWA_WH_MARKETEXPOSURESTD;
/

